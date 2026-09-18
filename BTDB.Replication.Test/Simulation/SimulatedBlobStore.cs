using System;
using System.Collections.Generic;
using System.Linq;

namespace BTDB.Replication.Test.Simulation;

internal enum WriteOutcome { Applied, Rejected, Ambiguous }
internal enum MutationKind { Create, Replace, Append, Delete }
internal sealed record BlobSnapshot(ulong Version, byte[] Content, IReadOnlyDictionary<string, string>? Metadata = null);
internal sealed record WriteResult(WriteOutcome Outcome, ulong? Version = null);
internal sealed record WriteFaults(TimeSpan EffectDelay, TimeSpan ResponseDelay, TimeSpan? Timeout = null,
    bool LoseResponse = false);

// Test-only reference storage; real Azure conformance remains separate from this implementation.
internal sealed class SimulatedBlobStore
{
    internal sealed record Request(long Id, string Key, MutationKind Kind, ulong? ExpectedVersion,
        byte[] Bytes, string Actor, IReadOnlyDictionary<string, string>? Metadata = null, string? LeaseId = null);
    internal sealed record Observation(Request Request, bool IsDispatch, BlobSnapshot? Result, bool Applied);

    readonly DeterministicScheduler _scheduler;
    readonly DeterministicScheduler.Scope _service;
    readonly Dictionary<string, BlobSnapshot> _blobs = [];
    readonly List<Observation> _journal = [];
    ulong _version;
    long _requestId;

    public SimulatedBlobStore(DeterministicScheduler scheduler)
    {
        _scheduler = scheduler;
        _service = scheduler.CreateScope("blob-service");
    }

    static IReadOnlyDictionary<string, string>? CopyMetadata(IReadOnlyDictionary<string, string>? metadata)
        => metadata is null ? null : new Dictionary<string, string>(metadata);
    static BlobSnapshot? Copy(BlobSnapshot? blob) => blob is null ? null :
        new(blob.Version, blob.Content.ToArray(), CopyMetadata(blob.Metadata));
    public Func<string, string?, bool>? LeaseAllowsMutation { get; set; }
    public Action<string, ulong?>? MutationApplied { get; set; }
    public BlobSnapshot? Read(string key) => Copy(_blobs.GetValueOrDefault(key));
    public IReadOnlyList<Observation> Journal => _journal.Select(o => o with
    {
        Request = o.Request with { Bytes = o.Request.Bytes.ToArray(), Metadata = CopyMetadata(o.Request.Metadata) },
        Result = Copy(o.Result)
    }).ToArray();

    public long Dispatch(DeterministicScheduler.Scope caller, string actor, string key, MutationKind kind,
        ulong? expectedVersion, ReadOnlySpan<byte> bytes, WriteFaults faults, Action<WriteResult> response,
        IReadOnlyDictionary<string, string>? metadata = null, string? leaseId = null)
    {
        ObjectDisposedException.ThrowIf(caller.Stopped, caller);
        ArgumentOutOfRangeException.ThrowIfNegative(faults.EffectDelay.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegative(faults.ResponseDelay.Ticks);
        if (faults.Timeout is { } timeout) ArgumentOutOfRangeException.ThrowIfNegative(timeout.Ticks);
        if (kind != MutationKind.Create && expectedVersion is null)
            throw new ArgumentException("Existing-object mutations require a version.", nameof(expectedVersion));
        if (kind == MutationKind.Create && expectedVersion is not null)
            throw new ArgumentException("Create requires absence.", nameof(expectedVersion));

        var request = new Request(++_requestId, key, kind, expectedVersion, bytes.ToArray(), actor,
            CopyMetadata(metadata), leaseId);
        _journal.Add(new(request, true, null, false));
        _scheduler.Record($"dispatch blob {request.Id} {actor} {kind} {key} expected={expectedVersion}");
        var delivered = false;
        IDisposable? deadline = null;
        if (faults.Timeout is { } delay)
            deadline = caller.Schedule(delay, () =>
            {
                if (delivered) return;
                delivered = true;
                response(new(WriteOutcome.Ambiguous));
            }, $"blob {request.Id} timeout");

        _service.Schedule(faults.EffectDelay, () =>
        {
            var before = _blobs.GetValueOrDefault(key);
            var applied = kind == MutationKind.Create ? before is null : before?.Version == expectedVersion;
            applied &= LeaseAllowsMutation?.Invoke(key, leaseId) ?? true;
            BlobSnapshot? after = before;
            if (applied)
            {
                if (kind == MutationKind.Delete)
                {
                    _blobs.Remove(key);
                    after = null;
                }
                else
                {
                    var content = kind == MutationKind.Append
                        ? before!.Content.Concat(request.Bytes).ToArray() : request.Bytes.ToArray();
                    after = new(++_version, content, CopyMetadata(request.Metadata));
                    _blobs[key] = after;
                }
            }
            if (applied) MutationApplied?.Invoke(key, after?.Version);
            _journal.Add(new(request, false, Copy(after), applied));
            _scheduler.Record($"effect blob {request.Id} applied={applied} version={after?.Version}");
            if (faults.LoseResponse || caller.Stopped) return;
            var result = new WriteResult(applied ? WriteOutcome.Applied : WriteOutcome.Rejected,
                applied ? after?.Version : null);
            caller.Schedule(faults.ResponseDelay, () =>
            {
                if (delivered) return;
                delivered = true;
                deadline?.Dispose();
                response(result);
            }, $"blob {request.Id} response");
        }, $"blob {request.Id} effect");
        return request.Id;
    }
}
