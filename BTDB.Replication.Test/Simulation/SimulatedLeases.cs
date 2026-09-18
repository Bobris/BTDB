using System;
using System.Collections.Generic;

namespace BTDB.Replication.Test.Simulation;

internal enum LeaseOperation { Acquire, Renew, Change, Release }

/// <summary>Reference finite lease service. Expiry is evaluated at remote effect time, not response time.</summary>
internal sealed class SimulatedLeases
{
    sealed record Lease(string Id, long Duration, long Expires, ulong Version);
    readonly Dictionary<string, Lease> _leases = [];
    readonly DeterministicScheduler _scheduler;
    readonly DeterministicScheduler.Scope _service;
    readonly SimulatedBlobStore _storage;

    public SimulatedLeases(DeterministicScheduler scheduler, SimulatedBlobStore storage)
    {
        _scheduler = scheduler;
        _storage = storage;
        _service = scheduler.CreateScope("lease-service");
        storage.LeaseAllowsMutation = AllowsMutation;
        storage.MutationApplied = (key, version) =>
        {
            if (!_leases.TryGetValue(key, out var lease)) return;
            if (version is null || lease.Expires <= scheduler.Elapsed.Ticks) _leases.Remove(key);
            else _leases[key] = lease with { Version = version.Value };
        };
    }

    bool AllowsMutation(string key, string? id)
    {
        if (!_leases.TryGetValue(key, out var lease) || lease.Expires <= _scheduler.Elapsed.Ticks)
            return id is null;
        return lease.Id == id;
    }

    public bool IsOwned(string key, string id) => _leases.TryGetValue(key, out var lease) &&
        lease.Id == id && lease.Expires > _scheduler.Elapsed.Ticks;

    public void Dispatch(DeterministicScheduler.Scope caller, string key, LeaseOperation operation, string id,
        TimeSpan duration, WriteFaults faults, Action<WriteOutcome> response, string? proposedId = null)
    {
        ObjectDisposedException.ThrowIf(caller.Stopped, caller);
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentOutOfRangeException.ThrowIfNegative(faults.EffectDelay.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegative(faults.ResponseDelay.Ticks);
        if (faults.Timeout is { } timeout) ArgumentOutOfRangeException.ThrowIfNegative(timeout.Ticks);
        if (operation == LeaseOperation.Acquire) ArgumentOutOfRangeException.ThrowIfNegativeOrZero(duration.Ticks);
        if (operation == LeaseOperation.Change) ArgumentException.ThrowIfNullOrEmpty(proposedId);
        var delivered = false;
        IDisposable? deadline = null;
        if (faults.Timeout is { } delay)
            deadline = caller.Schedule(delay, () =>
            {
                if (delivered) return;
                delivered = true;
                response(WriteOutcome.Ambiguous);
            }, $"lease {operation} timeout");

        _service.Schedule(faults.EffectDelay, () =>
        {
            var now = _scheduler.Elapsed.Ticks;
            var current = _leases.GetValueOrDefault(key);
            var held = current is not null && current.Expires > now;
            var blob = _storage.Read(key);
            var renewable = current is not null && current.Id == id && (held || blob?.Version == current.Version);
            var applied = blob is not null && operation switch
            {
                LeaseOperation.Acquire => !held || current!.Id == id,
                LeaseOperation.Renew or LeaseOperation.Release => renewable,
                LeaseOperation.Change => held && current!.Id == id,
                _ => false
            };
            if (applied)
            {
                switch (operation)
                {
                    case LeaseOperation.Acquire: _leases[key] = new(id, duration.Ticks, checked(now + duration.Ticks), blob!.Version); break;
                    case LeaseOperation.Renew: _leases[key] = current! with { Expires = checked(now + current!.Duration), Version = blob!.Version }; break;
                    case LeaseOperation.Change: _leases[key] = current! with { Id = proposedId! }; break;
                    case LeaseOperation.Release: _leases.Remove(key); break;
                }
            }
            _scheduler.Record($"lease effect {operation} {key} applied={applied}");
            if (faults.LoseResponse || caller.Stopped) return;
            caller.Schedule(faults.ResponseDelay, () =>
            {
                if (delivered) return;
                delivered = true;
                deadline?.Dispose();
                response(applied ? WriteOutcome.Applied : WriteOutcome.Rejected);
            }, $"lease {operation} response");
        }, $"lease {operation} effect");
    }
}
