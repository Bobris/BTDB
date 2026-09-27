using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDB.Replication;

public sealed record RemoteMaintenanceFile(string Key, uint FileId, KVFileType FileType, string Version,
    bool RetainForDiscovery = false, DateTimeOffset? DeleteAfter = null);

internal sealed record PublishedCheckpoint(uint FileId, uint ReplayFromFileId, IReadOnlySet<uint> Dependencies);

/// <summary>One leader session, serialized with its checkpoint/export lane. Deletion deadlines live on objects. Only a positively confirmed checkpoint authorizes deletion; no persisted GC manifest or follower pins.</summary>
internal sealed class RemoteGarbageCollector(IReplicationStorage storage, LeaseAuthority authority,
    TimeSpan deletionDelay)
{
    // Positive (production assumes at least a day), so a fenced predecessor's in-flight mark cannot become due
    // before this leader protects the file; unmarked files are then never rewritten merely to change their version.
    readonly TimeSpan _deletionDelay = deletionDelay > TimeSpan.Zero ? deletionDelay
        : throw new ArgumentOutOfRangeException(nameof(deletionDelay));

    public async ValueTask CollectAsync(PublishedCheckpoint checkpoint, CancellationToken cancellation, Action<int, int>? progress = null)
    {
        var inventory = new List<RemoteMaintenanceFile>();
        await foreach (var file in storage.EnumerateMaintenanceAsync(cancellation).ConfigureAwait(false))
        {
            inventory.Add(file);
            progress?.Invoke(0, inventory.Count);
        }
        // A higher checkpoint may have landed after discovery. Do not infer its dependencies from our older snapshot.
        if (inventory.Any(f => f.FileType == KVFileType.KeyIndex && f.FileId > checkpoint.FileId))
        { return; }
        if (!inventory.Any(f => f.FileType == KVFileType.KeyIndex && f.FileId == checkpoint.FileId))
            throw new IOException("Confirmed checkpoint disappeared before cleanup.");
        // Keep the highest even identity as an allocation anchor, including an orphan upload. No reservation ledger.
        var maximumEvenId = inventory.Where(f => (f.FileId & 1) == 0).Select(f => f.FileId).DefaultIfEmpty().Max();
        var firstRetainedTrl = inventory.Where(f => f.FileType == KVFileType.TransactionLog &&
                checkpoint.Dependencies.Contains(f.FileId)).Select(f => f.FileId)
            .Append(checkpoint.ReplayFromFileId).Min();
        var index = 0;
        foreach (var file in inventory)
        {
            cancellation.ThrowIfCancellationRequested();
            progress?.Invoke(1, index++);
            if (!authority.IsValid) return;
            var keep = file.RetainForDiscovery || file.FileId == maximumEvenId || file.FileId == checkpoint.FileId ||
                       checkpoint.Dependencies.Contains(file.FileId) ||
                       (file.FileType == KVFileType.TransactionLog && file.FileId >= firstRetainedTrl);
            if (keep)
            {
                if (file.DeleteAfter != null) await storage.CancelDeletionAsync(file, cancellation).ConfigureAwait(false);
                continue;
            }
            // A listed deadline already carries the current version; scheduling never extends it anyway.
            var scheduled = file.DeleteAfter != null ? file
                : await storage.ScheduleDeletionAsync(file, _deletionDelay, cancellation).ConfigureAwait(false);
            if (!authority.IsValid) return;
            await storage.DeleteAsync(scheduled, cancellation).ConfigureAwait(false);
        }
    }
}

/// <summary>One database/leader session. Local compaction is scheduled separately and never receives this lane's
/// cancellation. Retains exactly one native export snapshot across unresolved publication; releases it on shutdown.</summary>
public sealed class ReplicationMaintenance(BTreeKeyValueDB database, ReplicationFileSet files,
    CanonicalTrlPublisher canonical, IReplicationStorage storage, LeaseAuthority authority,
    IReplicationScheduler clock, TimeSpan interval, TimeSpan deletionDelay,
    IObjectDB? objects = null, Func<LeakRemovalCandidates, CancellationToken, ValueTask>? publishLeakEvent = null) : IDisposable
{
    readonly CheckpointPublisher _checkpoints = new(files, canonical, storage);
    readonly RemoteGarbageCollector _garbage = new(storage, authority, deletionDelay);
    KeyIndexSnapshot? _pending;
    // Cut and source files of the last published checkpoint. An unchanged database would export the same KVI again.
    (uint FileId, ulong Length)[]? _publishedSources;
    readonly TimeSpan _interval = interval > TimeSpan.Zero ? interval
        : throw new ArgumentOutOfRangeException(nameof(interval));
    TimeSpan _next;
    bool _collecting;
    internal ReplicationMaintenanceWatchdog? Watchdog { get; set; }

    // The coordinator runs maintenance; hosts only construct it in CreateMaintenance.
    internal async ValueTask RunDueAsync(CancellationToken cancellation)
    {
        if (!authority.IsValid || (_pending == null && !_collecting && clock.Elapsed < _next)) return;
        Watchdog?.Observe((0, 0, 0));
        if (!_collecting)
        {
            _pending ??= database.CaptureKeyIndexSnapshot(cancellation);
            if (_pending.TransactionLogFileId == 0)
            {
                _pending.Dispose();
                _pending = null;
                _next = clock.Elapsed + _interval;
                Watchdog?.Observe(null);
                return;
            }
            var sources = Sources(_pending);
            // Unchanged since the published checkpoint: skip the export, but still collect so due deletions happen.
            if (_publishedSources == null || !sources.AsSpan().SequenceEqual(_publishedSources))
            {
                var result = await _checkpoints.PublishAsync(_pending, cancellation, true,
                    Watchdog == null ? null : (step, item) => Watchdog.Observe((1, step, item))).ConfigureAwait(false);
                if (result != CheckpointPublishResult.Published) return;
                _publishedSources = sources;
            }
            _pending.Dispose();
            _pending = null;
            _collecting = true;
        }
        Watchdog?.Observe((2, -1, 0));
        await _garbage.CollectAsync(_checkpoints.Published!, cancellation,
            Watchdog == null ? null : (step, item) => Watchdog.Observe((2, step, item))).ConfigureAwait(false);
        _collecting = false;
        _next = clock.Elapsed + _interval;
        Watchdog?.Observe(null);
        // Submitting an application-owned leak event is outside replication's progress deadline.
        if (authority.IsValid && objects != null && publishLeakEvent != null)
        {
            var candidates = objects.CollectLeakRemovalCandidates(cancellation);
            cancellation.ThrowIfCancellationRequested();
            if (candidates.KeyCount != 0 && authority.IsValid)
                await publishLeakEvent(candidates, cancellation).ConfigureAwait(false);
        }
    }

    static (uint FileId, ulong Length)[] Sources(KeyIndexSnapshot snapshot) =>
        [(snapshot.TransactionLogFileId, snapshot.TransactionLogOffset), .. snapshot.Sources.Select(s => (s.FileId, s.Length))];

    public void Dispose() { Watchdog?.Dispose(); _pending?.Dispose(); _pending = null; }
}
