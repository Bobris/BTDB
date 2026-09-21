using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDB.Replication;

internal sealed record RemoteMaintenanceFile(string Key, uint FileId, KVFileType FileType, string Version,
    bool RetainForDiscovery = false);

/// <summary>Physical database-scoped inventory. Deletion and protection check live authority at dispatch.
/// Protection changes the object's version conditionally, so an earlier in-flight delete cannot erase a reused PVL.</summary>
internal interface IRemoteMaintenanceStorage : ICheckpointStorage
{
    IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync(CancellationToken cancellation);
    ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation);
    // False means absent: the caller must allocate a fresh identity, never recreate a retired key.
    ValueTask<bool> ProtectPureValuesAsync(uint fileId, KeyIndexFileSource source, CancellationToken cancellation);
}

internal sealed record PublishedCheckpoint(uint FileId, uint ReplayFromFileId, IReadOnlySet<uint> Dependencies);

/// <summary>One leader session, serialized with its checkpoint/export lane. Restart forgets candidates and restarts
/// the delay. Only a positively confirmed checkpoint authorizes deletion; no persisted GC manifest or follower pins.</summary>
internal sealed class RemoteGarbageCollector(IRemoteMaintenanceStorage storage, LeaseAuthority authority,
    IReplicationScheduler clock, TimeSpan deletionDelay)
{
    readonly Dictionary<string, (string Version, TimeSpan Since)> _obsolete = new(StringComparer.Ordinal);

    public async ValueTask CollectAsync(PublishedCheckpoint checkpoint, CancellationToken cancellation)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(deletionDelay.Ticks);
        var inventory = new List<RemoteMaintenanceFile>();
        await foreach (var file in storage.EnumerateMaintenanceAsync(cancellation).ConfigureAwait(false)) inventory.Add(file);
        // A higher checkpoint may have landed after discovery. Do not infer its dependencies from our older snapshot.
        if (inventory.Any(f => f.FileType == KVFileType.KeyIndex && f.FileId > checkpoint.FileId))
        { _obsolete.Clear(); return; }
        if (!inventory.Any(f => f.FileType == KVFileType.KeyIndex && f.FileId == checkpoint.FileId))
            throw new IOException("Confirmed checkpoint disappeared before cleanup.");
        // Keep the highest even identity as an allocation anchor, including an orphan upload. No reservation ledger.
        var maximumEvenId = inventory.Where(f => (f.FileId & 1) == 0).Select(f => f.FileId).DefaultIfEmpty().Max();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in inventory)
        {
            cancellation.ThrowIfCancellationRequested();
            if (!authority.IsValid) return;
            seen.Add(file.Key);
            var keep = file.RetainForDiscovery || file.FileId == maximumEvenId || file.FileId == checkpoint.FileId ||
                       checkpoint.Dependencies.Contains(file.FileId) ||
                       (file.FileType == KVFileType.TransactionLog && file.FileId >= checkpoint.ReplayFromFileId);
            if (keep) { _obsolete.Remove(file.Key); continue; }
            if (!_obsolete.TryGetValue(file.Key, out var candidate) || candidate.Version != file.Version)
                _obsolete[file.Key] = candidate = (file.Version, clock.Elapsed);
            if (clock.Elapsed - candidate.Since < deletionDelay) continue;
            if (!authority.IsValid) return;
            await storage.DeleteAsync(file, cancellation).ConfigureAwait(false);
            _obsolete.Remove(file.Key);
        }
        foreach (var key in _obsolete.Keys.Where(k => !seen.Contains(k)).ToArray()) _obsolete.Remove(key);
    }
}

/// <summary>One database/leader session. Local compaction is scheduled separately and never receives this lane's
/// cancellation. Retains exactly one native export snapshot across unresolved publication; releases it on shutdown.</summary>
internal sealed class ReplicationMaintenance(BTreeKeyValueDB database, ReplicationFileSet files,
    CanonicalTrlPublisher canonical, IRemoteMaintenanceStorage storage, LeaseAuthority authority,
    IReplicationScheduler clock, TimeSpan interval, TimeSpan deletionDelay,
    IObjectDB? objects = null, Func<LeakRemovalCandidates, CancellationToken, ValueTask>? publishLeakEvent = null) : IDisposable
{
    readonly CheckpointPublisher _checkpoints = new(files, canonical, storage);
    readonly RemoteGarbageCollector _garbage = new(storage, authority, clock, deletionDelay);
    KeyIndexSnapshot? _pending;
    TimeSpan _next;

    public async ValueTask RunDueAsync(CancellationToken cancellation)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks);
        if (!authority.IsValid || (_pending == null && clock.Elapsed < _next)) return;
        _pending ??= database.CaptureKeyIndexSnapshot(cancellation);
        if (_pending.TransactionLogFileId == 0) { Dispose(); _next = clock.Elapsed + interval; return; }
        var result = await _checkpoints.PublishAsync(_pending, cancellation, true).ConfigureAwait(false);
        if (result != CheckpointPublishResult.Published) return;
        _pending.Dispose();
        _pending = null;
        _next = clock.Elapsed + interval;
        await _garbage.CollectAsync(_checkpoints.Published!, cancellation).ConfigureAwait(false);
        if (authority.IsValid && objects != null && publishLeakEvent != null)
        {
            var candidates = objects.CollectLeakRemovalCandidates(cancellation);
            cancellation.ThrowIfCancellationRequested();
            if (candidates.KeyCount != 0 && authority.IsValid)
                await publishLeakEvent(candidates, cancellation).ConfigureAwait(false);
        }
    }

    public void Dispose() { _pending?.Dispose(); _pending = null; }
}
