using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>Database-scoped replication storage: canonical TRL CAS, immutable PVL/KVI publication and cleanup.
/// TRL append and metadata updates are atomic; reads bind to the supplied version. Dispatched operations may
/// succeed despite cancellation or a lost response. Keys and native IDs must never be reused.
/// Immutable publication checks session authority before every dispatch, creates only if absent, and reconciles
/// length and whole-file SHA metadata on retry; conflicting content throws RemoteFileConflictException.
/// Cleanup checks authority and object version. PVL protection changes the version to defeat stale deletes.
/// Inventory reads require a selected canonical snapshot; restore needs no leadership authority.</summary>
public interface IReplicationStorage : IRemoteFileCollection
{
    /// <summary>Resolve a published checkpoint's retained TRL root, or the supplied genesis before the first checkpoint.</summary>
    ValueTask<TrlSuccessor> ResolveRecoveryRootAsync(TrlSuccessor genesis, CancellationToken cancellation) => ValueTask.FromResult(genesis);
    /// <summary>Persist deletion eligibility without extending an existing deadline; return its current version.</summary>
    ValueTask<RemoteMaintenanceFile> ScheduleDeletionAsync(RemoteMaintenanceFile file, TimeSpan delay, CancellationToken cancellation) =>
        throw new NotSupportedException("Persistent delayed cleanup is not implemented by this adapter.");
    ValueTask CancelDeletionAsync(RemoteMaintenanceFile file, CancellationToken cancellation) =>
        throw new NotSupportedException("Persistent delayed cleanup is not implemented by this adapter.");

    ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation);
    ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination, CancellationToken cancellation);
    /// <summary>The canonical publisher checks live authority before dispatch. The adapter enforces the expected
    /// token and atomically installs bytes and term metadata; unbound adapters support bootstrap/adoption.</summary>
    ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation);

    ValueTask EnsurePureValuesAsync(uint remoteFileId, KeyIndexFileSource source, CancellationToken cancellation);
    /// <summary>Publish or reconcile this exact chosen immutable KVI identity. A lost response retries the same
    /// ID, snapshot and mapping; verify existing content instead of overwriting or allocating another ID.</summary>
    ValueTask PublishKeyIndexAsync(uint remoteFileId, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> pureValueFileIds,
        CancellationToken cancellation);

    IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync(CancellationToken cancellation);
    /// <summary>Delete only if the persisted deadline has elapsed and the version still matches; unmarked files are protected.</summary>
    ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation);
    // Clear any deletion mark and change the version before KVI publication. False means absent: the caller must allocate a fresh identity, never recreate a retired key.
    ValueTask<bool> ProtectPureValuesAsync(uint fileId, KeyIndexFileSource source, CancellationToken cancellation);
}
