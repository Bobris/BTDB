using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>Database-scoped replication storage: canonical TRL CAS, immutable PVL/KVI publication and cleanup.
/// TRL conditional writes are atomic; reads bind to the supplied version. Dispatched operations may
/// succeed despite cancellation or a lost response. Keys and native IDs must never be reused.
/// Immutable publication checks session authority before every dispatch, creates only if absent, and reconciles
/// length and whole-file SHA metadata on retry; conflicting content throws RemoteFileConflictException.
/// Cleanup checks authority and object version. PVL protection changes the version to defeat stale deletes.
/// Inventory reads require a selected canonical snapshot; restore and create-only legacy header bootstrap need no leadership authority.</summary>
public interface IReplicationStorage : IRemoteFileCollection
{
    /// <summary>Resolve the oldest retained TRL, or the supplied genesis for an empty database.</summary>
    async ValueTask<TrlSuccessor> ResolveRecoveryRootAsync(TrlSuccessor genesis, CancellationToken cancellation)
    {
        TrlHead? first = null;
        await foreach (var head in EnumerateTrlsAsync(cancellation).ConfigureAwait(false))
            if (first == null || head.FileId < first.FileId) first = head;
        return first == null ? genesis : new(first.Key, first.FileId);
    }

    /// <summary>List committed TRL objects directly under the database prefix. There is only one key per native ID.</summary>
    async IAsyncEnumerable<TrlHead> EnumerateTrlsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellation)
    {
        await foreach (var file in EnumerateMaintenanceAsync(cancellation).ConfigureAwait(false))
        {
            if (file.FileType != KVFileType.TransactionLog) continue;
            var state = await ReadAsync(file.Key, cancellation).ConfigureAwait(false)
                ?? throw new System.IO.FileNotFoundException("A listed TRL disappeared.", file.Key);
            yield return new(file.FileId, file.Key, state);
        }
    }
    /// <summary>Persist deletion eligibility without extending an existing deadline; return its current version.</summary>
    ValueTask<RemoteMaintenanceFile> ScheduleDeletionAsync(RemoteMaintenanceFile file, TimeSpan delay, CancellationToken cancellation) =>
        throw new NotSupportedException("Persistent delayed cleanup is not implemented by this adapter.");
    ValueTask CancelDeletionAsync(RemoteMaintenanceFile file, CancellationToken cancellation) =>
        throw new NotSupportedException("Persistent delayed cleanup is not implemented by this adapter.");

    /// <summary>Read token and length from one consistent version. Keys end in {fileId}.trl.</summary>
    ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation);
    ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination, CancellationToken cancellation);
    /// <summary>The canonical publisher checks live authority before dispatch; legacy startup may only create a native header if absent.
    /// The adapter enforces the expected
    /// token and atomically installs native bytes; unbound adapters support bootstrap/adoption. An applied
    /// write keeps the expected version's first ExpectedLength bytes, so reconciliation compares only the append.</summary>
    ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation);

    ValueTask EnsurePureValuesAsync(uint remoteFileId, KeyIndexFileSource source, CancellationToken cancellation);
    /// <summary>Publish or reconcile this exact chosen immutable KVI identity. A lost response retries the same
    /// ID, snapshot and mapping; verify existing content instead of overwriting or allocating another ID.</summary>
    ValueTask PublishKeyIndexAsync(uint remoteFileId, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> pureValueFileIds,
        CancellationToken cancellation);

    IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync(CancellationToken cancellation);
    /// <summary>Delete only if the persisted deadline has elapsed and the version still matches; unmarked files are protected.</summary>
    ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation);
    // Before KVI publication: verify the object and clear a deletion mark, which changes its version; an unmarked object
    // keeps its version. False means absent: the caller must allocate a fresh identity, never recreate a retired key.
    ValueTask<bool> ProtectPureValuesAsync(uint fileId, KeyIndexFileSource source, CancellationToken cancellation);
}
