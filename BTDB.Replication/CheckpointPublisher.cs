using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Bound to one selected database/authority session. Allocations must be fresh in the remote inventory.
/// Ensure methods return only after exact content is confirmed; ambiguous outcomes retry/reconcile the same ID.
/// Adapters check the selected session authority before every dispatch, including individual KVI chunks.
/// </summary>
internal interface ICheckpointStorage : IRemoteFileCollection
{
    ValueTask EnsurePureValuesAsync(uint remoteFileId, KeyIndexFileSource source, CancellationToken cancellation);
    ValueTask PublishKeyIndexAsync(KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> pureValueFileIds,
        CancellationToken cancellation);
}

internal enum CheckpointPublishResult { Published, Pending, AuthorityLost, Conflict }

/// <summary>
/// Publish a fixed native checkpoint using the session file set for verified local-to-remote placements.
/// The live database keeps its local IDs; KVI publication starts only after all dependencies are confirmed.
/// </summary>
internal sealed class CheckpointPublisher(ReplicationFileSet files, CanonicalTrlPublisher canonical)
{
    readonly SemaphoreSlim _lane = new(1);

    /// <summary>The caller retains a snapshot from the canonical publisher's database until completion, including retries.
    /// Pending leaves the canonical intent intact and starts no KVI upload. Storage adapters must also check authority
    /// before individual upload requests and reconcile ambiguous PVL/KVI outcomes at the same reserved identity.</summary>
    public async ValueTask<CheckpointPublishResult> PublishAsync(KeyIndexSnapshot snapshot, CancellationToken remoteCancellation = default,
        bool retryPending = false)
    {
        var cancellation = remoteCancellation; // Deliberately independent of the local compactor token.
        await _lane.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            if (!canonical.HasAuthority) return CheckpointPublishResult.AuthorityLost;
            var cut = new TransactionLogPosition(snapshot.TransactionLogFileId, snapshot.TransactionLogOffset);
            var published = await canonical.PublishThroughAsync(cut, retryPending, cancellation).ConfigureAwait(false);
            switch (published)
            {
                case TrlPublishResult.Pending: return CheckpointPublishResult.Pending;
                case TrlPublishResult.AuthorityLost: return CheckpointPublishResult.AuthorityLost;
                case TrlPublishResult.Conflict: return CheckpointPublishResult.Conflict;
                case TrlPublishResult.Idle:
                case TrlPublishResult.Published: break;
                default: throw new InvalidOperationException("Canonical checkpoint cut was not established.");
            }
            var map = new Dictionary<uint, uint>();
            var destinations = new HashSet<uint>();
            foreach (var source in snapshot.Sources)
                if (source.FileType == KVFileType.TransactionLog) destinations.Add(source.FileId);
            foreach (var source in snapshot.Sources)
            {
                cancellation.ThrowIfCancellationRequested();
                if (!canonical.HasAuthority) return CheckpointPublishResult.AuthorityLost;
                // The canonical lane selected the complete recovery prefix, including historical TRL dependencies.
                // File existence/length alone would also accept an unselected prepared successor.
                if (source.FileType == KVFileType.TransactionLog) continue;
                var remoteId = await files.PublishPureValuesAsync(source, cancellation).ConfigureAwait(false);
                if (!destinations.Add(remoteId)) throw new InvalidOperationException("Remote file IDs collide.");
                map.Add(source.FileId, remoteId);
            }
            cancellation.ThrowIfCancellationRequested();
            if (!canonical.HasAuthority) return CheckpointPublishResult.AuthorityLost;
            // This is the first possible KVI staging/upload request, after every dependency has succeeded.
            await files.Remote.PublishKeyIndexAsync(snapshot, new ReadOnlyDictionary<uint, uint>(map), cancellation)
                .ConfigureAwait(false);
            return CheckpointPublishResult.Published;
        }
        finally { _lane.Release(); }
    }
}
