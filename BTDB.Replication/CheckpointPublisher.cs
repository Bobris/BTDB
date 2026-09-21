using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal sealed class RemoteFileConflictException() : IOException("Remote file SHA metadata does not match intended content.");

/// <summary>
/// Bound to one selected database/authority session. Allocations must be fresh in the remote inventory.
/// Ensure methods return only after exact content is confirmed; ambiguous outcomes retry/reconcile the same ID.
/// Adapters check the selected session authority before every dispatch, including individual KVI chunks.
/// Create only if absent, with whole-file SHA metadata bound atomically to content. Matching SHA confirms a retry;
/// missing/different SHA on an existing object throws RemoteFileConflictException. Never overwrite it.
/// </summary>
internal interface ICheckpointStorage : IRemoteFileCollection
{
    ValueTask EnsurePureValuesAsync(uint remoteFileId, KeyIndexFileSource source, CancellationToken cancellation);
    /// <summary>Publish or reconcile this exact chosen immutable KVI identity. A lost response retries the same
    /// ID, snapshot and mapping; verify existing content instead of overwriting or allocating another ID.</summary>
    ValueTask PublishKeyIndexAsync(uint remoteFileId, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> pureValueFileIds,
        CancellationToken cancellation);
}

internal enum CheckpointPublishResult { Published, Pending, AuthorityLost, Conflict }

/// <summary>
/// Publish a fixed native checkpoint using the session file set for verified local-to-remote placements.
/// The live database keeps its local IDs; KVI publication starts only after all dependencies are confirmed.
/// </summary>
internal sealed class CheckpointPublisher(ReplicationFileSet files, CanonicalTrlPublisher canonical, ICheckpointStorage? storage = null)
{
    sealed record PendingCheckpoint(KeyIndexSnapshot Snapshot, uint FileId, IReadOnlyDictionary<uint, uint> Map);

    readonly SemaphoreSlim _lane = new(1);
    PendingCheckpoint? _pending;
    readonly ICheckpointStorage _storage = storage ?? files.Remote;
    public PublishedCheckpoint? Published { get; private set; }

    /// <summary>The caller retains a snapshot from the canonical publisher's database until completion, including retries.
    /// Pending leaves the canonical intent intact and starts no KVI upload. Storage adapters must also check authority
    /// before individual upload requests and reconcile ambiguous PVL/KVI outcomes at the same chosen identity.</summary>
    public async ValueTask<CheckpointPublishResult> PublishAsync(KeyIndexSnapshot snapshot, CancellationToken remoteCancellation = default,
        bool retryPending = false)
    {
        var cancellation = remoteCancellation; // Deliberately independent of the local compactor token.
        await _lane.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            if (_pending != null && !ReferenceEquals(_pending.Snapshot, snapshot))
                throw new InvalidOperationException("Resolve the pending checkpoint before publishing another snapshot.");
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
            // A pending upload already has its fixed mapping and validated destinations.
            var map = _pending == null ? new Dictionary<uint, uint>() : null;
            var destinations = _pending == null ? new HashSet<uint>() : null;
            if (destinations != null)
                foreach (var source in snapshot.Sources)
                    if (source.FileType == KVFileType.TransactionLog) destinations.Add(source.FileId);
            foreach (var source in snapshot.Sources)
            {
                cancellation.ThrowIfCancellationRequested();
                if (!canonical.HasAuthority) return CheckpointPublishResult.AuthorityLost;
                // The canonical lane selected the complete recovery prefix, including historical TRL dependencies.
                // File existence/length alone would also accept an unselected prepared successor.
                if (source.FileType == KVFileType.TransactionLog) continue;
                var remoteId = await files.PublishPureValuesAsync(source, _storage, cancellation).ConfigureAwait(false);
                if (_pending != null)
                {
                    if (_pending.Map[source.FileId] != remoteId)
                        throw new InvalidOperationException("The pending checkpoint placement changed.");
                }
                else
                {
                    if (!destinations!.Add(remoteId)) throw new InvalidOperationException("Remote file IDs collide.");
                    map!.Add(source.FileId, remoteId);
                }
            }
            cancellation.ThrowIfCancellationRequested();
            if (!canonical.HasAuthority) return CheckpointPublishResult.AuthorityLost;
            if (_pending == null)
            {
                var id = await files.AllocateRemoteFileIdAsync(cancellation, _storage).ConfigureAwait(false);
                if (destinations!.Contains(id))
                    throw new InvalidOperationException("Remote file IDs collide.");
                _pending = new(snapshot, id, new ReadOnlyDictionary<uint, uint>(map!));
            }
            cancellation.ThrowIfCancellationRequested();
            if (!canonical.HasAuthority) return CheckpointPublishResult.AuthorityLost;
            // Retain the exact identity and mapping across exceptions, including a lost successful response.
            // The adapter must reconcile the same immutable object before returning success.
            await _storage.PublishKeyIndexAsync(_pending.FileId, snapshot, _pending.Map, cancellation)
                .ConfigureAwait(false);
            var dependencies = new HashSet<uint>(_pending.Map.Values);
            foreach (var source in snapshot.Sources)
                if (source.FileType == KVFileType.TransactionLog) dependencies.Add(source.FileId);
            Published = new(_pending.FileId, snapshot.TransactionLogFileId, dependencies);
            _pending = null;
            return CheckpointPublishResult.Published;
        }
        catch (RemoteFileConflictException)
        {
            canonical.Fence();
            return CheckpointPublishResult.Conflict;
        }
        finally { _lane.Release(); }
    }
}
