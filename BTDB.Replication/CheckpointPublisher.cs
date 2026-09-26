using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

public sealed class RemoteFileConflictException() : IOException("Remote file SHA metadata does not match intended content.");

internal enum CheckpointPublishResult { Published, Pending, AuthorityLost, Conflict }

/// <summary>
/// Publish a fixed native checkpoint using the session file set for verified local-to-remote placements.
/// The live database keeps its local IDs; KVI publication starts only after all dependencies are confirmed.
/// </summary>
internal sealed class CheckpointPublisher(ReplicationFileSet files, CanonicalTrlPublisher canonical, IReplicationStorage? storage = null)
{
    sealed record PendingCheckpoint(KeyIndexSnapshot Snapshot, uint FileId, IReadOnlyDictionary<uint, uint> Map);

    readonly SemaphoreSlim _lane = new(1);
    PendingCheckpoint? _pending;
    readonly IReplicationStorage _storage = storage ?? files.PublicationStorage;
    public PublishedCheckpoint? Published { get; private set; }

    /// <summary>The caller retains a snapshot from the canonical publisher's database until completion, including retries.
    /// Pending leaves the canonical intent intact and starts no KVI upload. Storage adapters must also check authority
    /// before individual upload requests and reconcile ambiguous PVL/KVI outcomes at the same chosen identity.</summary>
    public async ValueTask<CheckpointPublishResult> PublishAsync(KeyIndexSnapshot snapshot, CancellationToken remoteCancellation = default,
        bool retryPending = false, Action<int, int>? progress = null)
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
            progress?.Invoke(0, 0);
            // A pending upload already has its fixed mapping and validated destinations.
            var map = _pending == null ? new Dictionary<uint, uint>() : null;
            var destinations = _pending == null ? new HashSet<uint>() : null;
            if (destinations != null)
                foreach (var source in snapshot.Sources)
                    if (source.FileType == KVFileType.TransactionLog) destinations.Add(source.FileId);
            var sourceIndex = 0;
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
                progress?.Invoke(1, ++sourceIndex);
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
            progress?.Invoke(2, 0);
            // Retain the exact identity and mapping across exceptions, including a lost successful response.
            // The adapter must reconcile the same immutable object before returning success.
            await _storage.PublishKeyIndexAsync(_pending.FileId, snapshot, _pending.Map, cancellation)
                .ConfigureAwait(false);
            progress?.Invoke(3, 0);
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
