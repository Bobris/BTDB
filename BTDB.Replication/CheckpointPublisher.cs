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
/// Production storage/authority integration belongs to the canonical mutation lane, not this ordering helper.
/// </summary>
internal interface ICheckpointStorage : IRemoteFileCollection
{
    ValueTask EnsurePureValuesAsync(uint remoteFileId, KeyIndexFileSource source, CancellationToken cancellation);
    ValueTask EnsureTransactionLogAsync(uint fileId, ulong requiredLength, CancellationToken cancellation);
    ValueTask PublishKeyIndexAsync(KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> pureValueFileIds,
        CancellationToken cancellation);
}

/// <summary>
/// Publish a fixed native checkpoint using the session file set for verified local-to-remote placements.
/// The live database keeps its local IDs; KVI publication starts only after all dependencies are confirmed.
/// </summary>
internal sealed class CheckpointPublisher(ReplicationFileSet files)
{
    readonly SemaphoreSlim _lane = new(1);

    /// <summary>The caller owns the snapshot until completion and must establish canonical validity of its cut.</summary>
    public async ValueTask PublishAsync(KeyIndexSnapshot snapshot, CancellationToken remoteCancellation = default)
    {
        var cancellation = remoteCancellation; // Deliberately independent of the local compactor token.
        await _lane.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            var map = new Dictionary<uint, uint>();
            var destinations = new HashSet<uint>();
            foreach (var source in snapshot.Sources)
                if (source.FileType == KVFileType.TransactionLog) destinations.Add(source.FileId);
            foreach (var source in snapshot.Sources)
            {
                cancellation.ThrowIfCancellationRequested();
                if (source.FileType == KVFileType.TransactionLog)
                {
                    // Never remap or publish arbitrary local TRL bytes as canonical history.
                    await files.Remote.EnsureTransactionLogAsync(source.FileId, source.Length, cancellation).ConfigureAwait(false);
                    continue;
                }
                var remoteId = await files.PublishPureValuesAsync(source, cancellation).ConfigureAwait(false);
                if (!destinations.Add(remoteId)) throw new InvalidOperationException("Remote file IDs collide.");
                map.Add(source.FileId, remoteId);
            }
            cancellation.ThrowIfCancellationRequested();
            // This is the first possible KVI staging/upload request, after every dependency has succeeded.
            await files.Remote.PublishKeyIndexAsync(snapshot, new ReadOnlyDictionary<uint, uint>(map), cancellation)
                .ConfigureAwait(false);
        }
        finally { _lane.Release(); }
    }
}
