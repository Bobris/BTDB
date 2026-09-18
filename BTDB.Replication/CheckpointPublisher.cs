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
internal interface ICheckpointStorage
{
    uint ReservePureValuesFileId();
    ValueTask EnsurePureValuesAsync(uint remoteFileId, KeyIndexFileSource source, CancellationToken cancellation);
    ValueTask EnsureTransactionLogAsync(uint fileId, ulong requiredLength, CancellationToken cancellation);
    ValueTask PublishKeyIndexAsync(KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> pureValueFileIds,
        CancellationToken cancellation);
}

/// <summary>
/// Session-local publication receipts for whole sealed PVLs; local IDs must not be reused in the session. Reuses downloaded/uploaded files across checkpoints;
/// the live database still uses its original IDs. No bytes, roots or pins are retained after PublishAsync returns.
/// </summary>
internal sealed class CheckpointPublisher(ICheckpointStorage storage)
{
    sealed class Placement(KeyIndexFileSource source, uint remoteId, bool confirmed)
    {
        public readonly ulong Length = source.Length;
        public readonly uint RemoteId = remoteId;
        public bool Confirmed = confirmed;
    }

    readonly Dictionary<uint, Placement> _placements = new();
    readonly SemaphoreSlim _lane = new(1);

    /// <summary>Called during restore only after the whole sealed PVL was verified against Blob Storage.
    /// Peer downloads qualify only when the same complete file is known to exist in Blob Storage.</summary>
    public void RememberDownloadedPureValues(KeyIndexFileSource source)
    {
        if (source.FileType != KVFileType.PureValues || source.Length != source.File.GetSize())
            throw new ArgumentException("Only a complete sealed PVL can be remembered.", nameof(source));
        if (!_placements.TryAdd(source.FileId, new(source, source.FileId, true)))
            throw new InvalidOperationException("The file already has a publication placement.");
    }

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
                    await storage.EnsureTransactionLogAsync(source.FileId, source.Length, cancellation).ConfigureAwait(false);
                    continue;
                }
                if (!_placements.TryGetValue(source.FileId, out var placement))
                {
                    var remoteId = storage.ReservePureValuesFileId();
                    if (remoteId == 0) throw new InvalidOperationException("Remote file ID cannot be zero.");
                    placement = new(source, remoteId, false);
                    _placements.Add(source.FileId, placement);
                }
                if (placement.Length != source.Length)
                    throw new InvalidOperationException("A sealed local PVL identity changed; start a new restore session.");
                if (!destinations.Add(placement.RemoteId)) throw new InvalidOperationException("Remote file IDs collide.");
                if (!placement.Confirmed)
                {
                    await storage.EnsurePureValuesAsync(placement.RemoteId, source, cancellation).ConfigureAwait(false);
                    placement.Confirmed = true;
                }
                map.Add(source.FileId, placement.RemoteId);
            }
            cancellation.ThrowIfCancellationRequested();
            // This is the first possible KVI staging/upload request, after every dependency has succeeded.
            await storage.PublishKeyIndexAsync(snapshot, new ReadOnlyDictionary<uint, uint>(map), cancellation)
                .ConfigureAwait(false);
        }
        finally { _lane.Release(); }
    }
}
