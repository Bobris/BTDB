using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Snapshot of the shared TRL namespace. Native headers and KVI references determine recovery; no term directory,
/// successor metadata or checkpoint sidecar participates. A missing dependency fails this attempt; a newer version of
/// a selected TRL still serves its selected bytes (see <see cref="ReadAsync"/>).
/// </summary>
public sealed class CanonicalTrlInventory : IRemoteFileCollection
{
    readonly IReplicationStorage _storage;
    // Shared native IDs in ascending order.
    readonly List<TrlHead> _chain;
    readonly Dictionary<uint, TrlHead> _byId;
    // Newer versions of selected TRLs that were verified to still cover their selected length.
    readonly ConcurrentDictionary<uint, string> _followed = new();

    CanonicalTrlInventory(IReplicationStorage storage, List<TrlHead> chain, TrlSuccessor root)
    {
        _storage = storage;
        _chain = chain;
        _byId = chain.ToDictionary(head => head.FileId);
        Root = root;
    }

    public TrlHead Tail => _chain[^1];

    internal TrlSuccessor Root { get; }

    internal TrlHead GetHead(uint fileId) => _byId.TryGetValue(fileId, out var head) ? head :
        throw new FileNotFoundException("TRL is outside the selected canonical inventory.");

    internal bool IsFrom(IReplicationStorage storage) => ReferenceEquals(_storage, storage);

    public static ValueTask<CanonicalTrlInventory> DiscoverAsync(IReplicationStorage storage,
        TrlSuccessor genesis, CancellationToken cancellation = default) => DiscoverAsync(storage, genesis, cancellation, null);

    internal static async ValueTask<CanonicalTrlInventory> DiscoverAsync(IReplicationStorage storage,
        TrlSuccessor genesis, CancellationToken cancellation, Action<uint>? progress)
    {
        TrlFileName.Validate(genesis);
        var chain = new List<TrlHead>();
        await foreach (var head in storage.EnumerateTrlsAsync(cancellation).ConfigureAwait(false))
        {
            if (TrlFileName.FileIdFromKey(head.Key) != head.FileId || head.State.Length == 0 ||
                string.IsNullOrEmpty(head.State.Token)) throw new InvalidDataException("Invalid canonical TRL identity or state.");
            chain.Add(head);
            progress?.Invoke(head.FileId);
        }
        chain.Sort((a, b) => a.FileId.CompareTo(b.FileId));
        if (chain.Count == 0) throw new FileNotFoundException("Published TRL history is missing.", genesis.Key);
        for (var i = 1; i < chain.Count; i++)
            if (chain[i].FileId == chain[i - 1].FileId) throw new InvalidDataException("Duplicate canonical TRL identity.");
        // Native PreviousFileId headers and KVI references select the retained recovery closure during open.
        return new(storage, chain, new(chain[0].Key, chain[0].FileId));
    }

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        for (var i = 0; i < _chain.Count; i++)
        {
            cancellation.ThrowIfCancellationRequested();
            var head = _chain[i];
            // Only a TRL sealed with its whole-file checksum lets a restore reuse cached bytes; the tail may grow.
            var isSealed = i + 1 < _chain.Count;
            yield return new(head.FileId, KVFileType.TransactionLog, head.State.Length, head.State.Token,
                isSealed, isSealed ? head.State.Sha256 : null);
        }
    }

    public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (!_byId.TryGetValue(file.FileId, out var head) || file.Version != head.State.Token ||
            file.Length != head.State.Length || offset > file.Length)
            throw new InvalidDataException("Read is outside the selected canonical version.");
        var count = (int)Math.Min((ulong)buffer.Length, file.Length - offset);
        if (count == 0) return count;
        var token = _followed.GetValueOrDefault(file.FileId, head.State.Token);
        for (var followed = 0; ; followed++)
        {
            try
            {
                await _storage.ReadRangeAsync(head.Key, token, (uint)offset, buffer[..count], cancellation).ConfigureAwait(false);
                return count;
            }
            catch (IOException) when (followed < MaximumFollowedVersions)
            {
                // Canonical TRLs are append-only: appends, sealing and adoption keep every published byte, and deletion
                // marks change only metadata. A leader publishing during a restore therefore changes the version of the
                // selected tail (and cleanup those of older TRLs) without changing the selected bytes. Continue from a
                // newer version that still covers the selection; a deleted or shorter object still fails this attempt.
                var current = await _storage.ReadAsync(head.Key, cancellation).ConfigureAwait(false);
                if (current == null || current.Token == token || current.Length < head.State.Length) throw;
                _followed[file.FileId] = token = current.Token;
            }
        }
    }

    // A leader appending faster than one metadata and range round trip can change the version again before the read;
    // each retry needs a newer version, so this bounds only a leader outpacing the reader, never a stuck one.
    const int MaximumFollowedVersions = 16;
}
