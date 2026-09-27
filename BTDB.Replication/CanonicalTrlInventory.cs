using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Selected remote TRL inventory adapter. Follow selected links, never an object listing or its maximum ID. The caller supplies
/// the database-scoped genesis identity; storage may resolve a retained root from a published checkpoint. A missing
/// published root is an error, not permission to initialize again.
/// A version change or missing dependency fails this attempt; the owner may rediscover in a new attempt.
/// </summary>
public sealed class CanonicalTrlInventory : IRemoteFileCollection
{
    readonly IReplicationStorage _storage;
    // Selected links in chain order, which is also ascending native ID order.
    readonly List<TrlHead> _chain;
    readonly Dictionary<uint, TrlHead> _byId;

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
        var chain = new List<TrlHead>();
        var keys = new HashSet<string>(StringComparer.Ordinal);
        var root = await storage.ResolveRecoveryRootAsync(genesis, cancellation).ConfigureAwait(false);
        var current = root;
        ulong previousTerm = 0;
        uint previousId = 0;
        while (true)
        {
            cancellation.ThrowIfCancellationRequested();
            // Validate the supplied root as well as every selected link without serializing metadata.
            TrlMetadata.Validate(current);
            if (current.FileId <= previousId || !keys.Add(current.Key))
                throw new InvalidDataException("Canonical TRL links repeat a key or do not advance native IDs.");
            var state = await storage.ReadAsync(current.Key, cancellation).ConfigureAwait(false)
                ?? throw new FileNotFoundException("A selected canonical TRL is missing.", current.Key);
            if (state.Length == 0 || string.IsNullOrEmpty(state.Token) || state.Metadata.Term == 0 ||
                state.Metadata.Term < previousTerm)
                throw new InvalidDataException("Invalid canonical TRL state or decreasing authority term.");
            chain.Add(new(current.FileId, current.Key, state));
            progress?.Invoke(current.FileId);
            if (state.Metadata.Next is not { } next) break;
            previousId = current.FileId;
            previousTerm = state.Metadata.Term;
            current = next;
        }
        return new(storage, chain, root);
    }

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        foreach (var head in _chain)
        {
            cancellation.ThrowIfCancellationRequested();
            // No trustworthy checksum was supplied by the TRL storage seam: do not reuse local cache bytes.
            yield return new(head.FileId, KVFileType.TransactionLog, head.State.Length, head.State.Token,
                head.State.Metadata.Next != null, null);
        }
    }

    public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (!_byId.TryGetValue(file.FileId, out var head) || file.Version != head.State.Token ||
            file.Length != head.State.Length || offset > file.Length)
            throw new InvalidDataException("Read is outside the selected canonical version.");
        var count = (int)Math.Min((ulong)buffer.Length, file.Length - offset);
        if (count != 0)
            await _storage.ReadRangeAsync(head.Key, head.State.Token, (uint)offset, buffer[..count], cancellation)
                .ConfigureAwait(false);
        return count;
    }

}
