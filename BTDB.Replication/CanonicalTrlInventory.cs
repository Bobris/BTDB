using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>
/// Selected remote TRL inventory adapter. Follow selected links, never an object listing or its maximum ID. The caller supplies
/// the database-scoped genesis identity; a missing published root is an error, not permission to initialize again.
/// A version change or missing dependency fails this attempt; the owner may rediscover in a new attempt.
/// </summary>
internal sealed class CanonicalTrlInventory : ICheckpointStorage
{
    readonly ICanonicalTrlStorage _storage;
    readonly Dictionary<uint, TrlHead> _byId;

    CanonicalTrlInventory(ICanonicalTrlStorage storage, Dictionary<uint, TrlHead> byId, TrlHead tail)
    {
        _storage = storage;
        _byId = byId;
        Tail = tail;
    }

    public TrlHead Tail { get; }

    public static async ValueTask<CanonicalTrlInventory> DiscoverAsync(ICanonicalTrlStorage storage,
        TrlSuccessor genesis, CancellationToken cancellation = default)
    {
        var byId = new Dictionary<uint, TrlHead>();
        TrlHead tail;
        var keys = new HashSet<string>(StringComparer.Ordinal);
        var current = genesis;
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
            tail = new(current.FileId, current.Key, state);
            byId.Add(current.FileId, tail);
            if (state.Metadata.Next is not { } next) break;
            previousId = current.FileId;
            previousTerm = state.Metadata.Term;
            current = next;
        }
        return new(storage, byId, tail);
    }

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        foreach (var head in _byId.Values)
        {
            cancellation.ThrowIfCancellationRequested();
            // No trustworthy checksum was supplied by the TRL storage seam: do not reuse local cache bytes.
            yield return new(head.FileId, KVFileType.TransactionLog, head.State.Length, head.State.Token,
                head.State.Metadata.Next != null, null);
        }
        await Task.CompletedTask;
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

    public ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) =>
        throw new NotSupportedException("The selected inventory does not publish files.");
    public ValueTask PublishKeyIndexAsync(uint remoteFileId, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map,
        CancellationToken cancellation) => throw new NotSupportedException("The selected inventory does not publish files.");
}
