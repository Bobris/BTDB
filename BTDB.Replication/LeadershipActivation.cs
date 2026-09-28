using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>RestoredBase is the fixed verified startup cut, never the follower's advancing acknowledgement.
/// The owner retains native files from that cut until takeover or restart, and supplies only compatible databases.
/// Genesis selects the first retained canonical link (which may follow a restored KVI).</summary>
public sealed record ActivationDatabase(string Name, BTreeKeyValueDB Database, TransactionLogCapture Capture,
    IReplicationStorage Storage, TrlSuccessor Genesis, TransactionLogPosition RestoredBase,
    Func<uint, string> KeyForFile);

internal static class LeadershipActivation
{
    /// <summary>Returns publishers only after every required database is validated and adopted. I/O/version races
    /// retry through fresh discovery; divergence requires ordinary restore. Returns null while a candidate's complete
    /// local prefix matches canonical history but has not reached its end yet: the host keeps executing inputs and the
    /// caller retries under the same lease. <paramref name="validated"/> keeps verified prefixes across those retries.
    /// The caller must not publish from any database until this method succeeds. Lease maintenance continues
    /// independently during activation. Progress reports (stage, database, step, file, offset): stage 0 discovers
    /// (step 0) and validates (step 1) each database, stage 1 adopts them.</summary>
    public static async ValueTask<IReadOnlyList<CanonicalTrlPublisher>?> ActivateAsync(SelectedLeadership selected,
        IReadOnlyList<ActivationDatabase> databases, CancellationToken cancellation = default,
        Action<int, int, int, uint, ulong>? progress = null, IDictionary<string, TransactionLogPosition>? validated = null)
    {
        var required = new HashSet<string>(selected.DatabaseNames, StringComparer.Ordinal);
        if (required.Count != databases.Count) throw new ArgumentException("Activation must include every selected database exactly once.");
        foreach (var database in databases)
            if (!required.Remove(database.Name)) throw new ArgumentException("Activation database set differs from the selected leader record.");
        // Validate every database before adopting any: adoption needs the local tail bytes, and a lagging database
        // must not make each retry fence and adopt the databases before it again.
        var tails = new TrlHead?[databases.Count];
        for (var index = 0; index < databases.Count; index++)
        {
            var database = databases[index];
            RequireAuthority(selected);
            if (database.RestoredBase.FileId == 0)
            {
                if (await database.Storage.ResolveRecoveryRootAsync(database.Genesis, cancellation).ConfigureAwait(false) != database.Genesis ||
                    await database.Storage.ReadAsync(database.Genesis.Key, cancellation).ConfigureAwait(false) != null)
                    throw new InvalidDataException("Initialization was published; restore its fixed history before activation.");
                continue;
            }
            var inventory = await CanonicalTrlInventory.DiscoverAsync(database.Storage, database.Genesis, cancellation,
                progress == null ? null : id => progress(0, index, 0, id, 0))
                .ConfigureAwait(false);
            if (!await ValidateAsync(database, inventory, selected, validated, cancellation,
                    progress == null ? null : (id, offset) => progress(0, index, 1, id, offset)).ConfigureAwait(false))
                return null;
            tails[index] = inventory.Tail;
        }
        var publishers = new List<CanonicalTrlPublisher>();
        var completed = false;
        try
        {
            for (var index = 0; index < databases.Count; index++)
            {
                var database = databases[index];
                RequireAuthority(selected);
                if (tails[index] is not { } tail)
                {
                    publishers.Add(new(database.Database, database.Capture, database.Storage, selected.Authority,
                        selected.Term, id => id == database.Genesis.FileId ? database.Genesis.Key : database.KeyForFile(id)));
                    progress?.Invoke(1, index, 0, 0, 0);
                    continue;
                }
                var publisher = new CanonicalTrlPublisher(database.Database, database.Capture, database.Storage,
                    selected.Authority, selected.Term, database.KeyForFile, tail);
                publishers.Add(publisher);
                var result = await publisher.AdoptAsync(cancellation).ConfigureAwait(false);
                if (result is not (TrlPublishResult.Adopted or TrlPublishResult.Idle))
                    throw new IOException("Canonical adoption changed or is unresolved; rediscover before activation.");
                progress?.Invoke(1, index, 0, 0, 0);
            }
            cancellation.ThrowIfCancellationRequested();
            RequireAuthority(selected);
            completed = true;
            return publishers;
        }
        finally
        {
            if (!completed)
                foreach (var publisher in publishers) publisher.Dispose();
        }
    }

    /// <summary>Compare the complete local prefix with canonical history. False means every compared byte matches,
    /// but local execution has not produced the canonical end yet; mismatching or impossible local history throws.</summary>
    static async ValueTask<bool> ValidateAsync(ActivationDatabase database, CanonicalTrlInventory inventory,
        SelectedLeadership selected, IDictionary<string, TransactionLogPosition>? validated, CancellationToken cancellation,
        Action<uint, ulong>? progress)
    {
        var baseline = database.RestoredBase;
        if (baseline.FileId == 0) throw new ArgumentException("Activation requires a verified restored base.");
        // Canonical TRLs only grow, so a prefix verified by an earlier attempt of this lease stays verified.
        var resume = validated != null && validated.TryGetValue(database.Name, out var verified) ? verified : baseline;
        uint previousId = 0;
        var found = false;
        await foreach (var file in inventory.EnumerateAsync(cancellation).ConfigureAwait(false))
        {
            if (file.FileId < baseline.FileId) continue;
            RequireAuthority(selected);
            if (!found && file.FileId != baseline.FileId) throw new InvalidDataException("Canonical history omits the retained base.");
            found = true;
            // Only complete local transactions count; the physical file may hold an unfinished one. Capture reports
            // nothing before the first local commit, when the verified restored base is the complete local end.
            var local = database.Capture.Completed;
            if (local < baseline) local = baseline;
            if (file.FileId > local.FileId)
            {
                // Local execution has not rotated into this continuation yet. A different local continuation diverged.
                if (local.FileId != previousId)
                    throw new InvalidDataException("Candidate continues canonical history in another TRL; restore is required.");
                return false;
            }
            var source = database.Database.FileCollection.GetFile(file.FileId)
                ?? throw new InvalidDataException("Candidate lacks canonical TRL bytes; restore is required.");
            var localEnd = file.FileId == local.FileId ? local.Offset : source.GetSize();
            var offset = file.FileId == baseline.FileId ? (ulong)baseline.Offset : 0;
            if (offset > file.Length || (file.FileId < local.FileId && source.GetSize() < file.Length) ||
                (file.IsSealed && (file.FileId < local.FileId ? source.GetSize() != file.Length : localEnd > file.Length)))
                throw new InvalidDataException("Candidate does not match the selected canonical prefix; restore is required.");
            if (file.FileId < resume.FileId) offset = file.Length;
            else if (file.FileId == resume.FileId) offset = Math.Max(offset, resume.Offset);
            var compareEnd = Math.Min(file.Length, localEnd);
            await CompareRangeAsync(inventory, file, source, offset, compareEnd, selected, end =>
            {
                if (validated != null) validated[database.Name] = new(file.FileId, checked((uint)end));
                progress?.Invoke(file.FileId, end);
            }, cancellation).ConfigureAwait(false);
            if (compareEnd < file.Length) return false;
            previousId = file.FileId;
        }
        if (!found) throw new InvalidDataException("Canonical history does not contain the restored base.");
        return true;
    }

    const int BlockSize = 256 * 1024;
    const int ParallelReads = 4;

    /// <summary>Compare local bytes [offset, end) with the selected canonical version, keeping several Blob reads in
    /// flight; blocks are verified and reported in order, so a retry resumes after the last verified block.</summary>
    static async ValueTask CompareRangeAsync(CanonicalTrlInventory inventory, RemoteFile file, IFileCollectionFile source,
        ulong offset, ulong end, SelectedLeadership selected, Action<ulong> verified, CancellationToken cancellation)
    {
        if (offset >= end) return;
        using var reads = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        var local = ArrayPool<byte>.Shared.Rent(BlockSize);
        var pending = new Queue<(ulong Offset, int Count, byte[] Buffer, Task<int> Read)>();
        var free = new Stack<byte[]>();
        var requested = offset;
        try
        {
            while (true)
            {
                while (pending.Count < ParallelReads && requested < end)
                {
                    var count = (int)Math.Min(BlockSize, end - requested);
                    var buffer = free.Count != 0 ? free.Pop() : ArrayPool<byte>.Shared.Rent(BlockSize);
                    pending.Enqueue((requested, count, buffer,
                        inventory.ReadAsync(file, requested, buffer.AsMemory(0, count), reads.Token).AsTask()));
                    requested += (uint)count;
                }
                if (!pending.TryDequeue(out var block)) return;
                try
                {
                    var read = await block.Read.ConfigureAwait(false);
                    cancellation.ThrowIfCancellationRequested();
                    RequireAuthority(selected);
                    source.RandomRead(local.AsSpan(0, block.Count), block.Offset, false);
                    if (read != block.Count || !local.AsSpan(0, block.Count).SequenceEqual(block.Buffer.AsSpan(0, block.Count)))
                        throw new InvalidDataException("Candidate diverges from canonical Blob history; restore is required.");
                }
                finally { free.Push(block.Buffer); }
                verified(block.Offset + (uint)block.Count);
            }
        }
        finally
        {
            // Stop and drain outstanding reads before their pooled buffers are returned.
            await reads.CancelAsync().ConfigureAwait(false);
            foreach (var (_, _, buffer, read) in pending)
            {
                await ((Task)read).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                ArrayPool<byte>.Shared.Return(buffer);
            }
            foreach (var buffer in free) ArrayPool<byte>.Shared.Return(buffer);
            ArrayPool<byte>.Shared.Return(local);
        }
    }

    static void RequireAuthority(SelectedLeadership selected)
    {
        if (!selected.Authority.IsValid) throw new InvalidOperationException("Activation lost lease authority.");
    }
}
