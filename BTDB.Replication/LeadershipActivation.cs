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
    /// independently during activation.</summary>
    public static async ValueTask<IReadOnlyList<CanonicalTrlPublisher>?> ActivateAsync(SelectedLeadership selected,
        IReadOnlyList<ActivationDatabase> databases, CancellationToken cancellation = default,
        Action<int, int, uint, ulong>? progress = null, IDictionary<string, TransactionLogPosition>? validated = null)
    {
        var required = new HashSet<string>(selected.DatabaseNames, StringComparer.Ordinal);
        if (required.Count != databases.Count) throw new ArgumentException("Activation must include every selected database exactly once.");
        foreach (var database in databases)
            if (!required.Remove(database.Name)) throw new ArgumentException("Activation database set differs from the selected leader record.");
        var publishers = new List<CanonicalTrlPublisher>();
        var completed = false;
        try
        {
            for (var index = 0; index < databases.Count; index++)
            {
                var database = databases[index];
                RequireAuthority(selected);
                if (database.RestoredBase.FileId == 0)
                {
                    if (await database.Storage.ResolveRecoveryRootAsync(database.Genesis, cancellation).ConfigureAwait(false) != database.Genesis ||
                        await database.Storage.ReadAsync(database.Genesis.Key, cancellation).ConfigureAwait(false) != null)
                        throw new InvalidDataException("Initialization was published; restore its fixed history before activation.");
                    publishers.Add(new(database.Database, database.Capture, database.Storage, selected.Authority,
                        selected.Term, id => id == database.Genesis.FileId ? database.Genesis.Key : database.KeyForFile(id)));
                    progress?.Invoke(index, 2, 0, 0);
                    continue;
                }
                var inventory = await CanonicalTrlInventory.DiscoverAsync(database.Storage, database.Genesis, cancellation,
                    progress == null ? null : id => progress(index, 0, id, 0))
                    .ConfigureAwait(false);
                if (inventory.Tail.State.Metadata.Term > selected.Term)
                {
                    selected.Authority.Fence();
                    throw new InvalidDataException("Canonical history has a newer authority term.");
                }
                // Adoption needs the local tail bytes, so a lagging candidate stops before fencing this database.
                if (!await ValidateAsync(database, inventory, selected, validated, cancellation,
                        progress == null ? null : (id, offset) => progress(index, 1, id, offset)).ConfigureAwait(false))
                    return null;
                var publisher = new CanonicalTrlPublisher(database.Database, database.Capture, database.Storage,
                    selected.Authority, selected.Term, database.KeyForFile, inventory.Tail);
                publishers.Add(publisher);
                var result = await publisher.AdoptAsync(cancellation).ConfigureAwait(false);
                if (result is not (TrlPublishResult.Adopted or TrlPublishResult.Idle))
                    throw new IOException("Canonical adoption changed or is unresolved; rediscover before activation.");
                progress?.Invoke(index, 2, 0, 0);
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
        var localBuffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        var remoteBuffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        var expectedId = baseline.FileId;
        uint previousId = 0;
        var found = false;
        try
        {
            await foreach (var file in inventory.EnumerateAsync(cancellation).ConfigureAwait(false))
            {
                if (file.FileId < baseline.FileId) continue;
                RequireAuthority(selected);
                if (file.FileId != expectedId) throw new InvalidDataException("Canonical history omits the retained base or continuation.");
                found = true;
                // Only complete local transactions count; the physical file may hold an unfinished one. Capture reports
                // nothing before the first local commit, when the verified restored base is the complete local end.
                var local = database.Capture.Completed;
                if (Order(local) < Order(baseline)) local = baseline;
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
                while (offset < compareEnd)
                {
                    var count = (int)Math.Min(256 * 1024ul, compareEnd - offset);
                    source.RandomRead(localBuffer.AsSpan(0, count), offset, false);
                    var read = await inventory.ReadAsync(file, offset, remoteBuffer.AsMemory(0, count), cancellation)
                        .ConfigureAwait(false);
                    cancellation.ThrowIfCancellationRequested();
                    RequireAuthority(selected);
                    if (read != count || !localBuffer.AsSpan(0, count).SequenceEqual(remoteBuffer.AsSpan(0, count)))
                        throw new InvalidDataException("Candidate diverges from canonical Blob history; restore is required.");
                    offset += (uint)count;
                    if (validated != null) validated[database.Name] = new(file.FileId, checked((uint)offset));
                    progress?.Invoke(file.FileId, offset);
                }
                if (compareEnd < file.Length) return false;
                previousId = file.FileId;
                // Follow the selected link: native allocation may skip IDs reserved by legacy files.
                if (file.IsSealed) expectedId = inventory.GetHead(file.FileId).State.Metadata.Next!.FileId;
            }
            if (!found) throw new InvalidDataException("Canonical history does not contain the restored base.");
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(localBuffer);
            ArrayPool<byte>.Shared.Return(remoteBuffer);
        }
    }

    static ulong Order(TransactionLogPosition position) => ((ulong)position.FileId << 32) | position.Offset;

    static void RequireAuthority(SelectedLeadership selected)
    {
        if (!selected.Authority.IsValid) throw new InvalidOperationException("Activation lost lease authority.");
    }
}
