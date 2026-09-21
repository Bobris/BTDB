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
internal sealed record ActivationDatabase(string Name, BTreeKeyValueDB Database, TransactionLogCapture Capture,
    ICanonicalTrlStorage Storage, TrlSuccessor Genesis, TransactionLogPosition RestoredBase,
    Func<uint, string> KeyForFile);

internal static class LeadershipActivation
{
    /// <summary>Returns publishers only after every required database is validated and adopted. I/O/version races
    /// retry through fresh discovery; divergence requires ordinary restore. The caller must not publish from any
    /// database until this method succeeds. Lease maintenance continues independently during activation.</summary>
    public static async ValueTask<IReadOnlyList<CanonicalTrlPublisher>> ActivateAsync(SelectedLeadership selected,
        IReadOnlyList<ActivationDatabase> databases, CancellationToken cancellation = default)
    {
        var required = new HashSet<string>(selected.DatabaseNames, StringComparer.Ordinal);
        if (required.Count != databases.Count) throw new ArgumentException("Activation must include every selected database exactly once.");
        foreach (var database in databases)
            if (!required.Remove(database.Name)) throw new ArgumentException("Activation database set differs from the selected leader record.");
        var publishers = new List<CanonicalTrlPublisher>();
        try
        {
            foreach (var database in databases)
            {
                RequireAuthority(selected);
                var inventory = await CanonicalTrlInventory.DiscoverAsync(database.Storage, database.Genesis, cancellation)
                    .ConfigureAwait(false);
                if (inventory.Tail.State.Metadata.Term > selected.Term)
                {
                    selected.Authority.Fence();
                    throw new InvalidDataException("Canonical history has a newer authority term.");
                }
                await ValidateAsync(database, inventory, selected, cancellation).ConfigureAwait(false);
                var publisher = new CanonicalTrlPublisher(database.Database, database.Capture, database.Storage,
                    selected.Authority, selected.Term, database.KeyForFile, inventory.Tail);
                publishers.Add(publisher);
                var result = await publisher.AdoptAsync(cancellation).ConfigureAwait(false);
                if (result is not (TrlPublishResult.Adopted or TrlPublishResult.Idle))
                    throw new IOException("Canonical adoption changed or is unresolved; rediscover before activation.");
            }
            cancellation.ThrowIfCancellationRequested();
            RequireAuthority(selected);
            return publishers;
        }
        catch
        {
            foreach (var publisher in publishers) publisher.Dispose();
            throw;
        }
    }

    static async ValueTask ValidateAsync(ActivationDatabase database, CanonicalTrlInventory inventory,
        SelectedLeadership selected, CancellationToken cancellation)
    {
        var baseline = database.RestoredBase;
        if (baseline.FileId == 0) throw new ArgumentException("Activation requires a verified restored base.");
        var localBuffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        var remoteBuffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        var expectedId = baseline.FileId;
        var found = false;
        try
        {
            await foreach (var file in inventory.EnumerateAsync(cancellation).ConfigureAwait(false))
            {
                if (file.FileId < baseline.FileId) continue;
                RequireAuthority(selected);
                if (file.FileId != expectedId) throw new InvalidDataException("Canonical history omits the retained base or continuation.");
                found = true;
                var source = database.Database.FileCollection.GetFile(file.FileId)
                    ?? throw new InvalidDataException("Candidate lacks canonical TRL bytes; restore is required.");
                var offset = file.FileId == baseline.FileId ? (ulong)baseline.Offset : 0;
                if (offset > file.Length || source.GetSize() < file.Length ||
                    (file.IsSealed && source.GetSize() != file.Length))
                    throw new InvalidDataException("Candidate does not cover the selected canonical prefix; restore is required.");
                while (offset < file.Length)
                {
                    var count = (int)Math.Min(256 * 1024ul, file.Length - offset);
                    source.RandomRead(localBuffer.AsSpan(0, count), offset, false);
                    var read = await inventory.ReadAsync(file, offset, remoteBuffer.AsMemory(0, count), cancellation)
                        .ConfigureAwait(false);
                    cancellation.ThrowIfCancellationRequested();
                    RequireAuthority(selected);
                    if (read != count || !localBuffer.AsSpan(0, count).SequenceEqual(remoteBuffer.AsSpan(0, count)))
                        throw new InvalidDataException("Candidate diverges from canonical Blob history; restore is required.");
                    offset += (uint)count;
                }
                if (file.IsSealed) expectedId = checked(file.FileId + ((file.FileId & 1) == 0 ? 1u : 2u));
            }
            if (!found) throw new InvalidDataException("Canonical history does not contain the restored base.");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(localBuffer);
            ArrayPool<byte>.Shared.Return(remoteBuffer);
        }
    }

    static void RequireAuthority(SelectedLeadership selected)
    {
        if (!selected.Authority.IsValid) throw new InvalidOperationException("Activation lost lease authority.");
    }
}
