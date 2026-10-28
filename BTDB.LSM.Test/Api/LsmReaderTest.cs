using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Readers in other processes (ImplementationPlan.md, section 15), simulated by separate instances on one storage.
public class LsmReaderTest
{
    static LsmKeyValueDBOptions Options(InMemoryLsmStorage storage, bool writer) => new()
    {
        Storage = storage, AcquireWriterLeaseOnOpen = writer, MemtableSize = 8 * 1024, BlockSize = 512,
        Level0CompactionTrigger = 2, LevelBaseBytes = 24 * 1024, LevelMultiplier = 3, LevelCount = 4,
        TargetFileSize = 8 * 1024, ValueSeparationThreshold = 300, WalChunkTargetSize = 2048,
        WalFlushInterval = TimeSpan.FromMilliseconds(1), DeletionGracePeriod = TimeSpan.FromSeconds(2),
        RefreshRetryInterval = TimeSpan.FromMilliseconds(5), ManifestListInterval = TimeSpan.Zero, LeaseDuration = TimeSpan.FromSeconds(3),
        LeaseSafetyMargin = TimeSpan.FromMilliseconds(500), LeaseRetryInterval = TimeSpan.FromMilliseconds(20)
    };

    static byte[] Key(int i) => [(byte)(i >> 8), (byte)i];

    static SortedDictionary<string, string> Content(IKeyValueDBTransaction tr)
    {
        using var cursor = tr.CreateCursor();
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (cursor.FindFirstKey([]))
            do result.Add(Convert.ToHexString(cursor.SlowGetKey()), Convert.ToHexString(cursor.SlowGetValue()));
            while (cursor.FindNextKey([]));
        return result;
    }

    static async Task<SortedDictionary<string, string>> Commit(LsmKeyValueDB db, Random random, ulong commitUlong,
        SortedDictionary<string, string> model)
    {
        using var tr = await db.StartWritingTransaction();
        using var cursor = tr.CreateCursor();
        for (var op = random.Next(1, 25); op > 0; op--)
        {
            var key = Key(random.Next(200));
            var hex = Convert.ToHexString(key);
            if (random.Next(5) == 0)
            {
                if (!cursor.FindExactKey(key)) continue;
                cursor.EraseCurrent();
                model.Remove(hex);
            }
            else
            {
                var value = new byte[random.Next(3) == 0 ? random.Next(800) : 4];
                random.NextBytes(value);
                cursor.CreateOrUpdateKeyValue(key, value);
                model[hex] = Convert.ToHexString(value);
            }
        }

        tr.SetCommitUlong(commitUlong);
        tr.Commit();
        return new(model, StringComparer.Ordinal);
    }

    [Fact]
    public async Task ReaderFollowsWriterThroughFlushesCompactionsAndWalDeletion()
    {
        var storage = new InMemoryLsmStorage();
        using var writer = await LsmKeyValueDB.OpenAsync(Options(storage, true));
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, false));
        var random = new Random(1);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var history = new List<SortedDictionary<string, string>> { new(model) };
        IKeyValueDBTransaction? held = null;
        var heldCommit = 0;
        try
        {
        for (var commit = 1; commit <= 300; commit++)
        {
            history.Add(await Commit(writer, random, (ulong)commit, model));
            if (commit % 7 != 0) continue;
            await writer.FlushAsync();
            await reader.WaitForCommitUlongAsync((ulong)commit);
            using (var tr = reader.StartReadOnlyTransaction())
            {
                var seen = (int)tr.GetCommitUlong();
                Assert.True(seen >= commit);
                Assert.Equal(history[seen], Content(tr));
            }

            // An older snapshot stays readable and unchanged while the reader refreshes.
            if (held != null) Assert.Equal(history[heldCommit], Content(held));
            held?.Dispose();
            held = reader.StartReadOnlyTransaction();
            heldCommit = (int)held.GetCommitUlong();
        }
        }
        finally
        {
            held?.Dispose();
        }

        Assert.False(reader.HoldsWriterLease);
    }

    [Fact]
    public async Task ReaderCatchesUpAfterTheWalItRead()
    {
        var storage = new InMemoryLsmStorage();
        using var writer = await LsmKeyValueDB.OpenAsync(Options(storage, true));
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, false));
        var random = new Random(2);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (var commit = 1; commit <= 20; commit++) model = await Commit(writer, random, (ulong)commit, model);
        await writer.FlushAsync();
        await reader.WaitForCommitUlongAsync(20);
        // The writer moves far ahead: flushes, compacts and deletes the WAL the reader would continue from.
        for (var commit = 21; commit <= 400; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        await writer.Compact(default);
        Assert.Empty(storage.Keys("wal/0000000000000000"));
        Assert.Equal(400ul, await reader.RefreshAsync());
        using var tr = reader.StartReadOnlyTransaction();
        Assert.Equal(model, Content(tr));
    }

    [Fact]
    public async Task ReaderSurvivesWriterTakeover()
    {
        var storage = new InMemoryLsmStorage();
        var first = await LsmKeyValueDB.OpenAsync(Options(storage, true));
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, false));
        var random = new Random(3);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (var commit = 1; commit <= 30; commit++) model = await Commit(first, random, (ulong)commit, model);
        await first.FlushAsync();
        await reader.WaitForCommitUlongAsync(30);
        first.SimulateCrash();
        storage.ExpireAllLeases();
        using var second = await LsmKeyValueDB.OpenAsync(Options(storage, true));
        for (var commit = 31; commit <= 60; commit++) model = await Commit(second, random, (ulong)commit, model);
        await second.FlushAsync();
        await reader.WaitForCommitUlongAsync(60);
        using var tr = reader.StartReadOnlyTransaction();
        Assert.Equal(model, Content(tr));
    }

    [Fact]
    public async Task UnregisteredOldSnapshotFailsClearlyAfterGracePeriod()
    {
        var storage = new InMemoryLsmStorage();
        using var writer = await LsmKeyValueDB.OpenAsync(Options(storage, true) with
        {
            MemtableSize = 1 << 20, DeletionGracePeriod = TimeSpan.Zero
        });
        var random = new Random(4);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (var commit = 1; commit <= 50; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, false) with
        {
            BlockCache = new LsmBlockCache(64 * 1024)
        });
        using var old = reader.StartReadOnlyTransaction();
        for (var commit = 51; commit <= 100; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        await writer.Compact(default);
        var error = Assert.Throws<BTDBException>(() => Content(old));
        Assert.Contains("no longer exists", error.Message);
    }

    sealed class ShiftedTimeProvider(TimeSpan offset) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => base.GetUtcNow() + offset;
    }

    sealed class ManualTimeProvider : TimeProvider
    {
        public DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    /// Fails the next WAL chunk read once asked to, as a transient storage error.
    sealed class FailingWalReadStorage(InMemoryLsmStorage inner) : ILsmStorage
    {
        public bool FailNextWalRead;

        public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation)
        {
            if (!FailNextWalRead || !key.StartsWith("wal/", StringComparison.Ordinal))
                return inner.TryReadAsync(key, cancellation);
            FailNextWalRead = false;
            throw new System.IO.IOException("Storage is unreachable.");
        }

        public ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation) => inner.CreateAsync(key, content, cancellation);
        public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);
        public void ReadRange(string key, long offset, Span<byte> destination) => inner.ReadRange(key, offset, destination);
        public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
        public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
        public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
        public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
        public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
    }

    [Fact]
    public async Task RefreshFailingAfterANewerManifestNeverPublishesAMixedState()
    {
        var storage = new InMemoryLsmStorage();
        var time = new ManualTimeProvider();
        var failing = new FailingWalReadStorage(storage);
        using var writer = await LsmKeyValueDB.OpenAsync(Options(storage, true) with
        {
            MemtableSize = 1 << 20, TimeProvider = time
        });
        var random = new Random(8);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var models = new Dictionary<ulong, SortedDictionary<string, string>>();
        async Task CommitRange(ulong from, ulong to)
        {
            for (var commit = from; commit <= to; commit++)
            {
                model = await Commit(writer, random, commit, model);
                models[commit] = new(model, StringComparer.Ordinal);
            }

            await writer.FlushAsync();
        }

        await CommitRange(1, 20);
        writer.CreateKvi(default);
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, false) with
        {
            Storage = failing, MemtableSize = 1 << 20, ManifestListInterval = TimeSpan.FromHours(1)
        });
        // The reader holds commits 21 to 40 in its memtable; this refresh also lists manifests, so the next miss does
        // not list again for an hour.
        await CommitRange(21, 40);
        Assert.Equal(40UL, await reader.RefreshAsync());
        writer.CreateKvi(default);
        var flushedThem = storage.Keys("manifest/").Max(StringComparer.Ordinal)!;
        // The refresh reads the manifest that flushed them, drops their memtable from its replay and then fails.
        failing.FailNextWalRead = true;
        await Assert.ThrowsAnyAsync<Exception>(async () => await reader.RefreshAsync());
        // That manifest is replaced twice and deleted.
        await CommitRange(41, 50);
        writer.CreateKvi(default);
        time.Now += TimeSpan.FromMinutes(2);
        await CommitRange(51, 60);
        writer.CreateKvi(default);
        Assert.DoesNotContain(flushedThem, storage.Keys("manifest/"));
        // Before the fix the next refresh missed the deleted manifest and published the older tables without the
        // dropped memtable, under the newer commit metadata; reading then failed on tables deleted since.
        await reader.RefreshAsync();
        using var tr = reader.StartReadOnlyTransaction();
        Assert.Equal(models[tr.GetCommitUlong()], Content(tr));
    }

    [Fact]
    public async Task ReplacedManifestsStayForTheRetentionEvenWithoutGracePeriod()
    {
        var storage = new InMemoryLsmStorage();
        var time = new ManualTimeProvider();
        using var writer = await LsmKeyValueDB.OpenAsync(Options(storage, true) with
        {
            MemtableSize = 1 << 20, DeletionGracePeriod = TimeSpan.Zero, TimeProvider = time
        });
        var random = new Random(6);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (var commit = 1; commit <= 20; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        var replaced = storage.Keys("manifest/");
        for (var commit = 21; commit <= 40; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        // Before the fix a zero grace period deleted replaced manifests at once, so a delayed create could take their
        // names again.
        Assert.All(replaced, key => Assert.Contains(key, storage.Keys("manifest/")));
        time.Now += TimeSpan.FromMinutes(2);
        for (var commit = 41; commit <= 60; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        Assert.All(replaced, key => Assert.DoesNotContain(key, storage.Keys("manifest/")));
        using var tr = writer.StartReadOnlyTransaction();
        Assert.Equal(model, Content(tr));
    }

    static LsmManifest NewestManifest(InMemoryLsmStorage storage)
    {
        var key = storage.Keys("manifest/").Max(StringComparer.Ordinal)!;
        return LsmManifest.Decode(storage.Peek(key), key);
    }

    [Fact]
    public async Task PendingDeletionsKeepTheirTimeAcrossWriterTakeover()
    {
        var storage = new InMemoryLsmStorage();
        var options = Options(storage, true) with { MemtableSize = 1 << 20, DeletionGracePeriod = TimeSpan.FromHours(1) };
        var random = new Random(5);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        using (var first = await LsmKeyValueDB.OpenAsync(options))
        {
            for (var commit = 1; commit <= 50; commit++) model = await Commit(first, random, (ulong)commit, model);
            first.CreateKvi(default);
            for (var commit = 51; commit <= 100; commit++) model = await Commit(first, random, (ulong)commit, model);
            await first.Compact(default);
        }

        var pending = NewestManifest(storage).PendingDeletions;
        Assert.Contains(pending, d => d.Kind == LsmObjectKind.Table);
        Assert.Contains(pending, d => d.Kind == LsmObjectKind.Manifest);
        // A table no manifest ever published, as a crashed writer leaves it.
        storage.CreateAsync("tables/0000000000999999.sst", new byte[] { 1 }, default).AsTask().Wait();

        // Within the grace period of the first writer's clock only the unpublished table may go.
        using (var second = await LsmKeyValueDB.OpenAsync(options with
               {
                   TimeProvider = new ShiftedTimeProvider(TimeSpan.FromMinutes(30))
               }))
        {
            await second.Compact(default);
            Assert.DoesNotContain("tables/0000000000999999.sst", storage.Keys("tables/"));
            Assert.True(storage.Keys("tables/").Length > NewestManifest(storage).Tables.Length);
        }

        // A takeover does not restart the period: once it passed on the first writer's clock, everything goes.
        using var third = await LsmKeyValueDB.OpenAsync(options with
        {
            TimeProvider = new ShiftedTimeProvider(TimeSpan.FromHours(2))
        });
        await third.Compact(default);
        var manifest = NewestManifest(storage);
        Assert.Equal(manifest.Tables.Length, storage.Keys("tables/").Length);
        Assert.Equal(manifest.ValueFiles.Length, storage.Keys("values/").Length);
        Assert.True(storage.Keys("manifest/").Length <= 2, string.Join(",", storage.Keys("manifest/")));
        using var tr = third.StartReadOnlyTransaction();
        Assert.Equal(model, Content(tr));
    }
}
