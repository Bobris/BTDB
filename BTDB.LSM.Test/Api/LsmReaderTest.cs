using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
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

    [Fact]
    public async Task RegisteredReaderKeepsItsTables()
    {
        var storage = new InMemoryLsmStorage();
        using var writer = await LsmKeyValueDB.OpenAsync(Options(storage, true) with
        {
            MemtableSize = 1 << 20, ReaderHeartbeatInterval = TimeSpan.FromSeconds(1),
            DeletionGracePeriod = TimeSpan.Zero
        });
        var random = new Random(5);
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (var commit = 1; commit <= 50; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, false) with
        {
            RegisterReader = true, ReaderHeartbeatInterval = TimeSpan.FromSeconds(1),
            BlockCache = new LsmBlockCache(64 * 1024)
        });
        // The registry entry expires after three heartbeats, which must outlast a delayed heartbeat timer on a loaded
        // test machine for the whole test; an expired entry rightly lets the writer delete the old tables.
        var expected = new SortedDictionary<string, string>(model, StringComparer.Ordinal);
        var old = reader.StartReadOnlyTransaction();
        var tablesBefore = storage.Keys("tables/").Length;
        try
        {
        await reader.RefreshAsync();
        for (var commit = 51; commit <= 100; commit++) model = await Commit(writer, random, (ulong)commit, model);
        writer.CreateKvi(default);
        await writer.Compact(default);
        await Task.Delay(200);
        Assert.Equal(expected, Content(old));
        }
        finally
        {
            old.Dispose();
        }

        await reader.RefreshAsync();
        await Task.Delay(200);
        // Once the reader moved on, the writer deletes what only the old snapshot used; deletion runs in the
        // background, so wait for it with a generous limit for loaded test machines.
        await writer.Compact(default);
        for (var wait = 0; wait < 100 && storage.Keys("tables/").Length >= tablesBefore + 2; wait++)
        {
            await Task.Delay(100);
            await writer.Compact(default);
        }

        Assert.True(storage.Keys("tables/").Length < tablesBefore + 2, string.Join(",", storage.Keys("tables/")));
        using var tr = reader.StartReadOnlyTransaction();
        Assert.Equal(model, Content(tr));
    }
}
