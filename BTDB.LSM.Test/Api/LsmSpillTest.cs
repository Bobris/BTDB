using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Writing transactions above MaxTransactionMemory (ImplementationPlan.md, section 7.5).
public class LsmSpillTest
{
    static LsmKeyValueDBOptions Options(InMemoryLsmStorage storage, bool background, bool native = true) => new()
    {
        Storage = storage, BackgroundCompaction = background, NativeMemtable = native, MemtableSize = 256 * 1024,
        MaxTransactionMemory = 32 * 1024, BlockSize = 1024, ValueSeparationThreshold = 300,
        WalChunkTargetSize = 4096, WalFlushInterval = TimeSpan.FromMilliseconds(1),
        RefreshRetryInterval = TimeSpan.FromMilliseconds(5)
    };

    static byte[] Key(int i) => [(byte)(i >> 8), (byte)i];

    static byte[] Value(Random random)
    {
        var value = new byte[random.Next(4) == 0 ? random.Next(300, 700) : random.Next(1, 60)];
        random.NextBytes(value);
        return value;
    }

    static SortedDictionary<string, string> Content(IKeyValueDBTransaction tr)
    {
        using var cursor = tr.CreateCursor();
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (cursor.FindFirstKey([]))
            do result.Add(Convert.ToHexString(cursor.SlowGetKey()), Convert.ToHexString(cursor.SlowGetValue()));
            while (cursor.FindNextKey([]));
        return result;
    }

    static SortedDictionary<string, string> Content(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        return Content(tr);
    }

    static long Bytes(InMemoryLsmStorage storage, string prefix) =>
        storage.Keys(prefix).Sum(k => (long)storage.Peek(k)!.Length);

    /// Commits small transactions so the shared memtable holds keys a large transaction then updates and erases.
    static async Task<SortedDictionary<string, string>> WriteBase(LsmKeyValueDB db, Random random)
    {
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        for (var commit = 1; commit <= 5; commit++)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 100; i++)
            {
                var key = Key(random.Next(4000));
                var value = Value(random);
                cursor.CreateOrUpdateKeyValue(key, value);
                model[Convert.ToHexString(key)] = Convert.ToHexString(value);
            }

            tr.SetCommitUlong((ulong)commit);
            tr.Commit();
        }

        return model;
    }

    /// Updates, inserts and erases keys and ranges in one transaction far above MaxTransactionMemory, checking its
    /// own view on the way.
    static void WriteLarge(IKeyValueDBTransaction tr, Random random, SortedDictionary<string, string> model)
    {
        using var cursor = tr.CreateCursor();
        for (var op = 0; op < 4000; op++)
        {
            var choice = random.Next(100);
            if (choice < 80)
            {
                var key = Key(random.Next(4000));
                var value = Value(random);
                cursor.CreateOrUpdateKeyValue(key, value);
                model[Convert.ToHexString(key)] = Convert.ToHexString(value);
            }
            else if (choice < 99)
            {
                var key = Key(random.Next(4000));
                if (!cursor.FindExactKey(key)) continue;
                cursor.EraseCurrent();
                model.Remove(Convert.ToHexString(key));
            }
            else
            {
                // Short ranges erase points, long ones write a range tombstone.
                var from = random.Next(4000);
                var to = from + (random.Next(2) == 0 ? 5 : 300);
                using var end = tr.CreateCursor();
                if (cursor.FindFirstKey([]) && cursor.FindKeyIndex(0) && FindFrom(cursor, Key(from)) &&
                    FindLastBefore(end, Key(to)) && cursor.GetKeyIndex() <= end.GetKeyIndex())
                {
                    cursor.EraseUpTo(end);
                    foreach (var key in model.Keys.Where(k =>
                                 string.CompareOrdinal(k, Convert.ToHexString(Key(from))) >= 0 &&
                                 string.CompareOrdinal(k, Convert.ToHexString(Key(to))) < 0).ToArray())
                        model.Remove(key);
                }
            }

            if (op % 500 == 0) Assert.Equal(model.Count, tr.GetKeyValueCount());
        }

        tr.SetCommitUlong(100);
        Assert.Equal(model.Count, tr.GetKeyValueCount());
        Assert.Equal(model, Content(tr));
    }

    static bool FindFrom(IKeyValueDBCursor cursor, byte[] key) =>
        cursor.Find(key, 0) switch
        {
            FindResult.Exact or FindResult.Next => true,
            FindResult.Previous => cursor.FindNextKey([]),
            _ => false
        };

    static bool FindLastBefore(IKeyValueDBCursor cursor, byte[] key) =>
        cursor.Find(key, 0) switch
        {
            FindResult.Previous => true,
            FindResult.Exact or FindResult.Next => cursor.FindPreviousKey([]),
            _ => false
        };

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public async Task LargeTransactionCommitsItsSpilledTablesWithoutWalRecord(bool background, bool native)
    {
        var storage = new InMemoryLsmStorage();
        var random = new Random(1);
        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, background, native));
        SortedDictionary<string, string> model;
        using (var writer = await LsmKeyValueDB.OpenAsync(Options(storage, background, native) with
               {
                   AcquireWriterLeaseOnOpen = true
               }))
        {
            model = await WriteBase(writer, random);
            await writer.FlushAsync();
            var committed = new SortedDictionary<string, string>(model, StringComparer.Ordinal);
            var walBytes = Bytes(storage, "wal/");
            using (var tr = await writer.StartWritingTransaction())
            {
                WriteLarge(tr, random, model);
                // The spilled tables exist, but no published state has them yet.
                Assert.NotEmpty(storage.Keys("tables/"));
                Assert.Equal(committed, Content(writer));
                tr.Commit();
            }

            Assert.Equal(model, Content(writer));
            Assert.True(Bytes(storage, "wal/") - walBytes < 1024, "The large commit was written to the WAL.");
            await writer.WaitForDurabilityAsync();
            await reader.WaitForCommitUlongAsync(100);
            Assert.Equal(model, Content(reader));

            // Small commits continue in the next generation and readers follow them.
            using (var tr = await writer.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key(5000), [1]);
                model[Convert.ToHexString(Key(5000))] = "01";
                tr.SetCommitUlong(101);
                tr.Commit();
            }

            await writer.FlushAsync();
            await reader.WaitForCommitUlongAsync(101);
            Assert.Equal(model, Content(reader));
        }

        using var reopened = await LsmKeyValueDB.OpenAsync(Options(storage, background, native) with
        {
            AcquireWriterLeaseOnOpen = true
        });
        Assert.Equal(model, Content(reopened));
        await reopened.Compact(default);
        Assert.Equal(model, Content(reopened));
    }

    static int Level0Runs(LsmKeyValueDB db) =>
        System.Text.RegularExpressions.Regex.Match(db.CalcStats(), @"^Level0:.* (\d+) runs$",
            System.Text.RegularExpressions.RegexOptions.Multiline) is { Success: true } match
            ? int.Parse(match.Groups[1].Value)
            : 0;

    [Fact]
    public async Task SpilledSegmentsMergeIntoFewRunsAndCommitAsOne()
    {
        var storage = new InMemoryLsmStorage();
        using var db = await LsmKeyValueDB.OpenAsync(Options(storage, false) with
        {
            AcquireWriterLeaseOnOpen = true, MaxTransactionMemory = 8 * 1024, Level0CompactionTrigger = 100,
            Level0StopWritesTrigger = 200
        });
        var random = new Random(4);
        var model = await WriteBase(db, random);
        var runsBefore = Level0Runs(db);
        using (var tr = await db.StartWritingTransaction())
        {
            var transaction = (LsmKeyValueDBTransaction)tr;
            var mostRuns = 0;
            using (var cursor = tr.CreateCursor())
            {
                for (var i = 0; i < 6000; i++)
                {
                    var key = Key(random.Next(6000));
                    var value = Value(random);
                    cursor.CreateOrUpdateKeyValue(key, value);
                    model[Convert.ToHexString(key)] = Convert.ToHexString(value);
                    mostRuns = Math.Max(mostRuns, transaction.SpilledRunCount);
                }
            }

            // Runs of equal weight merge four at a time: at most three of each weight.
            Assert.True(transaction.SpilledSegmentCount >= 64, transaction.SpilledSegmentCount.ToString());
            Assert.InRange(mostRuns, 1, 3 * (int)Math.Ceiling(Math.Log(transaction.SpilledSegmentCount, 4)));
            Assert.Equal(model, Content(tr));
            tr.Commit();
        }

        // Sealing the shared memtable may have flushed one run of its own.
        Assert.InRange(Level0Runs(db) - runsBefore, 1, 2);
        Assert.Equal(model, Content(db));
    }

    [Fact]
    public async Task RolledBackSpilledTransactionLeavesNothingBehind()
    {
        var storage = new InMemoryLsmStorage();
        var random = new Random(2);
        using var db = await LsmKeyValueDB.OpenAsync(Options(storage, false) with { AcquireWriterLeaseOnOpen = true });
        var model = await WriteBase(db, random);
        db.CreateKvi(default);
        var before = storage.Keys("tables/").Concat(storage.Keys("values/")).ToHashSet();
        string[] spilled;
        using (var tr = await db.StartWritingTransaction())
        {
            WriteLarge(tr, random, new(model, StringComparer.Ordinal));
            spilled = storage.Keys("tables/").Concat(storage.Keys("values/")).Where(k => !before.Contains(k)).ToArray();
            Assert.Contains(spilled, k => k.StartsWith("tables/"));
            Assert.Contains(spilled, k => k.StartsWith("values/"));
        }

        // No reader ever saw them, so they go at once, whatever the grace period.
        Assert.Empty(spilled.Intersect(storage.Keys("")));
        Assert.Equal(model, Content(db));
    }

    [Fact]
    public async Task OtherSnapshotsDoNotSeeASpillingTransaction()
    {
        var storage = new InMemoryLsmStorage();
        var random = new Random(3);
        using var db = await LsmKeyValueDB.OpenAsync(Options(storage, true) with { AcquireWriterLeaseOnOpen = true });
        var model = await WriteBase(db, random);
        using var before = db.StartReadOnlyTransaction();
        var changed = new SortedDictionary<string, string>(model, StringComparer.Ordinal);
        using (var tr = await db.StartWritingTransaction())
        {
            WriteLarge(tr, random, changed);
            using var during = db.StartReadOnlyTransaction();
            Assert.Equal(model, Content(during));
            tr.Commit();
        }

        Assert.Equal(model, Content(before));
        Assert.Equal(changed, Content(db));
    }
}
