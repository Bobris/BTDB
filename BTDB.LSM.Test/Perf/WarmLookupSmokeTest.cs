using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Cached cursor operations over a bottom level and four level 0 tables, from one thread and from eight; prints
/// latencies and throughput for Measurements.md and asserts nothing about speed.
public class WarmLookupSmokeTest(ITestOutputHelper output)
{
    static byte[] Key(long i)
    {
        var key = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(key, (ulong)i * 0x9E3779B97F4A7C15UL);
        return key;
    }

    /// BTDB_LSM_LOOKUP_KEYS scales the bottom level; Measurements.md used 200,000 keys.
    [Fact]
    public async Task WarmCursorOperations()
    {
        var keyCount = int.Parse(Environment.GetEnvironmentVariable("BTDB_LSM_LOOKUP_KEYS") ?? "20000");
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false,
            MemtableSize = 64 << 20, Level0CompactionTrigger = 8, Level0StopWritesTrigger = 16,
            BlockCacheSize = 512 << 20
        });
        var value = new byte[100];
        for (var round = 0; round < 5; round++)
        {
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < (round == 0 ? keyCount : keyCount / 10); i++)
                    cursor.CreateOrUpdateKeyValue(Key(round * 1_000_000L + i), value);
                tr.Commit();
            }

            if (round == 0) await db.Compact(default);
            else db.CreateKvi(default);
        }

        using var transaction = db.StartReadOnlyTransaction();
        using var c = transaction.CreateCursor();
        var count = transaction.GetKeyValueCount();
        // The first pass warms the cache; the second is reported.
        for (var pass = 0; pass < 2; pass++)
        {
            var random = new Random(1);
            var stopwatch = Stopwatch.StartNew();
            for (var i = 0; i < 20_000; i++) c.Find(Key(random.NextInt64(keyCount)), 0);
            var exact = stopwatch.Elapsed.TotalMicroseconds / 20_000;
            stopwatch.Restart();
            for (var i = 0; i < 20_000; i++)
            {
                var key = Key(random.NextInt64(keyCount));
                key[7] ^= 1;
                c.Find(key, 0);
                c.FindNextKey([]);
            }

            var inexact = stopwatch.Elapsed.TotalMicroseconds / 20_000;
            stopwatch.Restart();
            for (var i = 0; i < 2_000; i++)
            {
                c.FindKeyIndex(random.NextInt64(count));
                c.FindNextKey([]);
            }

            var index = stopwatch.Elapsed.TotalMicroseconds / 2_000;
            if (pass == 1)
                output.WriteLine($"exact Find {exact:F2} µs, inexact Find+Next {inexact:F2} µs, " +
                                 $"FindKeyIndex+Next {index:F2} µs");
        }

        foreach (var threads in new[] { 1, 8 })
        {
            var stopwatch = Stopwatch.StartNew();
            Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, worker =>
            {
                using var tr = db.StartReadOnlyTransaction();
                using var cursor = tr.CreateCursor();
                var random = new Random(worker);
                for (var i = 0; i < 100_000; i++) cursor.FindExactKey(Key(random.NextInt64(keyCount)));
            });
            output.WriteLine($"{threads} threads: {threads * 100_000 / stopwatch.Elapsed.TotalSeconds:F0} " +
                             "exact lookups/s");
        }
    }
}
