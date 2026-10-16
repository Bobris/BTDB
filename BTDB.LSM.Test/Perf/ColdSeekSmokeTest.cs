using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Cold seeks and key index lookups over a bottom level plus several level 0 tables on simulated object storage
/// latency; prints latencies for Measurements.md and asserts nothing about speed.
public class ColdSeekSmokeTest(ITestOutputHelper output)
{
    static byte[] Key(long i)
    {
        var key = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(key, (ulong)i * 0x9E3779B97F4A7C15UL);
        return key;
    }

    /// BTDB_LSM_SEEK_KEYS scales the bottom level; Measurements.md used 200,000 keys.
    [Fact]
    public async Task ColdSeeksAcrossComponents()
    {
        var keyCount = int.Parse(Environment.GetEnvironmentVariable("BTDB_LSM_SEEK_KEYS") ?? "40000");
        var storage = new InMemoryLsmStorage();
        var options = new LsmKeyValueDBOptions
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false,
            MemtableSize = 64 << 20, Level0CompactionTrigger = 8, Level0StopWritesTrigger = 16
        };
        using (var db = await LsmKeyValueDB.OpenAsync(options))
        {
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

            output.WriteLine(db.CalcStats());
        }

        var latency = new LatencyLsmStorage(storage);
        using var reader = await LsmKeyValueDB.OpenAsync(options with
        {
            Storage = latency, AcquireWriterLeaseOnOpen = false, BlockCacheSize = 16 << 20
        });
        using var transaction = reader.StartReadOnlyTransaction();
        using var seek = transaction.CreateCursor();
        var random = new Random(1);
        var count = transaction.GetKeyValueCount();
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < 100; i++) seek.Find(Key(random.NextInt64(keyCount)), 0);
        var seeks = stopwatch.Elapsed.TotalMilliseconds / 100;
        stopwatch.Restart();
        for (var i = 0; i < 100; i++) Assert.True(seek.FindKeyIndex(random.NextInt64(count)));
        var indexes = stopwatch.Elapsed.TotalMilliseconds / 100;
        output.WriteLine($"cold Find {seeks:F1} ms, cold FindKeyIndex {indexes:F1} ms");
        // The same keys and indexes again, now cached.
        var keys = new byte[100][];
        var indexes2 = new long[100];
        random = new Random(1);
        for (var i = 0; i < 100; i++) keys[i] = Key(random.NextInt64(keyCount));
        for (var i = 0; i < 100; i++) indexes2[i] = random.NextInt64(count);
        stopwatch.Restart();
        for (var round = 0; round < 100; round++)
        foreach (var key in keys) seek.Find(key, 0);
        var warmSeeks = stopwatch.Elapsed.TotalMicroseconds / 10_000;
        stopwatch.Restart();
        for (var round = 0; round < 10; round++)
        foreach (var index in indexes2) seek.FindKeyIndex(index);
        output.WriteLine($"warm Find {warmSeeks:F1} µs, warm FindKeyIndex {stopwatch.Elapsed.TotalMicroseconds / 1000:F1} µs");
        output.WriteLine(latency.Report());
    }
}
