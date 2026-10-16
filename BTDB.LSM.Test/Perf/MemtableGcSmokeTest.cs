using System;
using System.Diagnostics;
using System.Threading.Tasks;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Measures memtable GC cost for the native and the managed memtable; prints numbers for Measurements.md and asserts
/// nothing about speed.
public class MemtableGcSmokeTest(ITestOutputHelper output)
{
    [Theory]
    [InlineData(1000, true)]
    [InlineData(10, true)]
    [InlineData(1000, false)]
    [InlineData(10, false)]
    public async Task MemtableGcCost(int operationsPerTransaction, bool nativeMemtable)
    {
        const int keyCount = 1_000_000;
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), MemtableSize = 64 * 1024 * 1024, NativeMemtable = nativeMemtable,
            BlockCache = new(16 * 1024 * 1024)
        });
        GC.Collect();
        var gen0 = GC.CollectionCount(0);
        var gen1 = GC.CollectionCount(1);
        var gen2 = GC.CollectionCount(2);
        var pause = GC.GetTotalPauseDuration();
        var allocated = GC.GetTotalAllocatedBytes(true);
        var stopwatch = Stopwatch.StartNew();
        var key = new byte[16];
        var value = new byte[16];
        var random = new Random(1);
        for (var done = 0; done < keyCount; done += operationsPerTransaction)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < operationsPerTransaction; i++)
            {
                random.NextBytes(key);
                cursor.CreateOrUpdateKeyValue(key, value);
            }

            tr.Commit();
        }

        stopwatch.Stop();
        var info = GC.GetGCMemoryInfo();
        output.WriteLine(
            $"{(nativeMemtable ? "native" : "managed")} {operationsPerTransaction} ops/tx: " +
            $"{keyCount / stopwatch.Elapsed.TotalSeconds:F0} inserts/s, " +
            $"allocated {(GC.GetTotalAllocatedBytes(true) - allocated) / 1048576} MiB, " +
            $"gen0/1/2 {GC.CollectionCount(0) - gen0}/{GC.CollectionCount(1) - gen1}/{GC.CollectionCount(2) - gen2}, " +
            $"pause {(GC.GetTotalPauseDuration() - pause).TotalMilliseconds:F0} ms of {stopwatch.ElapsedMilliseconds} ms, " +
            $"heap {info.HeapSizeBytes / 1048576} MiB");
        output.WriteLine(db.CalcStats());
    }
}
