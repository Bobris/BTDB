using System.Diagnostics;
using System.Threading.Tasks;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Not a benchmark: a smoke check that sequential scans and indexed reads stay fast over several levels.
public class LsmScanSmokeTest(ITestOutputHelper output)
{
    [Fact]
    public async Task ScanOverLevelsIsLinear()
    {
        const int keyCount = 200_000;
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), MemtableSize = 256 * 1024, BlockSize = 4096,
            Level0CompactionTrigger = 4, LevelBaseBytes = 1024 * 1024, LevelMultiplier = 4,
            TargetFileSize = 256 * 1024
        });
        var stopwatch = Stopwatch.StartNew();
        for (var batch = 0; batch < keyCount / 1000; batch++)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 1000; i++)
            {
                var k = (batch * 7919 + i * 104729) % keyCount;
                cursor.CreateOrUpdateKeyValue([(byte)(k >> 16), (byte)(k >> 8), (byte)k], [1, 2, 3, 4]);
            }

            tr.Commit();
        }

        output.WriteLine($"insert {stopwatch.ElapsedMilliseconds} ms");
        output.WriteLine(db.CalcStats());
        using var read = db.StartReadOnlyTransaction();
        using var scan = read.CreateCursor();
        stopwatch.Restart();
        var count = 0;
        if (scan.FindFirstKey([]))
        {
            do
            {
                Assert.Equal(count, scan.GetKeyIndex());
                count++;
            } while (scan.FindNextKey([]));
        }

        output.WriteLine($"scan {count} keys {stopwatch.ElapsedMilliseconds} ms");
        Assert.Equal(read.GetKeyValueCount(), count);
        stopwatch.Restart();
        for (var i = 0; i < 2000; i++) Assert.True(scan.FindKeyIndex((long)i * 97 % count));
        output.WriteLine($"2000 FindKeyIndex {stopwatch.ElapsedMilliseconds} ms");
    }
}
