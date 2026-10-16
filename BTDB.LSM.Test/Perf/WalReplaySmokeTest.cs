using System;
using System.Diagnostics;
using System.Threading.Tasks;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Opening and refreshing across many WAL chunks on simulated object storage latency; prints times for
/// Measurements.md and asserts nothing about speed.
public class WalReplaySmokeTest(ITestOutputHelper output)
{
    [Fact]
    public async Task OpenAndRefreshAcrossManyChunks()
    {
        const int commits = 400;
        var storage = new InMemoryLsmStorage();
        var options = new LsmKeyValueDBOptions
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false, MemtableSize = 64 << 20
        };
        var latency = new LatencyLsmStorage(storage);
        using var follower = await LsmKeyValueDB.OpenAsync(options with
        {
            Storage = latency, AcquireWriterLeaseOnOpen = false
        });
        var writer = await LsmKeyValueDB.OpenAsync(options);
        for (var i = 0; i < commits; i++)
        {
            using var tr = await writer.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([(byte)(i >> 8), (byte)i], new byte[1000]);
            tr.SetCommitUlong((ulong)i + 1);
            tr.Commit();
        }

        writer.SimulateCrash();
        Assert.Equal(commits, storage.Keys("wal/").Length - 1);
        var stopwatch = Stopwatch.StartNew();
        Assert.Equal((ulong)commits, await follower.RefreshAsync());
        var refresh = stopwatch.Elapsed.TotalMilliseconds;
        stopwatch.Restart();
        using var reader = await LsmKeyValueDB.OpenAsync(options with
        {
            Storage = latency, AcquireWriterLeaseOnOpen = false
        });
        var open = stopwatch.Elapsed.TotalMilliseconds;
        using (var tr = reader.StartReadOnlyTransaction()) Assert.Equal((ulong)commits, tr.GetCommitUlong());
        output.WriteLine($"{commits} WAL chunks: refresh {refresh:F0} ms, open {open:F0} ms");
    }
}
