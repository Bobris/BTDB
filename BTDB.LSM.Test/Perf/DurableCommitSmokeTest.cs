using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Durable single-key commits from one and from several concurrent writers on simulated object storage latency;
/// prints throughput for Measurements.md and asserts nothing about speed.
public class DurableCommitSmokeTest(ITestOutputHelper output)
{
    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public async Task DurableCommitThroughput(int writers)
    {
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new LatencyLsmStorage(new InMemoryLsmStorage()), AcquireWriterLeaseOnOpen = true
        });
        db.DurableTransactions = true;
        var commits = 0;
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var stopwatch = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, writers).Select(writer => Task.Run(async () =>
        {
            var key = new byte[8];
            while (!stop.IsCancellationRequested)
            {
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                BitConverter.TryWriteBytes(key, Interlocked.Increment(ref commits));
                cursor.CreateOrUpdateKeyValue(key, key);
                tr.Commit();
            }
        })));
        output.WriteLine($"{writers} writers: {commits / stopwatch.Elapsed.TotalSeconds:F0} durable commits/s");
    }
}
