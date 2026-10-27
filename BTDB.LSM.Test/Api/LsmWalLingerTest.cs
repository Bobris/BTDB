using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.LSM.Storage;
using BTDB.LSM.Test.Perf;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Api;

[CollectionDefinition(Name, DisableParallelization = true)]
public class LsmWalLingerTiming
{
    public const string Name = "LSM WAL linger timing";
}

/// The adaptive wait before WAL uploads (LsmKeyValueDBOptions.AdaptiveWalLingerMax) on simulated object storage
/// latency: closed for one writer awaiting each commit, open for several. It measures timing, so it runs alone.
[Collection(LsmWalLingerTiming.Name)]
public class LsmWalLingerTest(ITestOutputHelper output)
{
    /// Runs writers that each commit one key and wait for its durability; returns commits, WAL uploads and the largest
    /// window seen after a commit.
    static async Task<(int Commits, long Uploads, TimeSpan MaxLinger, TimeSpan Linger)> Run(int writers,
        TimeSpan adaptiveMax, TimeSpan duration)
    {
        var storage = new LatencyLsmStorage(new InMemoryLsmStorage());
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, AdaptiveWalLingerMax = adaptiveMax
        });
        var commits = 0;
        var maxLinger = TimeSpan.Zero;
        using var stop = new CancellationTokenSource(duration);
        await Task.WhenAll(Enumerable.Range(0, writers).Select(_ => Task.Run(async () =>
        {
            var key = new byte[8];
            while (!stop.IsCancellationRequested)
            {
                long transactionNumber;
                using (var tr = await db.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    BitConverter.TryWriteBytes(key, Interlocked.Increment(ref commits));
                    cursor.CreateOrUpdateKeyValue(key, key);
                    tr.Commit();
                    transactionNumber = tr.GetTransactionNumber();
                }

                await db.WaitForDurabilityAsync(transactionNumber);
                var linger = db.WalLinger;
                lock (storage)
                    if (linger > maxLinger) maxLinger = linger;
            }
        })));
        return (commits, storage.Requests("create wal"), maxLinger, db.WalLinger);
    }

    [Fact]
    public async Task OneWriterAwaitingEachCommitNeverWaits()
    {
        var (commits, uploads, maxLinger, _) =
            await Run(1, TimeSpan.FromMilliseconds(3), TimeSpan.FromSeconds(1.5));
        output.WriteLine($"{commits} commits, {uploads} uploads");
        Assert.Equal(TimeSpan.Zero, maxLinger);
        Assert.True(commits > 50, $"{commits} commits");
    }

    [Fact]
    public async Task ConcurrentWritersAwaitingDurabilityShareUploads()
    {
        var (commits, uploads, _, linger) = await Run(16, TimeSpan.FromMilliseconds(3), TimeSpan.FromSeconds(2));
        output.WriteLine($"adaptive: {commits} commits, {uploads} uploads, window {linger.TotalMilliseconds:F2} ms");
        var (fixedCommits, fixedUploads, _, _) = await Run(16, TimeSpan.Zero, TimeSpan.FromSeconds(2));
        output.WriteLine($"disabled: {fixedCommits} commits, {fixedUploads} uploads");
        Assert.True(linger > TimeSpan.Zero);
        // Without the window the commits answering an upload split into a small upload and a large one.
        Assert.True((double)commits / uploads > 1.2 * fixedCommits / fixedUploads,
            $"{commits}/{uploads} against {fixedCommits}/{fixedUploads} commits per upload");
    }

    [Fact]
    public async Task ZeroMaximumDisablesTheWindow()
    {
        var (_, _, maxLinger, _) = await Run(16, TimeSpan.Zero, TimeSpan.FromSeconds(0.5));
        Assert.Equal(TimeSpan.Zero, maxLinger);
    }
}
