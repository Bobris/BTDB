using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

/// Writers move amounts between accounts while compaction keeps rewriting the files and readers check snapshots:
/// every snapshot, and the database reopened afterwards, holds the same total and readable values.
public class KeyValueDBConcurrentCompactionTest
{
    const int Accounts = 24;
    const long Total = Accounts * 1000L;

    static byte[] Key(int account) => [(byte)account];

    // The balance, padded to a large value now and then, so values move to value files on compaction.
    static byte[] Value(long balance, int padding)
    {
        var value = new byte[8 + padding];
        BitConverter.TryWriteBytes(value, balance);
        return value;
    }

    static long Sum(IKeyValueDBTransaction tr)
    {
        using var cursor = tr.CreateCursor();
        long sum = 0;
        var count = 0;
        if (cursor.FindFirstKey([]))
            do
            {
                var buffer = Span<byte>.Empty;
                sum += BitConverter.ToInt64(cursor.GetValueSpan(ref buffer)[..8]);
                count++;
            } while (cursor.FindNextKey([]));
        Assert.Equal(Accounts, count);
        return sum;
    }

    /// A snapshot taken before several compactions reads its own values after them, while the files they were in
    /// are rewritten, and once it ends compaction can delete those files.
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task OldSnapshotReadsItsValuesAfterSeveralCompactions(int seed)
    {
        var random = new Random(seed);
        var directory = Path.Combine(Path.GetTempPath(), "btdb-old-snapshot-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var db = new BTreeKeyValueDB(new KeyValueDBOptions
            {
                FileCollection = new OnDiskFileCollection(directory), Compression = new NoCompressionStrategy(),
                CompactorScheduler = null, FileSplitSize = 16 * 1024
            });
            byte[] ValueOf(int account, int version) => Enumerable.Repeat((byte)(account ^ version), 500 + account).ToArray();
            async Task WriteAll(int version)
            {
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                for (var a = 0; a < Accounts; a++) cursor.CreateOrUpdateKeyValue(Key(a), ValueOf(a, version));
                tr.Commit();
            }

            await WriteAll(0);
            using var old = db.StartReadOnlyTransaction();
            for (var version = 1; version <= 6; version++)
            {
                await WriteAll(version);
                if (random.Next(2) == 0) await db.Compact(CancellationToken.None);
                await db.Compact(CancellationToken.None);
            }

            using (var cursor = old.CreateCursor())
                for (var a = 0; a < Accounts; a++)
                {
                    Assert.True(cursor.FindExactKey(Key(a)));
                    var buffer = Span<byte>.Empty;
                    Assert.Equal(ValueOf(a, 0), cursor.GetValueSpan(ref buffer).ToArray());
                }

            var filesWhileOld = Directory.GetFiles(directory).Length;
            old.Dispose();
            await db.Compact(CancellationToken.None);
            await db.Compact(CancellationToken.None);
            Assert.True(Directory.GetFiles(directory).Length <= filesWhileOld);
            using var current = db.StartReadOnlyTransaction();
            using var check = current.CreateCursor();
            for (var a = 0; a < Accounts; a++)
            {
                Assert.True(check.FindExactKey(Key(a)));
                var buffer = Span<byte>.Empty;
                Assert.Equal(ValueOf(a, 6), check.GetValueSpan(ref buffer).ToArray());
            }
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task SnapshotsKeepTheTotalWhileCompactionRuns(int seed)
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-concurrent-compaction-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        KeyValueDBOptions Options() => new()
        {
            FileCollection = new OnDiskFileCollection(directory), Compression = new NoCompressionStrategy(),
            CompactorScheduler = null, FileSplitSize = 64 * 1024
        };
        try
        {
            using (var db = new BTreeKeyValueDB(Options()))
            {
                using (var tr = await db.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    for (var a = 0; a < Accounts; a++) cursor.CreateOrUpdateKeyValue(Key(a), Value(1000, 0));
                    tr.Commit();
                }

                using var stop = new CancellationTokenSource();
                var compactions = 0;
                var compactor = Task.Run(async () =>
                {
                    while (!stop.IsCancellationRequested)
                    {
                        await db.Compact(CancellationToken.None);
                        Interlocked.Increment(ref compactions);
                    }
                });
                var readers = Enumerable.Range(0, 3).Select(_ => Task.Run(() =>
                {
                    var checks = 0;
                    while (!stop.IsCancellationRequested)
                    {
                        using var tr = db.StartReadOnlyTransaction();
                        Assert.Equal(Total, Sum(tr));
                        checks++;
                    }

                    return checks;
                })).ToArray();
                var writers = Enumerable.Range(0, 2).Select(w => Task.Run(async () =>
                {
                    var random = new Random(seed * 10 + w);
                    for (var i = 0; i < 400; i++)
                    {
                        using var tr = await db.StartWritingTransaction();
                        using var cursor = tr.CreateCursor();
                        var from = random.Next(Accounts);
                        var to = (from + 1 + random.Next(Accounts - 1)) % Accounts;
                        var amount = random.Next(1, 50);
                        foreach (var (account, delta) in new[] { (from, -amount), (to, amount) })
                        {
                            Assert.True(cursor.FindExactKey(Key(account)));
                            var buffer = Span<byte>.Empty;
                            var balance = BitConverter.ToInt64(cursor.GetValueSpan(ref buffer)[..8]);
                            cursor.CreateOrUpdateKeyValue(Key(account),
                                Value(balance + delta, random.Next(4) == 0 ? random.Next(100, 3000) : 0));
                        }

                        tr.Commit();
                    }
                })).ToArray();
                await Task.WhenAll(writers).WaitAsync(TimeSpan.FromSeconds(120));
                await stop.CancelAsync();
                await compactor.WaitAsync(TimeSpan.FromSeconds(60));
                var checks = await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(60));
                Assert.True(compactions > 0 && checks.All(c => c > 0));
                await db.Compact(CancellationToken.None);
            }

            using (var reopened = new BTreeKeyValueDB(Options()))
            using (var tr = reopened.StartReadOnlyTransaction())
                Assert.Equal(Total, Sum(tr));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }
}
