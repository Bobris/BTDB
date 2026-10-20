using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// WAL durability, recovery after crashes, writer takeover and fencing (ImplementationPlan.md, sections 8 and 14).
public class LsmDurabilityTest
{
    static LsmKeyValueDBOptions Options(InMemoryLsmStorage storage, bool background = true, bool acquire = true,
        int chunkSize = 256 * 1024) => new()
    {
        Storage = storage, AcquireWriterLeaseOnOpen = acquire, BackgroundCompaction = background,
        MemtableSize = 16 * 1024, BlockSize = 512, Level0CompactionTrigger = 2, LevelBaseBytes = 32 * 1024,
        LevelMultiplier = 4, LevelCount = 4, TargetFileSize = 16 * 1024, WalChunkTargetSize = chunkSize,
        WalFlushInterval = TimeSpan.FromMilliseconds(2), LeaseDuration = TimeSpan.FromSeconds(3),
        LeaseSafetyMargin = TimeSpan.FromMilliseconds(500), LeaseRetryInterval = TimeSpan.FromMilliseconds(20)
    };

    static byte[] Key(int i) => [(byte)(i >> 8), (byte)i];

    static SortedDictionary<string, string> Content(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        using var cursor = tr.CreateCursor();
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (cursor.FindFirstKey([]))
            do result.Add(Convert.ToHexString(cursor.SlowGetKey()), Convert.ToHexString(cursor.SlowGetValue()));
            while (cursor.FindNextKey([]));
        return result;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WaitForDurabilityWithoutTransactionNumberCompletesWithoutCommits(bool acquire)
    {
        using var db = await LsmKeyValueDB.OpenAsync(Options(new InMemoryLsmStorage(), acquire: acquire));
        ILsmKeyValueDB api = db;
        Assert.True(api.WaitForDurabilityAsync().IsCompletedSuccessfully);
        Assert.True(db.WaitForDurabilityAsync(null).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitForDurabilityWithoutTransactionNumberCapturesLatestCommit()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new GatedWalStorage(inner);
        using var db = await LsmKeyValueDB.OpenAsync(Options(inner) with
        {
            Storage = storage, WalUploadParallelism = 1
        });
        storage.BlockCreates = true;
        try
        {
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key(1), [1]);
                tr.Commit();
            }

            Assert.True(await storage.Started.WaitAsync(TimeSpan.FromSeconds(5)));
            ILsmKeyValueDB api = db;
            var first = api.WaitForDurabilityAsync().AsTask();
            Assert.False(first.IsCompleted);

            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key(2), [2]);
                tr.Commit();
            }

            var second = db.WaitForDurabilityAsync(null).AsTask();
            Assert.False(second.IsCompleted);
            storage.Gate.Release();
            await first.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(await storage.Started.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.False(second.IsCompleted);
            using (var reader = await LsmKeyValueDB.OpenAsync(Options(inner, acquire: false)))
                Assert.Equal(new[] { "0001" }, Content(reader).Keys.ToArray());

            storage.Gate.Release();
            await second.WaitAsync(TimeSpan.FromSeconds(5));
            using var caughtUp = await LsmKeyValueDB.OpenAsync(Options(inner, acquire: false));
            Assert.Equal(Content(db), Content(caughtUp));
        }
        finally
        {
            storage.BlockCreates = false;
            storage.Gate.Release();
        }
    }

    [Fact]
    public async Task WaitForDurabilityWithoutTransactionNumberCanBeCancelled()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new GatedWalStorage(inner);
        using var db = await LsmKeyValueDB.OpenAsync(Options(inner) with
        {
            Storage = storage, WalUploadParallelism = 1
        });
        storage.BlockCreates = true;
        try
        {
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key(1), [1]);
                tr.Commit();
            }

            using var cancellation = new CancellationTokenSource();
            var waiting = db.WaitForDurabilityAsync(cancellation: cancellation.Token).AsTask();
            Assert.False(waiting.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        }
        finally
        {
            storage.BlockCreates = false;
            storage.Gate.Release();
        }

        await db.WaitForDurabilityAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        using var reader = await LsmKeyValueDB.OpenAsync(Options(inner, acquire: false));
        Assert.Equal(Content(db), Content(reader));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RollbackNeedsNoDurabilityWaitAndWritesNoWal(bool nativeMemtable)
    {
        var storage = new InMemoryLsmStorage();
        using var db = await LsmKeyValueDB.OpenAsync(Options(storage, background: false) with
        {
            NativeMemtable = nativeMemtable
        });
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key(1), [1]);
            tr.Commit();
        }

        var walBefore = storage.Keys("wal/");
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key(1), [2]);
            cursor.CreateOrUpdateKeyValue(Key(2), [2]);
            // The active transaction is not part of the last committed state.
            Assert.True(db.WaitForDurabilityAsync().IsCompletedSuccessfully);
        }

        Assert.True(db.WaitForDurabilityAsync().IsCompletedSuccessfully);
        Assert.Equal(walBefore, storage.Keys("wal/"));
        Assert.Equal(new[] { "0001" }, Content(db).Keys.ToArray());
        Assert.Equal("01", Content(db)["0001"]);
        db.SimulateCrash();
        using var recovered = await LsmKeyValueDB.OpenAsync(Options(storage, acquire: false));
        Assert.Equal(new[] { "0001" }, Content(recovered).Keys.ToArray());
        Assert.Equal("01", Content(recovered)["0001"]);
    }

    [Theory]
    [InlineData(1, true, 256 * 1024)]
    [InlineData(2, false, 256 * 1024)]
    [InlineData(3, true, 700)]
    [InlineData(4, false, 700)]
    public async Task RecoveredStateIsADurablePrefixOfHistory(int seed, bool background, int chunkSize)
    {
        var random = new Random(seed);
        var storage = new InMemoryLsmStorage();
        var history = new List<SortedDictionary<string, string>> { new() };
        var model = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var durable = 0;
        for (var round = 0; round < 4; round++)
        {
            var db = await LsmKeyValueDB.OpenAsync(Options(storage, background, chunkSize: chunkSize));
            Assert.Equal(model, Content(db));
            using (var tr = db.StartReadOnlyTransaction()) Assert.Equal((ulong)(history.Count - 1), tr.GetCommitUlong());
            var commits = random.Next(20, 120);
            for (var c = 0; c < commits; c++)
            {
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                for (var op = random.Next(1, 30); op > 0; op--)
                {
                    var key = Key(random.Next(300));
                    var hex = Convert.ToHexString(key);
                    if (random.Next(4) == 0)
                    {
                        if (cursor.FindExactKey(key))
                        {
                            cursor.EraseCurrent();
                            model.Remove(hex);
                        }
                    }
                    else
                    {
                        var value = new byte[random.Next(3) == 0 ? random.Next(2000) : 4];
                        random.NextBytes(value);
                        cursor.CreateOrUpdateKeyValue(key, value);
                        model[hex] = Convert.ToHexString(value);
                    }
                }

                tr.SetCommitUlong((ulong)history.Count);
                var number = tr.GetTransactionNumber();
                tr.Commit();
                history.Add(new(model, StringComparer.Ordinal));
                if (random.Next(10) == 0)
                {
                    await db.WaitForDurabilityAsync(number);
                    durable = history.Count - 1;
                }
            }

            db.SimulateCrash();
            storage.ExpireAllLeases();
            using var recovered = await LsmKeyValueDB.OpenAsync(Options(storage, background, acquire: false));
            ulong recoveredCommit;
            using (var tr = recovered.StartReadOnlyTransaction()) recoveredCommit = tr.GetCommitUlong();
            Assert.True((int)recoveredCommit >= durable, $"recovered {recoveredCommit} < durable {durable}");
            Assert.Equal(history[(int)recoveredCommit], Content(recovered));
            history.RemoveRange((int)recoveredCommit + 1, history.Count - (int)recoveredCommit - 1);
            model = new(history[^1], StringComparer.Ordinal);
        }
    }

    [Fact]
    public async Task LargeCommitSpanningManyChunksSurvivesCrash()
    {
        var storage = new InMemoryLsmStorage();
        var db = await LsmKeyValueDB.OpenAsync(Options(storage, chunkSize: 4096) with { MemtableSize = 64 << 20 });
        long number;
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 2000; i++) cursor.CreateOrUpdateKeyValue(Key(i), new byte[1000]);
            tr.SetCommitUlong(7);
            number = tr.GetTransactionNumber();
            tr.Commit();
        }

        await db.WaitForDurabilityAsync(number);
        Assert.True(storage.Keys("wal/").Length > 400);
        db.SimulateCrash();
        storage.ExpireAllLeases();
        using var recovered = await LsmKeyValueDB.OpenAsync(Options(storage));
        using var read = recovered.StartReadOnlyTransaction();
        Assert.Equal(2000, read.GetKeyValueCount());
        Assert.Equal(7ul, read.GetCommitUlong());
    }

    [Fact]
    public async Task NewWriterFencesZombieWriter()
    {
        var storage = new InMemoryLsmStorage();
        using var zombie = await LsmKeyValueDB.OpenAsync(Options(storage));
        long durableNumber;
        using (var tr = await zombie.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([1], [1]);
            durableNumber = tr.GetTransactionNumber();
            tr.Commit();
        }

        await zombie.WaitForDurabilityAsync(durableNumber);
        // The zombie loses its lease without noticing; a new writer takes over.
        storage.ExpireAllLeases();
        using var writer = await LsmKeyValueDB.OpenAsync(Options(storage));
        Assert.True(writer.HoldsWriterLease);
        // The zombie still believes it is the writer; its next upload hits the new writer's fence.
        var zombieFailed = false;
        try
        {
            using var tr = zombie.StartTransaction();
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([2], [2]);
            var number = tr.GetTransactionNumber();
            tr.Commit();
            await zombie.WaitForDurabilityAsync(number);
        }
        catch (Exception ex) when (ex is BTDBException)
        {
            zombieFailed = true;
        }

        Assert.True(zombieFailed);
        await WaitUntil(() => !zombie.HoldsWriterLease);
        using (var tr = await writer.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([3], [3]);
            tr.Commit();
        }

        await writer.FlushAsync();
        writer.SimulateCrash();
        storage.ExpireAllLeases();
        using var recovered = await LsmKeyValueDB.OpenAsync(Options(storage, acquire: false));
        Assert.Equal(new[] { "01", "03" }, Content(recovered).Keys.ToArray());
    }

    [Fact]
    public async Task ReaderBecomesWriterWhenTheWriterReleasesTheLease()
    {
        var storage = new InMemoryLsmStorage();
        var writer = await LsmKeyValueDB.OpenAsync(Options(storage));
        using (var tr = await writer.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([1], [1]);
            tr.Commit();
        }

        using var reader = await LsmKeyValueDB.OpenAsync(Options(storage, acquire: false));
        Assert.False(reader.HoldsWriterLease);
        using (var tr = reader.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.Throws<BTDBTransactionRetryException>(() => cursor.CreateOrUpdateKeyValue([9], [9]));
        }

        var writing = reader.StartWritingTransaction().AsTask();
        await Task.Delay(100);
        Assert.False(writing.IsCompleted);
        using (var tr = await writer.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([2], [2]);
            tr.Commit();
        }

        writer.Dispose();
        using (var tr = await writing)
        {
            Assert.True(reader.HoldsWriterLease);
            using var cursor = tr.CreateCursor();
            Assert.True(cursor.FindExactKey([2]));
            cursor.CreateOrUpdateKeyValue([3], [3]);
            tr.Commit();
        }

        Assert.Equal(new[] { "01", "02", "03" }, Content(reader).Keys.ToArray());
    }

    [Fact]
    public async Task CheckpointDeletesCoveredWalChunks()
    {
        var storage = new InMemoryLsmStorage();
        using var db = await LsmKeyValueDB.OpenAsync(Options(storage, background: false) with { MemtableSize = 64 << 20 });
        for (var i = 0; i < 50; i++)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key(i), [1]);
            tr.Commit();
        }

        Assert.True(storage.Keys("wal/").Length > 10);
        db.CreateKvi(default);
        Assert.True(storage.Keys("wal/").Length <= 2, string.Join(",", storage.Keys("wal/")));
    }

    static async Task WaitUntil(Func<bool> condition)
    {
        for (var i = 0; i < 200 && !condition(); i++) await Task.Delay(20);
        Assert.True(condition());
    }

    sealed class GatedWalStorage(InMemoryLsmStorage inner) : ILsmStorage
    {
        public volatile bool BlockCreates;
        public readonly SemaphoreSlim Gate = new(0);
        public readonly SemaphoreSlim Started = new(0);

        public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
            CancellationToken cancellation)
        {
            if (BlockCreates && key.StartsWith("wal/", StringComparison.Ordinal))
            {
                Started.Release();
                await Gate.WaitAsync(cancellation);
            }

            return await inner.CreateAsync(key, content, cancellation);
        }

        public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) => inner.TryReadAsync(key, cancellation);
        public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);
        public void ReadRange(string key, long offset, Span<byte> destination) => inner.ReadRange(key, offset, destination);
        public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
        public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
        public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
        public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
        public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
    }
}
