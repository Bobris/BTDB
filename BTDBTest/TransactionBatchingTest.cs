using System;
using System.Linq;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Allocators;
using BTDB.BTreeLib;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class TransactionBatchingTest : IDisposable
{
    readonly LeakDetectorWrapperAllocator _allocator = new(new MallocAllocator());

    BTreeKeyValueDB Open(IFileCollection files, uint splitSize = 1024, bool durable = false) => new(new KeyValueDBOptions
    {
        FileCollection = files, Allocator = _allocator, Compression = new NoCompressionStrategy(),
        CompactorScheduler = null, FileSplitSize = splitSize
    }) { DurableTransactions = durable };

    public void Dispose() => Assert.Equal(0ul, _allocator.QueryAllocations().Count);

    static void Put(IKeyValueDBTransaction transaction, byte key, byte value, int size = 100)
    {
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], Enumerable.Repeat(value, size).ToArray());
    }

    static byte[] Read(IKeyValueDBTransaction transaction, byte key)
    {
        using var cursor = transaction.CreateCursor();
        Assert.True(cursor.FindExactKey([key]));
        Span<byte> buffer = default;
        return cursor.GetValueSpan(ref buffer).ToArray();
    }

    static async Task Commit(BTreeKeyValueDB db, byte key, byte value)
    {
        using var transaction = await db.StartWritingTransaction(inBatch: true);
        Put(transaction, key, value);
        transaction.SetCommitUlong(value);
        transaction.SetUlong(3, value);
        transaction.Commit();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RollbackAcrossLogFilesKeepsSuccessfulCommits(bool durable)
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files, durable: durable))
        {
            using var oldReader = db.StartReadOnlyTransaction();
            for (byte i = 1; i <= 40; i++) await Commit(db, i, i);
            using (var failed = await db.StartWritingTransaction(inBatch: true))
            {
                for (byte i = 1; i <= 40; i++) Put(failed, i, 255);
                failed.SetUlong(3, 999);
                failed.SetCommitUlong(999);
            }
            using (var next = await db.StartWritingTransaction(inBatch: true))
            {
                Assert.Equal(40, next.GetKeyValueCount());
                Assert.Equal(40ul, next.GetCommitUlong());
                Assert.Equal(40ul, next.GetUlong(3));
                for (byte i = 1; i <= 40; i++) Assert.All(Read(next, i), b => Assert.Equal(i, b));
                Put(next, 41, 41);
                next.Commit();
            }
            db.FinishTransactionBatchAfterCurrentTransaction();
            Assert.Equal(0, oldReader.GetKeyValueCount());
        }
        using var reopened = Open(files);
        using var reader = reopened.StartReadOnlyTransaction();
        Assert.Equal(41, reader.GetKeyValueCount());
        Assert.Equal(40ul, reader.GetCommitUlong());
        for (byte i = 1; i <= 41; i++) Assert.All(Read(reader, i), b => Assert.Equal(i, b));
    }

    [Fact]
    public async Task ReadersDuringWriterSeeLatestCommitAndKeepTheirSnapshot()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        await Commit(db, 1, 1);
        using var writer = await db.StartWritingTransaction(inBatch: true);
        Put(writer, 1, 2);
        using var reader = db.StartReadOnlyTransaction();
        Assert.All(Read(reader, 1), b => Assert.Equal(1, b));
        Put(writer, 1, 3);
        writer.Commit();
        using var laterReader = db.StartTransaction();
        Assert.All(Read(laterReader, 1), b => Assert.Equal(3, b));
        Assert.All(Read(reader, 1), b => Assert.Equal(1, b));
        db.FinishTransactionBatchAfterCurrentTransaction();
    }

    [Fact]
    public async Task RollbackAfterReaderPublicationAndEmptyTransactions()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        await Commit(db, 1, 1);
        using (var empty = await db.StartWritingTransaction(inBatch: true)) empty.Commit();
        using (await db.StartWritingTransaction(inBatch: true)) { }
        using (var failed = await db.StartWritingTransaction(inBatch: true))
        {
            Put(failed, 1, 2);
            using var reader = db.StartReadOnlyTransaction();
            Assert.All(Read(reader, 1), b => Assert.Equal(1, b));
        }
        await Commit(db, 2, 2);
        db.FinishTransactionBatchAfterCurrentTransaction();
        using var final = db.StartReadOnlyTransaction();
        Assert.Equal(2, final.GetKeyValueCount());
        Assert.All(Read(final, 1), b => Assert.Equal(1, b));
    }

    [Fact]
    public async Task RollbackFirstTransactionAndDisposeWithoutFinishing()
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files))
        {
            using (var failed = await db.StartWritingTransaction(inBatch: true)) Put(failed, 1, 1);
            await Commit(db, 2, 2);
        }
        using var reopened = Open(files);
        using var reader = reopened.StartReadOnlyTransaction();
        Assert.Equal(1, reader.GetKeyValueCount());
        Assert.All(Read(reader, 2), b => Assert.Equal(2, b));
    }

    [Fact]
    public async Task QueuedWriterReusesBatchAndSeesPreviousCommit()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        using var first = await db.StartWritingTransaction(inBatch: true);
        Put(first, 1, 1);
        var waiting = db.StartWritingTransaction(inBatch: true);
        Assert.False(waiting.IsCompleted);
        first.Commit();
        using var second = await waiting;
        Assert.All(Read(second, 1), b => Assert.Equal(1, b));
        second.Commit();
        db.FinishTransactionBatchAfterCurrentTransaction();
    }

    [Fact]
    public async Task LogBytesAreIdenticalIncludingRollbackAndDecreasingMetadata()
    {
        async Task<byte[][]> Generate(bool batching)
        {
            using var files = new InMemoryFileCollection();
            using (var db = Open(files))
            {
                for (byte i = 1; i <= 30; i++)
                {
                    using var tr = await db.StartWritingTransaction(inBatch: batching);
                    Put(tr, i, i);
                    tr.SetCommitUlong((ulong)(31 - i));
                    tr.SetUlong(0, (ulong)(31 - i));
                    tr.SetUlong(5, i);
                    if (i % 7 != 0) tr.Commit();
                }
                if (batching) db.FinishTransactionBatchAfterCurrentTransaction();
            }
            return files.Enumerate().OrderBy(f => f.Index).Select(f =>
            {
                var reader = new MemReader(f.GetExclusiveReader());
                FileTransactionLog.SkipHeader(ref reader);
                var bytes = new byte[(int)(f.GetSize() - (ulong)reader.GetCurrentPosition())];
                reader.ReadBlock(bytes);
                return bytes;
            }).ToArray();
        }
        var normal = await Generate(false);
        var batched = await Generate(true);
        Assert.Equal(normal.Length, batched.Length);
        for (var i = 0; i < normal.Length; i++) Assert.Equal(normal[i], batched[i]);
    }

    [Fact]
    public async Task CompactionFlushDoesNotAppendLogAndSurvivesDataLoss()
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files, int.MaxValue))
        {
            await Commit(db, 1, 1);
            var log = files.Enumerate().Single();
            var size = log.GetSize();
            await db.Compact(CancellationToken.None);
            Assert.Equal(size, log.GetSize());
            files.SimulateDataLossOfNotFlushedData();
            Assert.Equal(size, log.GetSize());
            await Commit(db, 2, 2);
        }
        using var reopened = Open(files);
        using var reader = reopened.StartReadOnlyTransaction();
        Assert.Equal(2, reader.GetKeyValueCount());
    }

    [Fact]
    public async Task CompactionDuringBatchPreservesValuesAndRecovery()
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files))
        {
            for (byte i = 1; i <= 50; i++) await Commit(db, i, i);
            for (byte i = 1; i <= 50; i++) await Commit(db, i, 99);
            await db.Compact(CancellationToken.None);
            using (var failed = await db.StartWritingTransaction(inBatch: true)) Put(failed, 1, 255);
            await Commit(db, 51, 51);
            db.FinishTransactionBatchAfterCurrentTransaction();
        }
        using var reopened = Open(files);
        using var reader = reopened.StartReadOnlyTransaction();
        Assert.Equal(51, reader.GetKeyValueCount());
        for (byte i = 1; i <= 50; i++) Assert.All(Read(reader, i), b => Assert.Equal(99, b));
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DiskStorageSupportsReplayAndAppendingAfterFlush(bool memoryMapped)
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-batching-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        try
        {
            IFileCollection CreateFiles() => memoryMapped
                ? new OnDiskMemoryMappedFileCollection(directory)
                : new OnDiskFileCollection(directory);
            using (var files = CreateFiles())
            using (var db = Open(files, durable: true))
            {
                for (byte i = 1; i <= 40; i++) await Commit(db, i, i);
                using (var failed = await db.StartWritingTransaction(inBatch: true))
                {
                    Put(failed, 1, 255);
                    using var reader = db.StartReadOnlyTransaction();
                    Assert.All(Read(reader, 1), b => Assert.Equal(1, b));
                }
                await db.Compact(CancellationToken.None);
                await Commit(db, 41, 41);
                db.FinishTransactionBatchAfterCurrentTransaction();
            }
            using var reopenedFiles = CreateFiles();
            using var reopened = Open(reopenedFiles);
            using var final = reopened.StartReadOnlyTransaction();
            Assert.Equal(41, final.GetKeyValueCount());
            Assert.All(Read(final, 1), b => Assert.Equal(1, b));
            Assert.All(Read(final, 41), b => Assert.Equal(41, b));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task FlushWaitsForWriterAndDoesNotCreateAnEmptyTransaction()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files, int.MaxValue);
        await ((IKeyValueDBInternal)db).FlushTransactionLog();
        Assert.Empty(files.Enumerate());
        using var writer = await db.StartWritingTransaction(inBatch: true);
        Put(writer, 1, 1);
        var flush = ((IKeyValueDBInternal)db).FlushTransactionLog();
        Assert.False(flush.IsCompleted);
        writer.Commit();
        var log = files.Enumerate().Single();
        var committedSize = log.GetSize();
        await flush;
        Assert.Equal(committedSize, log.GetSize());
        files.SimulateDataLossOfNotFlushedData();
        Assert.Equal(committedSize, log.GetSize());
        await Commit(db, 2, 2);
        db.FinishTransactionBatchAfterCurrentTransaction();
    }

    [Fact]
    public async Task ConcurrentReadersNeverObserveUncommittedValues()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        await Commit(db, 1, 1);
        var writing = Task.Run(async () =>
        {
            for (var i = 0; i < 300; i++)
            {
                using (var failed = await db.StartWritingTransaction(inBatch: true))
                {
                    Put(failed, 1, 255);
                    await Task.Yield();
                }
                await Commit(db, 1, 1);
            }
        });
        for (var i = 0; i < 300; i++)
        {
            using var reader = db.StartReadOnlyTransaction();
            Assert.All(Read(reader, 1), b => Assert.Equal(1, b));
            await Task.Yield();
        }
        await writing;
        db.FinishTransactionBatchAfterCurrentTransaction();
    }

    [Fact]
    public async Task ObjectDbTransactionsKeepTheirOwnCommitAndRollbackBoundaries()
    {
        using var files = new InMemoryFileCollection();
        using var kv = Open(files);
        using IObjectDB db = new ObjectDB();
        db.Open(kv, false);
        for (var i = 0; i < 20; i++)
        {
            using var transaction = await db.StartWritingTransaction(inBatch: true);
            transaction.Store(new ObjectDbTest.Person { Name = i.ToString(), Age = (uint)i });
            if (i % 3 != 0) transaction.Commit();
        }
        var beforeFinish = kv.ReferenceAndGetLastCommitted();
        try
        {
            db.FinishTransactionBatchAfterCurrentTransaction();
            var afterFinish = kv.ReferenceAndGetLastCommitted();
            try
            {
                Assert.NotSame(beforeFinish, afterFinish);
            }
            finally
            {
                kv.DereferenceRootNodeInternal(afterFinish);
            }
        }
        finally
        {
            kv.DereferenceRootNodeInternal(beforeFinish);
        }
        using var reader = db.StartReadOnlyTransaction();
        Assert.Equal(Enumerable.Range(0, 20).Where(i => i % 3 != 0).Select(i => i.ToString()),
            reader.Enumerate<ObjectDbTest.Person>().Select(p => p.Name));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MappedReplaySurvivesConcurrentGrowthAndHardFlush(bool durable)
    {
        var directory = Path.Combine(Path.GetTempPath(), "btdb-batch-concurrent-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        try
        {
            using var files = new OnDiskMemoryMappedFileCollection(directory);
            using var db = Open(files, int.MaxValue, durable);
            await Commit(db, 1, 1);
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var writing = Task.Run(async () =>
            {
                for (byte i = 2; i < 60; i++)
                {
                    using var writer = await db.StartWritingTransaction(inBatch: true);
                    started.TrySetResult();
                    Put(writer, 1, i, 128 * 1024);
                    writer.SetCommitUlong(i);
                    await Task.Yield();
                    writer.Commit();
                }
            });
            await started.Task;
            do
            {
                using var reader = db.StartReadOnlyTransaction();
                var value = Read(reader, 1);
                Assert.True(value.All(b => b == (byte)reader.GetCommitUlong()));
                await Task.Yield();
            } while (!writing.IsCompleted);
            await writing;
            db.FinishTransactionBatchAfterCurrentTransaction();
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    [Fact]
    public async Task ReplayRestoresCommittedErasesAndSkipsFailedErases()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        for (byte i = 1; i <= 20; i++) await Commit(db, i, i);
        using (var erase = await db.StartWritingTransaction(inBatch: true))
        {
            using var from = erase.CreateCursor();
            using var to = erase.CreateCursor();
            Assert.True(from.FindExactKey([2]));
            Assert.True(to.FindExactKey([10]));
            Assert.Equal(9, from.EraseUpTo(to));
            erase.Commit();
        }
        using (var failed = await db.StartWritingTransaction(inBatch: true))
        {
            using var cursor = failed.CreateCursor();
            Assert.True(cursor.FindExactKey([1]));
            cursor.EraseCurrent();
        }
        using var final = db.StartReadOnlyTransaction();
        Assert.Equal(11, final.GetKeyValueCount());
        Assert.All(Read(final, 1), b => Assert.Equal(1, b));
        db.FinishTransactionBatchAfterCurrentTransaction();
    }

    static RootNode12 Root(IKeyValueDBTransaction transaction) =>
        (RootNode12)((BTreeKeyValueDBTransaction)transaction).BTreeRoot!;

    [Fact]
    public async Task OrdinaryWriterImmediatelyFinishesAnIdleBatch()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        using var batch = await db.StartWritingTransaction(inBatch: true);
        var root = Root(batch);
        Put(batch, 1, 1);
        batch.Commit();
        Assert.True(root.Writable);
        using var ordinary = await db.StartWritingTransaction();
        Assert.False(root.Writable);
        Assert.NotSame(root, Root(ordinary));
        Assert.All(Read(ordinary, 1), b => Assert.Equal(1, b));
        var ordinaryRoot = Root(ordinary);
        Put(ordinary, 2, 2);
        ordinary.Commit();
        Assert.False(ordinaryRoot.Writable);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OrdinaryWriterWaitsForActiveBatchTransaction(bool commit)
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        await Commit(db, 1, 1);
        using var current = await db.StartWritingTransaction(inBatch: true);
        var batchRoot = Root(current);
        Put(current, 2, 2);
        var waiting = db.StartWritingTransaction();
        Assert.False(waiting.IsCompleted);
        if (commit) current.Commit();
        else current.Dispose();
        using var ordinary = await waiting;
        Assert.NotSame(batchRoot, Root(ordinary));
        Assert.Equal(commit ? 2 : 1, ordinary.GetKeyValueCount());
        var ordinaryRoot = Root(ordinary);
        Put(ordinary, 3, 3);
        ordinary.Commit();
        Assert.False(ordinaryRoot.Writable);
    }

    [Fact]
    public async Task QueuedWritersKeepTheirOwnBatchModeAndOrder()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        using var first = await db.StartWritingTransaction(inBatch: true);
        var batchRoot = Root(first);
        Put(first, 1, 1);
        var nextBatch = db.StartWritingTransaction(inBatch: true);
        var nextOrdinary = db.StartWritingTransaction();
        var lastBatch = db.StartWritingTransaction(inBatch: true);
        first.Commit();
        using var second = await nextBatch;
        Assert.Same(batchRoot, Root(second));
        Assert.False(nextOrdinary.IsCompleted);
        Assert.False(lastBatch.IsCompleted);
        Put(second, 2, 2);
        second.Commit();
        using var third = await nextOrdinary;
        Assert.False(batchRoot.Writable);
        Assert.NotSame(batchRoot, Root(third));
        Assert.False(lastBatch.IsCompleted);
        Put(third, 3, 3);
        third.Commit();
        using var fourth = await lastBatch;
        Assert.NotSame(batchRoot, Root(fourth));
        Assert.Equal(3, fourth.GetKeyValueCount());
        var lastRoot = Root(fourth);
        Put(fourth, 4, 4);
        fourth.Commit();
        Assert.True(lastRoot.Writable);
        db.FinishTransactionBatchAfterCurrentTransaction();
        Assert.False(lastRoot.Writable);
    }

    [Fact]
    public async Task FinishIdleBatchPublishesWithoutAppendingToLog()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        db.FinishTransactionBatchAfterCurrentTransaction();
        Assert.Empty(files.Enumerate());
        using var transaction = await db.StartWritingTransaction(inBatch: true);
        var root = Root(transaction);
        Put(transaction, 1, 1);
        transaction.Commit();
        var size = files.Enumerate().Single().GetSize();
        db.FinishTransactionBatchAfterCurrentTransaction();
        db.FinishTransactionBatchAfterCurrentTransaction();
        Assert.False(root.Writable);
        Assert.Equal(size, files.Enumerate().Single().GetSize());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FinishAfterCurrentEndsOnlyThatBatchBeforeNextQueuedWriter(bool commit, bool empty)
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        await Commit(db, 1, 1);
        using var current = await db.StartWritingTransaction(inBatch: true);
        var originalRoot = Root(current);
        if (!empty) Put(current, 2, 2);
        var queued = db.StartWritingTransaction(inBatch: true);
        db.FinishTransactionBatchAfterCurrentTransaction();
        db.FinishTransactionBatchAfterCurrentTransaction();
        Assert.False(current.IsDisposed());
        Assert.True(originalRoot.Writable);
        Assert.False(queued.IsCompleted);
        if (commit) current.Commit();
        else current.Dispose();
        using var next = await queued;
        Assert.NotSame(originalRoot, Root(next));
        Assert.Equal(commit && !empty ? 2 : 1, next.GetKeyValueCount());
        var nextRoot = Root(next);
        Put(next, 3, 3);
        next.Commit();
        Assert.True(nextRoot.Writable);
        using var continuation = await db.StartWritingTransaction(inBatch: true);
        Assert.Same(nextRoot, Root(continuation));
        continuation.Commit();
        db.FinishTransactionBatchAfterCurrentTransaction();
        Assert.False(nextRoot.Writable);
    }

    [Fact]
    public async Task FinishDuringOrdinaryWriterDoesNotAffectTheNextBatch()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        using var ordinary = await db.StartWritingTransaction();
        Put(ordinary, 1, 1);
        var waiting = db.StartWritingTransaction(inBatch: true);
        db.FinishTransactionBatchAfterCurrentTransaction();
        ordinary.Commit();
        using var batch = await waiting;
        var root = Root(batch);
        Put(batch, 2, 2);
        batch.Commit();
        Assert.True(root.Writable);
        db.FinishTransactionBatchAfterCurrentTransaction();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FinishPublishesAfterCurrentEvenWithoutWaitingWriter(bool empty)
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        await Commit(db, 1, 1);
        using var current = await db.StartWritingTransaction(inBatch: true);
        var root = Root(current);
        if (!empty) Put(current, 2, 2);
        db.FinishTransactionBatchAfterCurrentTransaction();
        Assert.True(root.Writable);
        current.Commit();
        // Check publication before opening any reader or subsequent writer that could force it.
        Assert.False(root.Writable);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReferencingPublishedRootAndStatsDoNotPublishBatch(bool activeWriter)
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        using var seed = await db.StartWritingTransaction();
        var publishedRoot = Root(seed);
        Put(seed, 1, 1);
        seed.Commit();
        using var batch = await db.StartWritingTransaction(inBatch: true);
        var batchRoot = Root(batch);
        Put(batch, 2, 2);
        batch.Commit();
        using var current = activeWriter ? await db.StartWritingTransaction(inBatch: true) : null;
        if (current != null) Put(current, 3, 3);

        var referenced = db.ReferenceAndGetLastCommitted();
        try
        {
            Assert.Same(publishedRoot, referenced);
            Assert.Equal(1, publishedRoot.GetCount());
        }
        finally
        {
            db.DereferenceRootNodeInternal(referenced);
        }
        Assert.Contains("KeyValueCount:1\n", db.CalcStats());
        Assert.True(batchRoot.Writable);
        if (current != null)
            Assert.Same(batchRoot, Root(current));
        else
        {
            using var continuation = await db.StartWritingTransaction(inBatch: true);
            Assert.Same(batchRoot, Root(continuation));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CheckpointUsesPublishedRootAndRecoversPendingCommits(bool activeWriter)
    {
        using var files = new InMemoryFileCollection();
        using (var db = Open(files))
        {
            using var seed = await db.StartWritingTransaction();
            var publishedRoot = Root(seed);
            Put(seed, 1, 1);
            seed.Commit();
            using var batch = await db.StartWritingTransaction(inBatch: true);
            var batchRoot = Root(batch);
            Put(batch, 2, 2);
            batch.Commit();
            using var current = activeWriter ? await db.StartWritingTransaction(inBatch: true) : null;
            if (current != null) Put(current, 3, 3);
            db.CreateKvi(CancellationToken.None);
            Assert.True(batchRoot.Writable);
            var referenced = db.ReferenceAndGetLastCommitted();
            try
            {
                Assert.Same(publishedRoot, referenced);
            }
            finally
            {
                db.DereferenceRootNodeInternal(referenced);
            }
        }
        using var reopened = Open(files);
        using var reader = reopened.StartReadOnlyTransaction();
        Assert.Equal(2, reader.GetKeyValueCount());
        Assert.All(Read(reader, 1), b => Assert.Equal(1, b));
        Assert.All(Read(reader, 2), b => Assert.Equal(2, b));
    }

}
