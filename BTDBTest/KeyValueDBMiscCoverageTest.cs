using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.BTreeMem;
using BTDB.KVDBLayer.ReadOnly;
using BTDB.Locks;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class KeyValueDBMiscCoverageTest
{
    [Fact]
    public void InvalidSymmetricCipherRefusesEverything()
    {
        var cipher = new InvalidSymmetricCipher();
        var buf = new byte[10];
        Assert.Throws<NotSupportedException>(() => cipher.CalcEncryptedSizeFor(buf));
        Assert.Throws<NotSupportedException>(() => cipher.Encrypt(buf, buf));
        Assert.Throws<NotSupportedException>(() => cipher.CalcPlainSizeFor(buf));
        Assert.Throws<NotSupportedException>(() => cipher.Decrypt(buf, buf));
        Assert.Throws<NotSupportedException>(() => cipher.CalcOrderedEncryptedSizeFor(buf));
        Assert.Throws<NotSupportedException>(() => cipher.OrderedEncrypt(buf, buf));
        Assert.Throws<NotSupportedException>(() => cipher.CalcOrderedPlainSizeFor(buf));
        Assert.Throws<NotSupportedException>(() => cipher.OrderedDecrypt(buf, buf));
    }

    class SeqLockHolder
    {
        public SeqLock Lock;
        public long A;
        public long B;
    }

    [Fact]
    public async Task SeqLockReadersNeverSeeTornWrites()
    {
        var holder = new SeqLockHolder();
        var counter = holder.Lock.StartRead();
        Assert.False(holder.Lock.RetryRead(ref counter));
        holder.Lock.StartWrite();
        holder.A = 1;
        holder.B = 1;
        holder.Lock.EndWrite();
        Assert.True(holder.Lock.RetryRead(ref counter));
        Assert.False(holder.Lock.RetryRead(ref counter));

        var stop = 0;
        var torn = 0;
        var writers = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 20000; i++)
            {
                holder.Lock.StartWrite();
                try
                {
                    holder.A++;
                    Thread.SpinWait(5);
                    holder.B++;
                }
                finally
                {
                    holder.Lock.EndWrite();
                }
            }
        })).ToArray();
        var readers = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            while (Volatile.Read(ref stop) == 0)
            {
                var seq = holder.Lock.StartRead();
                long a, b;
                do
                {
                    a = holder.A;
                    Thread.SpinWait(2);
                    b = holder.B;
                } while (holder.Lock.RetryRead(ref seq));

                if (a != b) Interlocked.Increment(ref torn);
            }
        })).ToArray();
        await Task.WhenAll(writers);
        Volatile.Write(ref stop, 1);
        await Task.WhenAll(readers);
        Assert.Equal(0, torn);
        Assert.Equal(40001, holder.A);
        Assert.Equal(40001, holder.B);
    }

    sealed class MinimalLogger : IKeyValueDBLogger
    {
        public void ReportTransactionLeak(IKeyValueDBTransaction transaction)
        {
        }

        public void CompactionStart(ulong totalWaste)
        {
        }

        public void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory)
        {
        }

        public void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration,
            ulong beforeCompressionSize)
        {
        }

        public void TransactionLogCreated(uint fileId)
        {
        }

        public void FileMarkedForDelete(uint fileId)
        {
        }
    }

    [Fact]
    public void LoggerDefaultMethodsDoNothingAndRethrowCompactorExceptions()
    {
        IKeyValueDBLogger logger = new MinimalLogger();
        logger.LogInfo("info");
        logger.LogWarning("warning");
        Assert.True(logger.ReportCompactorException(new InvalidOperationException()));
    }

    static IKeyValueDB CreateBTree() => new BTreeKeyValueDB(new KeyValueDBOptions
    {
        FileCollection = new InMemoryFileCollection(), Compression = new NoCompressionStrategy(),
        CompactorScheduler = null
    });

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ExtensionMethodsWork(bool inMemory)
    {
        using var db = inMemory ? new InMemoryKeyValueDB() : CreateBTree();
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        Assert.True(cursor.CreateKey([1, 1]));
        Assert.False(cursor.CreateKey([1, 1]));
        cursor.CreateOrUpdateKeyValue([1, 2], [12]);
        cursor.CreateOrUpdateKeyValue([1, 4], [14]);
        cursor.CreateOrUpdateKeyValue([2, 1], [21]);
        cursor.CreateOrUpdateKeyValue([3, 1], [31]);
        Assert.Equal(3, cursor.GetKeyValueCount([1]));
        Assert.Equal(0, cursor.GetKeyValueCount([5]));
        Assert.True(cursor.FindExactKey([1, 2]));
        Assert.Equal([12], cursor.SlowGetValue());
        Assert.Equal(1, cursor.GetKeyIndex([1]));
        Assert.Equal(-1, cursor.GetKeyIndex([9]));
        Assert.False(cursor.FindExactKey([1, 3]));
        Assert.True(cursor.FindExactOrNextKey([1, 2]));
        Assert.False(cursor.FindExactOrNextKey([1, 3]));
        Assert.Equal([1, 4], cursor.SlowGetKey());
        Assert.False(cursor.FindExactOrNextKey([0]));
        Assert.Equal([1, 1], cursor.SlowGetKey());
        Assert.Equal(3, cursor.EraseAll([1]));
        Assert.Equal(0, cursor.EraseAll([1]));
        Assert.Equal(2, tr.GetKeyValueCount());

        var bigKey = new byte[70000];
        new Random(1).NextBytes(bigKey);
        cursor.CreateOrUpdateKeyValue(bigKey, [1]);
        var growing = new MemWriter();
        cursor.GetKeyIntoMemWriter(ref growing);
        Assert.Equal(bigKey, growing.GetSpan().ToArray());
    }

    [Fact]
    public void LockHelpersAndSmallExtensions()
    {
        using var rwLock = new ReaderWriterLockSlim();
        using (rwLock.ReadLock())
        {
            Assert.True(rwLock.IsReadLockHeld);
        }

        using (rwLock.UpgradableReadLock())
        {
            Assert.True(rwLock.IsUpgradeableReadLockHeld);
            using (rwLock.WriteLock())
            {
                Assert.True(rwLock.IsWriteLockHeld);
            }
        }

        Assert.False(rwLock.IsUpgradeableReadLockHeld);
        var created = 0;
        var lazy = new Lazy<int>(() => ++created).Force();
        Assert.True(lazy.IsValueCreated);
        Assert.Equal(1, created);
        var dict = new ConcurrentDictionary<int, int>();
        dict[1] = 1;
        Assert.True(dict.TryRemove(1));
        Assert.False(dict.TryRemove(1));
    }

    [Fact]
    public async Task InMemoryKeyValueDBMiscApi()
    {
        var db = new InMemoryKeyValueDB();
        Assert.Equal("KeyValueCount:0" + Environment.NewLine, db.CalcStats());
        Assert.Equal((0ul, 0ul, 0ul, 0ul), db.GetNativeMemoryStats());
        Assert.False(await db.Compact(CancellationToken.None));
        db.CreateKvi(CancellationToken.None);
        db.Logger = new MinimalLogger();
        Assert.NotNull(db.Logger);
        db.CompactorRamLimitInMb = 10;
        Assert.Equal(10u, db.CompactorRamLimitInMb);
        db.MaxTrLogFileSize = 1000;
        Assert.Equal(1000, db.MaxTrLogFileSize);
        db.CompactorReadBytesPerSecondLimit = 1;
        db.CompactorWriteBytesPerSecondLimit = 2;
        Assert.Equal(1ul, db.CompactorReadBytesPerSecondLimit);
        Assert.Equal(2ul, db.CompactorWriteBytesPerSecondLimit);
        db.PreserveHistoryUpToCommitUlong = 5;
        Assert.Null(db.PreserveHistoryUpToCommitUlong);
        db.DurableTransactions = true;
        Assert.True(db.DurableTransactions);

        var writing = await db.StartWritingTransaction();
        var waiting = db.StartWritingTransaction();
        Assert.False(waiting.IsCompleted);
        Assert.Single(db.Transactions());
        Assert.Throws<BTDBException>(() => db.Dispose());
        writing.Dispose();
        var second = await waiting;
        Assert.True(second.IsWriting());
        second.Dispose();
        var reader = db.StartReadOnlyTransaction();
        Assert.True(reader.IsReadOnly());
        Assert.Throws<BTDBException>(() => db.Dispose());
        reader.Dispose();
        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.StartTransaction());
        Assert.Throws<ObjectDisposedException>(() => db.StartReadOnlyTransaction());
        Assert.Throws<ObjectDisposedException>(() => db.StartWritingTransaction());
    }

    [Fact]
    public async Task InMemoryKeyValueDBDisposeCancelsWaitingWriters()
    {
        var db = new InMemoryKeyValueDB();
        var writing = await db.StartWritingTransaction();
        var waiting = db.StartWritingTransaction().AsTask();
        // Commit makes it possible to dispose DB, but waiting writer gets transaction
        writing.Commit();
        var next = await waiting;
        var waiting2 = db.StartWritingTransaction().AsTask();
        next.Commit();
        var third = await waiting2;
        third.Commit();
        writing.Dispose();
        next.Dispose();
        third.Dispose();
        db.Dispose();
    }

    [Fact]
    public async Task InMemoryTransactionMiscApi()
    {
        using var db = new InMemoryKeyValueDB();
        var tr = db.StartTransaction();
        Assert.False(tr.IsWriting());
        Assert.False(tr.IsReadOnly());
        Assert.Same(db, tr.Owner);
        tr.DescriptionForLeaks = "desc";
        Assert.Equal("desc", tr.DescriptionForLeaks);
        Assert.True(tr.CreatedTime <= DateTime.UtcNow);
        tr.RollbackAdvised = true;
        Assert.True(tr.RollbackAdvised);
        tr.NextCommitTemporaryCloseTransactionLog();
        Assert.Equal(0u, tr.GetUlongCount());
        tr.SetUlong(0, 0);
        Assert.False(tr.IsWriting());
        tr.SetUlong(1, 5);
        Assert.True(tr.IsWriting());
        Assert.Equal(2u, tr.GetUlongCount());
        Assert.Equal(1, tr.GetTransactionNumber());
        tr.DescriptionForLeaks = "writing";
        Assert.Empty(tr.CalcBTreeStats());
        tr.Commit();
        Assert.True(tr.IsDisposed());
        Assert.Throws<BTDBException>(() => tr.Commit());
        Assert.Throws<ObjectDisposedException>(() => tr.CreateCursor());
        Assert.Throws<ObjectDisposedException>(() => tr.GetKeyValueCount());
        Assert.Throws<ObjectDisposedException>(() => tr.SetCommitUlong(1));
        Assert.Throws<ObjectDisposedException>(() => tr.SetUlong(1, 1));
        tr.Dispose();
        tr.Dispose();

        using var ro = db.StartReadOnlyTransaction();
        Assert.Equal(5ul, ro.GetUlong(1));
        Assert.Throws<BTDBTransactionRetryException>(() => ro.SetCommitUlong(1));

        // Preapproved writing transaction with no change just releases writing lock on commit
        using (var w = await db.StartWritingTransaction())
        {
            w.Commit();
        }

        using var tr1 = db.StartTransaction();
        using var tr2 = db.StartTransaction();
        tr1.SetCommitUlong(10);
        Assert.Throws<BTDBTransactionRetryException>(() => tr2.SetCommitUlong(11));
        tr1.Commit();
        Assert.Throws<BTDBTransactionRetryException>(() => tr2.SetCommitUlong(12));
    }

    [Fact]
    public void InMemoryCursorsAreReusedAndDisposedSafely()
    {
        using var db = new InMemoryKeyValueDB();
        using var tr = db.StartTransaction();
        var cursors = Enumerable.Range(0, 5).Select(_ => tr.CreateCursor()).ToArray();
        cursors[0].CreateOrUpdateKeyValue([1], [1]);
        foreach (var c in cursors) c.Dispose();
        foreach (var c in cursors) c.Dispose();
        Assert.Throws<ObjectDisposedException>(() => cursors[4].FindFirstKey([]));
        using var reused = tr.CreateCursor();
        using var reused2 = tr.CreateCursor();
        Assert.True(reused.FindFirstKey([]));
        Assert.False(reused2.IsValid());
        Assert.Throws<InvalidOperationException>(() => reused2.GetKeySpan(new Span<byte>()));
        Assert.Equal(new KeyValuePair<uint, uint>(1, 1), reused.GetStorageSizeOfCurrentKey());
        Assert.False(reused.IsValueCorrupted());
        Assert.Same(tr, reused.Transaction);
    }

    [Fact]
    public async Task ReadOnlyKeyValueDBMiscApi()
    {
        using var source = new InMemoryKeyValueDB();
        using (var tr = source.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 100; i++) cursor.CreateOrUpdateKeyValue([(byte)i, 1], [(byte)i]);
            tr.SetCommitUlong(9);
            tr.SetUlong(1, 3);
            tr.Commit();
        }

        var fileName = Path.Combine(Path.GetTempPath(), "BTDBRO" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var srcTr = source.StartReadOnlyTransaction())
            {
                var creator = new ReadOnlyCreator(srcTr);
                creator.SetMaxChildren(4);
                creator.WriteToFile(fileName);
            }

            var db = ReadOnlyKeyValueDB.OpenFile(fileName);
            Assert.Contains("100 keys", db.CalcStats());
            Assert.Equal((0ul, 0ul, 0ul, 0ul), db.GetNativeMemoryStats());
            Assert.False(await db.Compact(CancellationToken.None));
            Assert.Throws<BTDBException>(() => db.CreateKvi(CancellationToken.None));
            await Assert.ThrowsAsync<BTDBException>(() => db.StartWritingTransaction().AsTask());
            Assert.Empty(db.Transactions());
            db.DurableTransactions = true;
            db.PreserveHistoryUpToCommitUlong = 1;
            db.Logger = null;
            db.CompactorRamLimitInMb = 1;
            db.MaxTrLogFileSize = 1;
            db.CompactorReadBytesPerSecondLimit = 1;
            db.CompactorWriteBytesPerSecondLimit = 1;
            Assert.True(db.DurableTransactions);
            var tr = db.StartTransaction();
            Assert.Same(db, tr.Owner);
            Assert.False(tr.IsWriting());
            Assert.True(tr.IsReadOnly());
            Assert.False(tr.IsDisposed());
            Assert.Equal(9ul, tr.GetCommitUlong());
            tr.SetCommitUlong(9);
            Assert.Throws<BTDBException>(() => tr.SetCommitUlong(10));
            Assert.Equal(2u, tr.GetUlongCount());
            Assert.Equal(3ul, tr.GetUlong(1));
            Assert.Equal(0ul, tr.GetUlong(100));
            tr.SetUlong(1, 3);
            Assert.Throws<BTDBException>(() => tr.SetUlong(1, 4));
            tr.NextCommitTemporaryCloseTransactionLog();
            tr.Commit();
            Assert.Equal(0, tr.GetTransactionNumber());
            tr.RollbackAdvised = true;
            Assert.True(tr.RollbackAdvised);
            tr.DescriptionForLeaks = "x";
            Assert.Equal("x", tr.DescriptionForLeaks);
            Assert.True(tr.CreatedTime <= DateTime.UtcNow);
            var stats = tr.CalcBTreeStats();
            Assert.Equal(100u, stats.Where(s => stats.Keys.Max(k => k.Depth) == s.Key.Depth)
                .Sum(s => s.Key.Children * s.Value));
            using (var cursor = tr.CreateCursor())
            {
                Assert.True(cursor.FindKeyIndex(50));
                Assert.Equal([50, 1], cursor.SlowGetKey());
                Assert.Equal([50], cursor.SlowGetValue());
                Assert.Same(tr, cursor.Transaction);
            }

            tr.Dispose();
            Assert.True(tr.IsDisposed());
            Assert.Throws<ObjectDisposedException>(() => tr.CreateCursor());
            db.Dispose();
            db.Dispose();
            Assert.Throws<ObjectDisposedException>(() => db.StartReadOnlyTransaction());
        }
        finally
        {
            File.Delete(fileName);
        }
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(1, 10)]
    [InlineData(29, 10)]
    [InlineData(31, 10)]
    [InlineData(1000, 10)]
    [InlineData(40000, 10)]
    [InlineData(200, 5000)]
    public async Task InMemoryBTreeBuildTreeCreatesValidTree(int count, int keyLength)
    {
        using var db = new InMemoryKeyValueDB();
        var keys = new List<byte[]>();
        using (var tr = await db.StartWritingTransaction())
        {
            // Make transaction really writing, otherwise commit of preapproved transaction is no-op
            tr.SetCommitUlong(1);
            var root = ((InMemoryKeyValueDBTransaction)tr)._btreeRoot!;
            var i = 0;
            root.BuildTree(count, () =>
            {
                var key = new byte[keyLength];
                PackUnpack.PackInt32BE(key, 0, i);
                keys.Add(key);
                return new() { Key = key, Value = BitConverter.GetBytes(i++) };
            });
            tr.Commit();
        }

        Assert.Equal(count, keys.Count);
        using (var tr = db.StartTransaction())
        {
            Assert.Equal(count, tr.GetKeyValueCount());
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < count; i++)
            {
                Assert.True(cursor.FindNextKey([]));
                Assert.Equal(i, cursor.GetKeyIndex());
                Assert.Equal(keys[i], cursor.SlowGetKey());
                Assert.Equal(BitConverter.GetBytes(i), cursor.SlowGetValue());
            }

            Assert.False(cursor.FindNextKey([]));
            if (count > 0)
            {
                Assert.True(cursor.FindLastKey([]));
                Assert.Equal(keys[^1], cursor.SlowGetKey());
                Assert.True(cursor.FindKeyIndex(count / 2));
                Assert.Equal(keys[count / 2], cursor.SlowGetKey());
                Assert.Equal(FindResult.Exact, cursor.Find(keys[count / 3], 0));
                Assert.NotEmpty(tr.CalcBTreeStats());
                // Built tree can be modified
                cursor.EraseCurrent();
                cursor.CreateOrUpdateKeyValue(new byte[keyLength + 1], [1]);
                Assert.Equal(count, tr.GetKeyValueCount());
                if (count > 2)
                {
                    using var from = tr.CreateCursor();
                    using var to = tr.CreateCursor();
                    Assert.True(from.FindKeyIndex(1));
                    Assert.True(to.FindKeyIndex(count - 2));
                    Assert.Equal(count - 2, from.EraseUpTo(to));
                    Assert.Equal(2, tr.GetKeyValueCount());
                }
            }
            else
            {
                Assert.Empty(tr.CalcBTreeStats());
            }

            tr.Commit();
        }
    }

    [Fact]
    public async Task KeyIndexSnapshotWriteToValidatesArguments()
    {
        using var files = new InMemoryFileCollection();
        using var output = new InMemoryFileCollection();
        using var db = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = files, CompactorScheduler = null, FileSplitSize = 4096,
            Compression = new NoCompressionStrategy()
        });
        for (var t = 0; t < 4; t++)
        {
            using var tr = await db.StartWritingTransaction((ulong)t + 1);
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 100; i++)
                cursor.CreateOrUpdateKeyValue(BitConverter.GetBytes(t == 0 ? i : i % 50), new byte[100 + t]);
            tr.Commit();
        }

        await db.Compact(CancellationToken.None);
        using (var tr = await db.StartWritingTransaction(100ul))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([200], new byte[600]);
            tr.Commit();
        }

        using var snapshot = db.CaptureKeyIndexSnapshot();
        Assert.Equal(100ul, snapshot.CommitUlong);
        Assert.NotEqual(0u, snapshot.TransactionLogFileId);
        Assert.NotEqual(0u, snapshot.TransactionLogOffset);
        var pvls = snapshot.Sources.Where(s => s.FileType == KVFileType.PureValues).ToArray();
        Assert.NotEmpty(pvls);
        Assert.Contains(snapshot.Sources, s => s.FileType == KVFileType.TransactionLog);
        var maxGeneration = snapshot.Sources.Max(s => s.Generation);
        var mapping = new Dictionary<uint, uint>();
        var nextId = 1000u;
        foreach (var pvl in pvls) mapping[pvl.FileId] = nextId++;

        IMemWriter Dest() => output.AddFile("kvi").GetAppenderWriter();
        Assert.Throws<ArgumentOutOfRangeException>(() => snapshot.WriteTo(Dest(), maxGeneration, mapping));
        Assert.Throws<ArgumentException>(() =>
            snapshot.WriteTo(Dest(), maxGeneration + 1, new Dictionary<uint, uint>()));
        var zero = mapping.ToDictionary(p => p.Key, _ => 0u);
        Assert.Throws<ArgumentException>(() => snapshot.WriteTo(Dest(), maxGeneration + 1, zero));
        var collide = mapping.ToDictionary(p => p.Key, _ => snapshot.TransactionLogFileId);
        Assert.Throws<ArgumentException>(() => snapshot.WriteTo(Dest(), maxGeneration + 1, collide));
        var extra = new Dictionary<uint, uint>(mapping) { [99999] = 5000 };
        Assert.Throws<ArgumentException>(() => snapshot.WriteTo(Dest(), maxGeneration + 1, extra));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Assert.Throws<OperationCanceledException>(() =>
            snapshot.WriteTo(Dest(), maxGeneration + 1, mapping, cancelled.Token));
        var file = output.AddFile("kvi");
        var writer = file.GetAppenderWriter();
        snapshot.WriteTo(writer, maxGeneration + 1, mapping);
        Assert.True(file.GetSize() > 0);
        snapshot.Dispose();
        snapshot.Dispose();
        Assert.Throws<ObjectDisposedException>(() => snapshot.WriteTo(Dest(), maxGeneration + 1, mapping));
    }

    // A value written compressed by older versions reports its compressed size as its stored size.
    public class CompressedSizeCursor : System.Reflection.DispatchProxy
    {
        public IKeyValueDBCursor Inner = null!;

        protected override object? Invoke(System.Reflection.MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(IKeyValueDBCursor.GetStorageSizeOfCurrentKey))
            {
                var size = Inner.GetStorageSizeOfCurrentKey();
                return new KeyValuePair<uint, uint>(size.Key, size.Value / 4);
            }

            try
            {
                return targetMethod.Invoke(Inner, args);
            }
            catch (System.Reflection.TargetInvocationException e)
            {
                throw e.InnerException!;
            }
        }
    }

    // Before the fix the buffer was sized by the stored size, and returned also when the value did not fit and was read
    // into a larger one, as for a value written compressed by older versions.
    [Fact]
    public void SlowGetValueReturnsWholeValueLargerThanItsStoredSize()
    {
        using var db = new InMemoryKeyValueDB();
        var value = new byte[1000];
        value[^1] = 1;
        using var tr = db.StartTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([1], value);
        Assert.True(cursor.FindExactKey([1]));
        var proxy = System.Reflection.DispatchProxy.Create<IKeyValueDBCursor, CompressedSizeCursor>();
        ((CompressedSizeCursor)(object)proxy).Inner = cursor;
        Assert.Equal(value, proxy.SlowGetValue());
        Assert.Equal([1], proxy.SlowGetKey());
    }
}
