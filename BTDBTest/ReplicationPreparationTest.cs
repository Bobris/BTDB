using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BTDB;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ReplicationPreparationTest
{
    static BTreeKeyValueDB Open(IFileCollection files, bool explicitTransactions = true, bool odd = true, TransactionLogCapture? capture = null) => new(new KeyValueDBOptions
    {
        FileCollection = files, CompactorScheduler = null, Compression = new NoCompressionStrategy(),
        TransactionLogCapture = capture, FileSplitSize = 1024, RequireExplicitTransactions = explicitTransactions, UseOddTransactionLogIds = odd
    });

    static void Put(IKeyValueDBTransaction tr, byte key, int length = 10)
    {
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], Enumerable.Repeat(key, length).ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EventCursorWaitsForWriterAndRollsBackWithIt(bool batch)
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        using (var first = await db.StartWritingTransaction(41ul, batch))
        {
            Assert.Equal(41ul, first.GetCommitUlong());
            var waiting = db.StartWritingTransaction(42ul, batch);
            Assert.False(waiting.IsCompleted);
            first.Commit();
            using var second = await waiting;
            Assert.Equal(42ul, second.GetCommitUlong());
            Put(second, 42);
        }
        db.FinishTransactionBatchAfterCurrentTransaction();
        using var read = db.StartReadOnlyTransaction();
        Assert.Equal(41ul, read.GetCommitUlong());
        Assert.Equal(0, read.GetKeyValueCount());
        Assert.Throws<InvalidOperationException>(() => db.StartTransaction());
    }

    [Fact]
    public async Task LegacyEvenTailRotatesAndLargeTransactionSpansOddFiles()
    {
        using var files = new InMemoryFileCollection();
        files.AddFile("reserved");
        uint legacy;
        using (var db = Open(files, false, false))
        {
            using var tr = await db.StartWritingTransaction(1ul);
            Put(tr, 1);
            tr.Commit();
            legacy = Assert.Single(db.FileCollection.FileInfos, f => f.Value is IFileTransactionLog).Key;
            Assert.Equal(0u, legacy & 1);
        }
        using (var db = Open(files))
        {
            using var tr = await db.StartWritingTransaction(2ul);
            for (byte i = 2; i < 12; i++) Put(tr, i, 700);
            tr.Commit();
            var logs = db.FileCollection.FileInfos.Where(f => f.Value is IFileTransactionLog).ToArray();
            Assert.True(logs.Length > 3);
            Assert.All(logs.Where(f => f.Key != legacy), f => Assert.Equal(1u, f.Key & 1));
            var firstNew = logs.Where(f => f.Key != legacy).MinBy(f => f.Key);
            Assert.Equal(legacy, ((IFileTransactionLog)firstNew.Value).PreviousFileId);
        }
        using (var db = Open(files))
        using (var tr = db.StartReadOnlyTransaction())
        {
            Assert.Equal(2ul, tr.GetCommitUlong());
            Assert.Equal(11, tr.GetKeyValueCount());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DiskCollectionsAllocateParityWithoutIntermediateFiles(bool mapped)
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        try
        {
            using (IFileCollection files = mapped ? new OnDiskMemoryMappedFileCollection(path) : new OnDiskFileCollection(path))
            {
                Assert.Equal(1u, files.AddFile("trl", FileIdParity.Odd).Index);
                Assert.Equal(2u, files.AddFile("pvl", FileIdParity.Even).Index);
                Assert.Equal(3u, files.AddFile("trl", FileIdParity.Odd).Index);
                Assert.Equal(5u, files.AddFile("trl", FileIdParity.Odd).Index);
                Assert.Equal(6u, files.AddFile("kvi", FileIdParity.Even).Index);
                Assert.Equal(8u, files.AddFile("pvl", FileIdParity.Even).Index);
                Assert.Equal(6u, files.GetCount());
            }
            using (IFileCollection files = mapped ? new OnDiskMemoryMappedFileCollection(path) : new OnDiskFileCollection(path))
                Assert.Equal(9u, files.AddFile("trl", FileIdParity.Odd).Index);
        }
        finally { Directory.Delete(path, true); }
    }

    [Fact]
    public async Task ConcurrentMixedAllocationDoesNotReuseSkippedIds()
    {
        using var files = new InMemoryFileCollection();
        var allocated = await Task.WhenAll(Enumerable.Range(0, 200).Select(i => Task.Run(() =>
        {
            var file = files.AddFile(i % 2 == 0 ? "trl" : "pvl", i % 2 == 0 ? FileIdParity.Odd : FileIdParity.Even);
            Assert.Equal(i % 2 == 0 ? 1u : 0u, file.Index & 1);
            return file.Index;
        })));
        Assert.Equal(200, allocated.Distinct().Count());
        Assert.Equal(200u, files.GetCount());
    }

    [Fact]
    public async Task DatabaseAllocationUsesEvenIdsForEveryNonTrlFile()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files);
        foreach (var hint in new[] { "kvi", "pvl", "hpv", "hid", "another-type" })
            Assert.Equal(0u, db.FileCollection.AddFile(hint).Index & 1);
        using (var tr = await db.StartWritingTransaction(1ul))
        {
            Put(tr, 1);
            tr.Commit();
        }
        db.CreateKvi(System.Threading.CancellationToken.None);
        Assert.All(db.FileCollection.FileInfos, entry =>
        {
            if (entry.Value is IFileTransactionLog) Assert.Equal(1u, entry.Key & 1);
            else Assert.Equal(0u, entry.Key & 1);
        });
    }

    public class Item
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public string Name { get; set; } = "";
    }

    [PersistedName("Items")]
    public interface IItems : IRelation<Item> { }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task StartupPersistsMetadataBeforeApplicationWrites(bool captureEnabled, bool inBatch)
    {
        using var files = new InMemoryFileCollection();
        var capture = new TransactionLogCapture();
        using (var kv = Open(files, capture: captureEnabled ? capture : null))
        using (var db = new ObjectDB())
        {
            db.Open(kv, false, new DBOptions());
            await db.InitializeRelations([typeof(IItems)]);
            using (var read = db.StartReadOnlyTransaction()) Assert.Empty(read.GetRelation<IItems>());
            using (var read = kv.StartReadOnlyTransaction()) Assert.Equal(2, read.GetKeyValueCount());
            using (var tr = await db.StartWritingTransaction(5ul, inBatch))
                tr.GetRelation<IItems>().Upsert(new Item { Id = 1, Name = "rollback" });
            using (var read = kv.StartReadOnlyTransaction()) Assert.Equal(2, read.GetKeyValueCount());
            using (var tr = await db.StartWritingTransaction(6ul, inBatch))
            {
                tr.GetRelation<IItems>().Upsert(new Item { Id = 2, Name = "committed" });
                tr.Commit();
            }
            if (captureEnabled)
            {
                Assert.NotEqual(default, capture.Completed);
                capture.Acknowledge(capture.Completed);
                Assert.Equal(capture.Completed, capture.Acknowledged);
            }
        }
        using (var kv = Open(files))
        using (var db = new ObjectDB())
        {
            db.Open(kv, false, new DBOptions());
            using var read = db.StartReadOnlyTransaction();
            Assert.Equal("committed", Assert.Single(read.GetRelation<IItems>()).Name);
            Assert.Equal(6ul, read.GetCommitUlong());
        }
    }
    public class IndexedItem
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; } = "";
    }

    [PersistedName("Items")]
    public interface IIndexedItems : IRelation<IndexedItem>
    {
        IndexedItem FindByName(string name);
    }

    [Fact]
    public async Task ExistingIndexUpgradeIsNotDeferredIntoApplicationUpsert()
    {
        using var files = new InMemoryFileCollection();
        using (var kv = Open(files))
        using (var db = new ObjectDB())
        {
            db.Open(kv, false, new DBOptions());
            using var tr = await db.StartWritingTransaction(9ul);
            tr.GetRelation<IItems>().Upsert(new Item { Id = 1, Name = "existing" });
            tr.Commit();
        }
        var capture = new TransactionLogCapture();
        using (var kv = Open(files, capture: capture))
        using (var db = new ObjectDB())
        {
            db.Open(kv, false, new DBOptions());
            using (var read = db.StartReadOnlyTransaction())
                Assert.Throws<BTDBTransactionRetryException>(() => read.GetRelation<IIndexedItems>());
            using (var upgrade = await db.StartWritingTransaction())
            {
                Assert.Equal(9ul, upgrade.GetCommitUlong());
                Assert.Equal(1ul, upgrade.GetRelation<IIndexedItems>().FindByName("existing").Id);
                upgrade.Commit();
            }
            var schemaPosition = capture.Completed;
            Assert.NotEqual(default, schemaPosition);
            using (var application = await db.StartWritingTransaction(10ul))
            {
                application.GetRelation<IIndexedItems>().Upsert(new IndexedItem { Id = 2, Name = "next" });
                application.Commit();
            }
            Assert.NotEqual(schemaPosition, capture.Completed);
        }
        using (var kv = Open(files))
        using (var db = new ObjectDB())
        {
            db.Open(kv, false, new DBOptions());
            using var read = db.StartReadOnlyTransaction();
            Assert.Equal(10ul, read.GetCommitUlong());
            Assert.Equal(1ul, read.GetRelation<IIndexedItems>().FindByName("existing").Id);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RollbackActionsRunBeforeNextWriterAndAreDiscardedOnCommit(bool commit)
    {
        using var files = new InMemoryFileCollection();
        using var kv = Open(files);
        using var db = new ObjectDB();
        db.Open(kv, false, new DBOptions());
        var tr = await db.StartWritingTransaction(1ul);
        var next = db.StartWritingTransaction(2ul).AsTask();
        var actions = new System.Collections.Generic.List<int>();
        ((IInternalObjectDBTransaction)tr).RegisterRollbackAction(() =>
        {
            Assert.False(next.IsCompleted);
            actions.Add(1);
        });
        ((IInternalObjectDBTransaction)tr).RegisterRollbackAction(() => actions.Add(2));
        if (commit) tr.Commit();
        tr.Dispose();
        tr.Dispose();
        if (commit) Assert.Empty(actions);
        else Assert.Equal(new[] { 1, 2 }, actions);
        using var nextTransaction = await next;
        nextTransaction.Commit();
    }

}
