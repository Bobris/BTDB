using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbInitializeRelationsTest : ObjectDbTestBase
{
    readonly CountingDb _countingDb;

    public ObjectDbInitializeRelationsTest(ITestOutputHelper output) : base(output)
    {
        _countingDb = new CountingDb(_lowDb);
        _lowDb = _countingDb;
        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations());
    }

    public class Item
    {
        [PrimaryKey] public ulong Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class IndexedItem
    {
        [PrimaryKey] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; } = "";
    }

    [PersistedName("Items")]
    public interface IItems : IRelation<Item>;

    [PersistedName("Items")]
    public interface IIndexedItems : IRelation<IndexedItem>
    {
        IndexedItem FindByName(string name);
    }

    public interface IOtherItems : IRelation<Item>;

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void SkymambaStartupInitializesRelationsInOrdinaryTransaction(bool upgrade, bool readBeforeStartup)
    {
        if (upgrade)
        {
            _db.AllowAutoRegistrationOfRelations = true;
            using (var seed = _db.StartTransaction())
            {
                seed.GetRelation<IItems>().Upsert(new Item { Id = 1, Name = "existing" });
                seed.Commit();
            }
            ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations());
        }
        var relationType = upgrade ? typeof(IIndexedItems) : typeof(IItems);
        if (readBeforeStartup)
        {
            _db.AllowAutoRegistrationOfRelations = true;
            using var read = _db.StartReadOnlyTransaction();
            read.GetRelation(relationType);
        }

        // ContinentInBTDB.InitializeData/InitializeRelations uses this sequence.
        using (var tr = _db.StartTransaction())
        {
            Assert.False(tr.KeyValueDBTransaction.IsReadOnly());
            _db.AllowAutoRegistrationOfRelations = true;
            _db.RegisterCustomRelation(typeof(IOtherItems), _ => throw new InvalidOperationException("Custom factory"));
            tr.GetRelation(relationType);
            _db.AllowAutoRegistrationOfRelations = false;
            tr.Commit();
        }
        using (var read = _db.StartReadOnlyTransaction())
        {
            if (upgrade) Assert.Equal(1ul, read.GetRelation<IIndexedItems>().FindByName("existing").Id);
            else Assert.Empty(read.GetRelation<IItems>());
            Assert.Equal("Custom factory", Assert.Throws<InvalidOperationException>(() => read.GetRelation<IOtherItems>()).Message);
        }
        ReopenDb(new DBOptions());
        using var reopened = _db.StartReadOnlyTransaction();
        if (upgrade) Assert.Equal(1ul, reopened.GetRelation<IIndexedItems>().FindByName("existing").Id);
        else
        {
            Assert.Empty(reopened.GetRelation<IItems>());
            Assert.Equal(2, reopened.KeyValueDBTransaction.GetKeyValueCount());
        }
    }

    [Fact]
    public async Task PersistsEmptyRelationsInOneWriterAndReopensReadOnly()
    {
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IItems), typeof(IOtherItems), typeof(IItems)]);
        Assert.Equal(["read", "write"], _countingDb.Starts);
        using (var tr = _lowDb.StartReadOnlyTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.Equal(2, cursor.GetKeyValueCount([0, 4]));
            Assert.Equal(2, cursor.GetKeyValueCount([0, 5]));
            Assert.Equal(0, cursor.GetKeyValueCount([3]));
        }

        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations());
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IOtherItems), typeof(IItems)]);
        Assert.Equal(["read"], _countingDb.Starts);
        Assert.False(_db.AllowAutoRegistrationOfRelations);
        using (var tr = _db.StartReadOnlyTransaction())
        {
            Assert.Empty(tr.GetRelation<IItems>());
            Assert.Empty(tr.GetRelation<IOtherItems>());
        }
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IItems), typeof(IOtherItems)]);
        Assert.Equal(["read"], _countingDb.Starts);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UpgradesEmptyOrPopulatedSchemaAndRegistersNewRelationTogether(bool populated)
    {
        await _db.InitializeRelations([typeof(IItems)]);
        using (var tr = await _db.StartWritingTransaction(42ul))
        {
            if (populated) tr.GetRelation<IItems>().Upsert(new Item { Id = 1, Name = "one" });
            tr.Commit();
        }
        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations());
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IIndexedItems), typeof(IOtherItems)]);
        Assert.Equal(["read", "write"], _countingDb.Starts);
        using (var tr = _db.StartReadOnlyTransaction())
        {
            Assert.Equal(42ul, tr.GetCommitUlong());
            if (populated) Assert.Equal(1ul, tr.GetRelation<IIndexedItems>().FindByName("one").Id);
            else Assert.Empty(tr.GetRelation<IIndexedItems>());
            using var cursor = tr.KeyValueDBTransaction.CreateCursor();
            Assert.Equal(3, cursor.GetKeyValueCount([0, 5]));
        }
        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations());
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IIndexedItems), typeof(IOtherItems)]);
        Assert.Equal(["read"], _countingDb.Starts);
    }

    [Fact]
    public async Task RepairsSecondaryIndexesWithoutSchemaChanges()
    {
        await _db.InitializeRelations([typeof(IIndexedItems)]);
        using (var tr = await _db.StartWritingTransaction())
        {
            tr.GetRelation<IIndexedItems>().Upsert(new IndexedItem { Id = 1, Name = "one" });
            tr.Commit();
        }
        using (var tr = await _lowDb.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.EraseAll([4]);
            tr.Commit();
        }
        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations());
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IIndexedItems)]);
        Assert.Equal(["read", "write"], _countingDb.Starts);
        using var read = _db.StartReadOnlyTransaction();
        Assert.Equal(1ul, read.GetRelation<IIndexedItems>().FindByName("one").Id);
    }

    public class ItemsOnCreate : IRelationOnCreate<IItems>
    {
        public bool Fail { get; set; }
        public int Calls { get; private set; }

        public void OnCreate(IObjectDBTransaction transaction, IItems creating)
        {
            Calls++;
            Assert.False(transaction.KeyValueDBTransaction.IsReadOnly());
            creating.Upsert(new Item { Id = 1, Name = "created" });
            transaction.GetRelation<IOtherItems>().Upsert(new Item { Id = 2, Name = "other" });
            if (Fail) throw new InvalidOperationException("Creation failed");
        }
    }

    [Fact]
    public async Task OnCreateUsesSameWriterAndFailureCanBeRetried()
    {
        var callback = new ItemsOnCreate { Fail = true };
        var builder = new ContainerBuilder();
        builder.RegisterInstance(callback).As<IRelationOnCreate<IItems>>();
        _container = builder.Build();
        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations().WithContainer(_container));
        _countingDb.Starts.Clear();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _db.InitializeRelations([typeof(IItems), typeof(IOtherItems)]));
        Assert.Equal(["read", "write"], _countingDb.Starts);
        using (var read = _lowDb.StartReadOnlyTransaction()) Assert.Equal(0, read.GetKeyValueCount());
        using (var read = _db.StartReadOnlyTransaction())
            Assert.Throws<BTDBException>(() => read.GetRelation<IItems>());

        callback.Fail = false;
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IItems), typeof(IOtherItems)]);
        Assert.Equal(["read", "write"], _countingDb.Starts);
        Assert.Equal(2, callback.Calls);
        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations().WithContainer(_container));
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IItems), typeof(IOtherItems)]);
        Assert.Equal(["read"], _countingDb.Starts);
        Assert.Equal(2, callback.Calls);
        using var tr = _db.StartReadOnlyTransaction();
        Assert.Single(tr.GetRelation<IItems>());
        Assert.Single(tr.GetRelation<IOtherItems>());
    }

    [Fact]
    public async Task ConflictingNamesDoNotPublishPartialRegistration()
    {
        _countingDb.Starts.Clear();
        await Assert.ThrowsAsync<BTDBException>(async () =>
            await _db.InitializeRelations([typeof(IItems), typeof(IIndexedItems)]));
        Assert.Equal(["read"], _countingDb.Starts);
        using (var read = _db.StartReadOnlyTransaction())
            Assert.Throws<BTDBException>(() => read.GetRelation<IItems>());
        await _db.InitializeRelations([typeof(IIndexedItems)]);
        using var tr = _db.StartReadOnlyTransaction();
        Assert.Empty(tr.GetRelation<IIndexedItems>());
    }

    public class OtherItemsOnCreate : IRelationOnCreate<IOtherItems>
    {
        public bool Fail { get; set; } = true;

        public void OnCreate(IObjectDBTransaction transaction, IOtherItems creating)
        {
            Assert.Equal(1ul, transaction.GetRelation<IIndexedItems>().FindByName("one").Id);
            if (Fail) throw new InvalidOperationException("Creation failed after schema upgrade");
        }
    }

    [Fact]
    public async Task FailedCreationRollsBackEarlierSchemaAndIndexUpgrades()
    {
        await _db.InitializeRelations([typeof(IItems)]);
        using (var tr = await _db.StartWritingTransaction())
        {
            tr.GetRelation<IItems>().Upsert(new Item { Id = 1, Name = "one" });
            tr.Commit();
        }
        var callback = new OtherItemsOnCreate();
        var builder = new ContainerBuilder();
        builder.RegisterInstance(callback).As<IRelationOnCreate<IOtherItems>>();
        _container = builder.Build();
        ReopenDb(new DBOptions().WithoutAutoRegistrationOfRelations().WithContainer(_container));
        _countingDb.Starts.Clear();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _db.InitializeRelations([typeof(IOtherItems), typeof(IIndexedItems)]));
        Assert.Equal(["read", "write"], _countingDb.Starts);
        using (var read = _lowDb.StartReadOnlyTransaction())
        {
            using var cursor = read.CreateCursor();
            Assert.Equal(1, cursor.GetKeyValueCount([0, 4]));
            Assert.Equal(1, cursor.GetKeyValueCount([0, 5]));
            Assert.Equal(1, cursor.GetKeyValueCount([3]));
            Assert.Equal(0, cursor.GetKeyValueCount([4]));
        }
        callback.Fail = false;
        _countingDb.Starts.Clear();
        await _db.InitializeRelations([typeof(IOtherItems), typeof(IIndexedItems)]);
        Assert.Equal(["read", "write"], _countingDb.Starts);
        using var tr2 = _db.StartReadOnlyTransaction();
        Assert.Equal(1ul, tr2.GetRelation<IIndexedItems>().FindByName("one").Id);
    }

    [Fact]
    public async Task PreservesCustomFactoriesWithoutInvokingThem()
    {
        _db.RegisterCustomRelation(typeof(IItems), _ => throw new InvalidOperationException("Custom factory"));
        await _db.InitializeRelations([typeof(IItems), typeof(IOtherItems)]);
        using var tr = _db.StartReadOnlyTransaction();
        Assert.Equal("Custom factory", Assert.Throws<InvalidOperationException>(() => tr.GetRelation<IItems>()).Message);
        Assert.Empty(tr.GetRelation<IOtherItems>());
    }

    [Fact]
    public async Task ReadOnlyRegistrationDefersSchemaUntilWriterTouchesRelationAndRetriesRollback()
    {
        _db.AllowAutoRegistrationOfRelations = true;
        _countingDb.Starts.Clear();
        using (var read = _db.StartReadOnlyTransaction())
            Assert.Empty(read.GetRelation<IItems>());
        Assert.Equal(["read"], _countingDb.Starts);
        using (var read = _lowDb.StartReadOnlyTransaction()) Assert.Equal(0, read.GetKeyValueCount());
        using (var write = await _db.StartWritingTransaction()) write.Commit();
        using (var read = _lowDb.StartReadOnlyTransaction()) Assert.Equal(0, read.GetKeyValueCount());
        using (var write = await _db.StartWritingTransaction())
        {
            Assert.Empty(write.GetRelation<IItems>());
            Assert.Equal(2, write.KeyValueDBTransaction.GetKeyValueCount());
        }
        using (var read = _lowDb.StartReadOnlyTransaction()) Assert.Equal(0, read.GetKeyValueCount());
        using (var write = await _db.StartWritingTransaction())
        {
            Assert.Empty(write.GetRelation<IItems>());
            write.Commit();
        }
        ReopenDb(new DBOptions());
        using var reopened = _db.StartReadOnlyTransaction();
        Assert.Empty(reopened.GetRelation<IItems>());
        Assert.Equal(2, reopened.KeyValueDBTransaction.GetKeyValueCount());
    }

    [Fact]
    public async Task DeferredOnCreateRunsInWriterAndRetriesAfterFailure()
    {
        var callback = new ItemsOnCreate { Fail = true };
        var builder = new ContainerBuilder();
        builder.RegisterInstance(callback).As<IRelationOnCreate<IItems>>();
        _container = builder.Build();
        ReopenDb(new DBOptions().WithContainer(_container));
        using (var read = _db.StartReadOnlyTransaction()) Assert.Empty(read.GetRelation<IItems>());
        Assert.Equal(0, callback.Calls);
        using (var write = await _db.StartWritingTransaction())
            Assert.Throws<InvalidOperationException>(() => write.GetRelation<IItems>());
        using (var read = _lowDb.StartReadOnlyTransaction()) Assert.Equal(0, read.GetKeyValueCount());
        callback.Fail = false;
        using (var write = await _db.StartWritingTransaction())
        {
            Assert.Equal("created", Assert.Single(write.GetRelation<IItems>()).Name);
            write.Commit();
        }
        using (var write = await _db.StartWritingTransaction())
        {
            Assert.Single(write.GetRelation<IItems>());
            write.Commit();
        }
        Assert.Equal(2, callback.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransactionRegistrationPersistsEmptySchemaAndRetriesAfterRollback(bool explicitFactory)
    {
        _db.AllowAutoRegistrationOfRelations = true;
        using (var tr = await _db.StartWritingTransaction())
        {
            if (explicitFactory) tr.InitRelation<IItems>("Items");
            else tr.GetRelation<IItems>();
            Assert.Equal(2, tr.KeyValueDBTransaction.GetKeyValueCount());
            // Dispose without commit: both schema keys and cached registrations must be discarded.
        }
        using (var read = _lowDb.StartReadOnlyTransaction()) Assert.Equal(0, read.GetKeyValueCount());
        using (var tr = await _db.StartWritingTransaction())
        {
            if (explicitFactory) tr.InitRelation<IItems>("Items");
            else tr.GetRelation<IItems>();
            tr.Commit();
        }
        ReopenDb(new DBOptions());
        using var reopened = _db.StartReadOnlyTransaction();
        Assert.Empty(reopened.GetRelation<IItems>());
        Assert.Equal(2, reopened.KeyValueDBTransaction.GetKeyValueCount());
    }

    sealed class CountingDb(IKeyValueDB inner) : IKeyValueDB
    {
        public List<string> Starts { get; } = [];
        public IKeyValueDBTransaction StartReadOnlyTransaction()
        {
            Starts.Add("read");
            return inner.StartReadOnlyTransaction();
        }
        public ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false)
        {
            Starts.Add("write");
            return inner.StartWritingTransaction(inBatch);
        }
        public IKeyValueDBTransaction StartTransaction()
        {
            Starts.Add("transaction");
            return inner.StartTransaction();
        }
        public void Dispose() => inner.Dispose();
        public bool DurableTransactions { get => inner.DurableTransactions; set => inner.DurableTransactions = value; }
        public string CalcStats() => inner.CalcStats();
        public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats() => inner.GetNativeMemoryStats();
        public ValueTask<bool> Compact(CancellationToken cancellation) => inner.Compact(cancellation);
        public void CreateKvi(CancellationToken cancellation) => inner.CreateKvi(cancellation);
        public ulong? PreserveHistoryUpToCommitUlong { get => inner.PreserveHistoryUpToCommitUlong; set => inner.PreserveHistoryUpToCommitUlong = value; }
        public IKeyValueDBLogger Logger { get => inner.Logger; set => inner.Logger = value; }
        public uint CompactorRamLimitInMb { get => inner.CompactorRamLimitInMb; set => inner.CompactorRamLimitInMb = value; }
        public long MaxTrLogFileSize { get => inner.MaxTrLogFileSize; set => inner.MaxTrLogFileSize = value; }
        public IEnumerable<IKeyValueDBTransaction> Transactions() => inner.Transactions();
        public ulong CompactorReadBytesPerSecondLimit { get => inner.CompactorReadBytesPerSecondLimit; set => inner.CompactorReadBytesPerSecondLimit = value; }
        public ulong CompactorWriteBytesPerSecondLimit { get => inner.CompactorWriteBytesPerSecondLimit; set => inner.CompactorWriteBytesPerSecondLimit = value; }
    }
}
