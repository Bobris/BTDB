using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbTableRemoveOptimizeTest : IDisposable
{
    IKeyValueDB _lowDb;
    IObjectDB _db;

    public ObjectDbTableRemoveOptimizeTest()
    {
        _lowDb = new InMemoryKeyValueDBWithCount();
        OpenDb();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }

    void OpenDb()
    {
        _db = new ObjectDB();
        _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration());
    }

    static KeyValueDBTransactionWithCount GetCountingTransaction(IObjectDBTransaction tr)
    {
        var trimpl = (ObjectDBTransaction)tr;
        return ((KeyValueDBTransactionWithCount)trimpl.KeyValueDBTransaction)!;
    }

    public interface IResourcesTable : IRelation<Resource>
    {
        void Insert(Resource resources);
        int RemoveById(ulong companyId);
    }

    public class Location
    {
        public string Name { get; set; }
    }

    public class RangeLocation : Location
    {
        public long From { get; set; }
        public long To { get; set; }
    }

    public class Resource
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }

        [PrimaryKey(2)] public string ResourceId { get; set; }

        public RangeLocation Location { get; set; }
    }

    [Fact]
    public void FastRemoveCanBeUsed()
    {
        Func<IObjectDBTransaction, IResourcesTable> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IResourcesTable>("FastRemoveCanBeUsed");
            var table = creator(tr);
            for (var i = 0; i < 100; i++)
            {
                table.Insert(new Resource
                {
                    CompanyId = 1,
                    Location = new RangeLocation(),
                    ResourceId = i.ToString()
                });
                table.Insert(new Resource
                {
                    CompanyId = 2,
                    Location = new RangeLocation(),
                    ResourceId = i.ToString()
                });
            }

            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            Assert.Equal(100, table.RemoveById(1));
            AssertCounts(tr, 1, 0);
            Assert.Equal(100, table.Count);
        }
    }

    public class DataDifferentPrefix
    {
        [PrimaryKey(1)] public int A { get; set; }

        [PrimaryKey(2)] [SecondaryKey("S")] public int B { get; set; }

        public int C { get; set; }
    }

    public interface ITableDataDifferentPrefix : IRelation<DataDifferentPrefix>
    {
        void Insert(DataDifferentPrefix data);
        int RemoveById(int a);
    }

    [Fact]
    public void SecIndexesCannotBeRemovedAtOnce()
    {
        Func<IObjectDBTransaction, ITableDataDifferentPrefix> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ITableDataDifferentPrefix>("SecIndexesCannotBeRemovedAtOnce");
            var table = creator(tr);
            for (var i = 0; i < 10; i++)
            {
                table.Insert(new DataDifferentPrefix { A = i % 2, B = i });
            }

            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            Assert.Equal(5, table.RemoveById(0));
            AssertCounts(tr, 1, 5);
            Assert.Equal(5, table.Count);
        }
    }

    public class DataSamePrefix
    {
        [PrimaryKey(1)]
        [SecondaryKey("S", Order = 1)]
        public int A { get; set; }

        [PrimaryKey(2)] public int B { get; set; }

        [PrimaryKey(3)]
        [SecondaryKey("S", Order = 2)]
        public int C { get; set; }
    }

    public interface ITableDataSamePrefix : IRelation<DataSamePrefix>
    {
        void Insert(DataSamePrefix data);
        int RemoveById(int a); //same prefix PK [A], SK("S") [A]
        int RemoveById(int a, int b); //PK [A,B], SK("S") [A C]
    }

    [Fact]
    public void SecIndexesCanBeRemovedAtOnce()
    {
        Func<IObjectDBTransaction, ITableDataSamePrefix> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ITableDataSamePrefix>("SecIndexesCannotBeRemovedAtOnce");
            var table = creator(tr);
            for (var i = 0; i < 10; i++)
            {
                table.Insert(new DataSamePrefix { A = i % 2, B = i % 2, C = i });
            }

            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            Assert.Equal(5, table.RemoveById(0));
            AssertCounts(tr, 2, 0);
            Assert.Equal(5, table.Count);
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            Assert.Equal(5, table.RemoveById(0, 0));
            AssertCounts(tr, 1, 5);
            Assert.Equal(5, table.Count);
        }
    }

    void AssertCounts(IObjectDBTransaction tr, int eraseAll = -1, int eraseCurrent = -1)
    {
        var ctr = GetCountingTransaction(tr);
        Assert.Equal(eraseAll, ctr.EraseRangeCount);
        Assert.Equal(eraseCurrent, ctr.EraseCurrentCount);
    }

    class InMemoryKeyValueDBWithCount : IKeyValueDB
    {
        readonly IKeyValueDB _keyValueDB;

        public InMemoryKeyValueDBWithCount()
        {
            _keyValueDB = new InMemoryKeyValueDB();
        }

        public bool DurableTransactions
        {
            get => _keyValueDB.DurableTransactions;
            set => _keyValueDB.DurableTransactions = value;
        }

        public IKeyValueDBTransaction StartTransaction()
        {
            return new KeyValueDBTransactionWithCount(_keyValueDB.StartTransaction());
        }

        public IKeyValueDBTransaction StartReadOnlyTransaction()
        {
            return new KeyValueDBTransactionWithCount(_keyValueDB.StartReadOnlyTransaction());
        }

        public ValueTask<IKeyValueDBTransaction> StartWritingTransaction()
        {
            return _keyValueDB.StartWritingTransaction();
        }

        public string CalcStats()
        {
            return _keyValueDB.CalcStats();
        }

        public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats()
        {
            return _keyValueDB.GetNativeMemoryStats();
        }

        public bool Compact(CancellationToken cancellation)
        {
            return _keyValueDB.Compact(cancellation);
        }

        public void CreateKvi(CancellationToken cancellation)
        {
            _keyValueDB.CreateKvi(cancellation);
        }

        public ulong? PreserveHistoryUpToCommitUlong
        {
            get => _keyValueDB.PreserveHistoryUpToCommitUlong;
            set => _keyValueDB.PreserveHistoryUpToCommitUlong = value;
        }

        public IKeyValueDBLogger Logger
        {
            get => _keyValueDB.Logger;
            set => _keyValueDB.Logger = value;
        }

        public uint CompactorRamLimitInMb { get; set; }
        public long MaxTrLogFileSize { get; set; }

        public IEnumerable<IKeyValueDBTransaction> Transactions()
        {
            return _keyValueDB.Transactions();
        }

        public ulong CompactorReadBytesPerSecondLimit { get; set; }
        public ulong CompactorWriteBytesPerSecondLimit { get; set; }

        public void Dispose()
        {
            _keyValueDB.Dispose();
        }
    }

    class KeyValueDBCursorWithCount : IKeyValueDBCursorInternal
    {
        KeyValueDBTransactionWithCount _transaction;
        IKeyValueDBCursorInternal _keyValueDBCursorInternalImplementation;

        public KeyValueDBCursorWithCount(KeyValueDBTransactionWithCount transaction,
            IKeyValueDBCursorInternal keyValueDBCursorInternalImplementation)
        {
            _transaction = transaction;
            _keyValueDBCursorInternalImplementation = keyValueDBCursorInternalImplementation;
            if (_transaction.FirstCursor == null)
            {
                _transaction.FirstCursor = this;
                _transaction.LastCursor = this;
            }
            else
            {
                ((IKeyValueDBCursorInternal)_transaction.LastCursor!).NextCursor = this;
                PrevCursor = (IKeyValueDBCursorInternal)_transaction.LastCursor;
                _transaction.LastCursor = this;
            }
        }

        public void Dispose()
        {
            if (PrevCursor == null)
            {
                _transaction.FirstCursor = NextCursor;
                if (_transaction.FirstCursor == null)
                {
                    _transaction.LastCursor = null;
                }
                else
                {
                    ((IKeyValueDBCursorInternal)_transaction.FirstCursor!).PrevCursor = null;
                }
            }
            else
            {
                ((IKeyValueDBCursorInternal)PrevCursor).NextCursor = NextCursor;
                if (NextCursor == null)
                {
                    _transaction.LastCursor = (IKeyValueDBCursorInternal)PrevCursor;
                }
                else
                {
                    ((IKeyValueDBCursorInternal)NextCursor).PrevCursor = PrevCursor;
                }
            }

            _keyValueDBCursorInternalImplementation.Dispose();
        }

        public IKeyValueDBTransaction Transaction => _transaction;

        public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
        {
            return _keyValueDBCursorInternalImplementation.FindFirstKey(in prefix);
        }

        public bool FindLastKey(in ReadOnlySpan<byte> prefix)
        {
            return _keyValueDBCursorInternalImplementation.FindLastKey(in prefix);
        }

        public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
        {
            return _keyValueDBCursorInternalImplementation.FindPreviousKey(in prefix);
        }

        public bool FindNextKey(in ReadOnlySpan<byte> prefix)
        {
            return _keyValueDBCursorInternalImplementation.FindNextKey(in prefix);
        }

        public FindResult Find(scoped in ReadOnlySpan<byte> key, uint prefixLen)
        {
            return _keyValueDBCursorInternalImplementation.Find(in key, prefixLen);
        }

        public long GetKeyIndex()
        {
            return _keyValueDBCursorInternalImplementation.GetKeyIndex();
        }

        public bool FindKeyIndex(in ReadOnlySpan<byte> prefix, long index)
        {
            return _keyValueDBCursorInternalImplementation.FindKeyIndex(in prefix, index);
        }

        public bool FindKeyIndex(long index)
        {
            return _keyValueDBCursorInternalImplementation.FindKeyIndex(index);
        }

        public void Invalidate()
        {
            _keyValueDBCursorInternalImplementation.Invalidate();
        }

        public bool IsValid()
        {
            return _keyValueDBCursorInternalImplementation.IsValid();
        }

        public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
        {
            return _keyValueDBCursorInternalImplementation.GetStorageSizeOfCurrentKey();
        }

        public ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy = false)
        {
            return _keyValueDBCursorInternalImplementation.GetKeyMemory(ref buffer, copy);
        }

        public ReadOnlySpan<byte> GetKeySpan(scoped ref Span<byte> buffer, bool copy = false)
        {
            return _keyValueDBCursorInternalImplementation.GetKeySpan(ref buffer, copy);
        }

        public bool IsValueCorrupted()
        {
            return _keyValueDBCursorInternalImplementation.IsValueCorrupted();
        }

        public ReadOnlyMemory<byte> GetValueMemory(ref Memory<byte> buffer, bool copy = false)
        {
            return _keyValueDBCursorInternalImplementation.GetValueMemory(ref buffer, copy);
        }

        public ReadOnlySpan<byte> GetValueSpan(scoped ref Span<byte> buffer, bool copy = false)
        {
            return _keyValueDBCursorInternalImplementation.GetValueSpan(ref buffer, copy);
        }

        public void SetValue(in ReadOnlySpan<byte> value)
        {
            _keyValueDBCursorInternalImplementation.SetValue(in value);
        }

        public void EraseCurrent()
        {
            _transaction.EraseCurrentCount++;
            _keyValueDBCursorInternalImplementation.EraseCurrent();
        }

        public long EraseUpTo(IKeyValueDBCursor to)
        {
            _transaction.EraseRangeCount++;
            return _keyValueDBCursorInternalImplementation.EraseUpTo(to);
        }

        public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            return _keyValueDBCursorInternalImplementation.CreateOrUpdateKeyValue(in key, in value);
        }

        public UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen)
        {
            return _keyValueDBCursorInternalImplementation.UpdateKeySuffix(in key, prefixLen);
        }

        public void NotifyRemove(ulong startIndex, ulong endIndex)
        {
            _keyValueDBCursorInternalImplementation.NotifyRemove(startIndex, endIndex);
        }

        public void PreNotifyUpsert()
        {
            _keyValueDBCursorInternalImplementation.PreNotifyUpsert();
        }

        public void NotifyInsert(ulong index)
        {
            _keyValueDBCursorInternalImplementation.NotifyInsert(index);
        }

        public IKeyValueDBCursorInternal? PrevCursor { get; set; }

        public IKeyValueDBCursorInternal? NextCursor { get; set; }

        public void NotifyWritableTransaction()
        {
            _keyValueDBCursorInternalImplementation.NotifyWritableTransaction();
        }
    }

    class KeyValueDBTransactionWithCount : IKeyValueDBTransaction
    {
        readonly IKeyValueDBTransaction _keyValueDBTransaction;

        public KeyValueDBTransactionWithCount(IKeyValueDBTransaction keyValueDBTransaction)
        {
            _keyValueDBTransaction = keyValueDBTransaction;
        }

        public IKeyValueDB Owner => _keyValueDBTransaction.Owner;
        public DateTime CreatedTime => _keyValueDBTransaction.CreatedTime;

        public string? DescriptionForLeaks
        {
            get => _keyValueDBTransaction.DescriptionForLeaks;
            set => _keyValueDBTransaction.DescriptionForLeaks = value;
        }

        public IKeyValueDBCursor CreateCursor()
        {
            return _keyValueDBTransaction.CreateCursor();
        }

        public long GetKeyValueCount()
        {
            return _keyValueDBTransaction.GetKeyValueCount();
        }

        public int EraseRangeCount { get; set; }
        public int EraseCurrentCount { get; set; }

        public bool RollbackAdvised
        {
            get => _keyValueDBTransaction.RollbackAdvised;
            set => _keyValueDBTransaction.RollbackAdvised = value;
        }

        public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats()
        {
            return _keyValueDBTransaction.CalcBTreeStats();
        }

        public IKeyValueDBCursor? FirstCursor
        {
            get => _keyValueDBTransaction.FirstCursor;
            set => _keyValueDBTransaction.FirstCursor = value;
        }

        public IKeyValueDBCursor? LastCursor
        {
            get => _keyValueDBTransaction.LastCursor;
            set => _keyValueDBTransaction.LastCursor = value;
        }

        public bool IsWriting()
        {
            return _keyValueDBTransaction.IsWriting();
        }

        public bool IsReadOnly()
        {
            return _keyValueDBTransaction.IsReadOnly();
        }

        public bool IsDisposed()
        {
            return _keyValueDBTransaction.IsDisposed();
        }

        public ulong GetCommitUlong()
        {
            return _keyValueDBTransaction.GetCommitUlong();
        }

        public void SetCommitUlong(ulong value)
        {
            _keyValueDBTransaction.SetCommitUlong(value);
        }

        public uint GetUlongCount()
        {
            return _keyValueDBTransaction.GetUlongCount();
        }

        public ulong GetUlong(uint idx)
        {
            return _keyValueDBTransaction.GetUlong(idx);
        }

        public void SetUlong(uint idx, ulong value)
        {
            _keyValueDBTransaction.SetUlong(idx, value);
        }

        public void NextCommitTemporaryCloseTransactionLog()
        {
            _keyValueDBTransaction.NextCommitTemporaryCloseTransactionLog();
        }

        public void Commit()
        {
            _keyValueDBTransaction.Commit();
        }

        public long GetTransactionNumber()
        {
            return _keyValueDBTransaction.GetTransactionNumber();
        }

        public void Dispose()
        {
            _keyValueDBTransaction.Dispose();
        }
    }

    public class NodeRegistration
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public string NodeId { get; set; }

        [PrimaryKey(3)]
        [SecondaryKey("Channel", IncludePrimaryKeyOrder = 1, Order = 2)]
        [SecondaryKey("Channel2", Order = 3)]
        public string? ChannelId { get; set; }

        [SecondaryKey("Channel", Order = 3)]
        [SecondaryKey("Channel2", IncludePrimaryKeyOrder = 1, Order = 2)]
        public bool IsExclusive { get; set; }
    }

    public interface INodeRegistrationTable : IRelation<NodeRegistration>
    {
        int RemoveById(ulong companyId, string nodeId);
    }

    [Fact]
    public void RemoveByIdPrefixWorks()
    {
        _db.Dispose();
        _lowDb.Dispose();
        var fc = new InMemoryFileCollection();
        _lowDb = new BTreeKeyValueDB(new KeyValueDBOptions()
        {
            FileCollection = fc
        });
        OpenDb();
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<INodeRegistrationTable>();
        t.Upsert(new NodeRegistration
        {
            CompanyId = 1,
            NodeId = "1",
            ChannelId = "1",
            IsExclusive = true
        });
        t.Upsert(new NodeRegistration
        {
            CompanyId = 1,
            NodeId = "1",
            ChannelId = "2",
            IsExclusive = true
        });
        t.Upsert(new NodeRegistration
        {
            CompanyId = 1,
            NodeId = "1",
            ChannelId = "3",
            IsExclusive = true
        });
        Assert.Equal(3, t.RemoveById(1, "1"));
    }
}
