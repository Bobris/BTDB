using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest
{
    public class ObjectDbTableRemoveOptimalizeTest : IDisposable
    {
        readonly IKeyValueDB _lowDb;
        IObjectDB _db;

        public ObjectDbTableRemoveOptimalizeTest()
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

        KeyValueDBTransactionWithCount GetCountingTransaction(IObjectDBTransaction tr)
        {
            var trimpl = tr as ObjectDBTransaction;
            var kvtr = trimpl.KeyValueDBTransaction as KeyValueDBTransactionWithCount;
            return kvtr;
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
                AssertCounts(tr, eraseAll: 1, eraseCurrent: 0);
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
                    table.Insert(new DataDifferentPrefix {A = i % 2, B = i});
                }

                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var table = creator(tr);
                Assert.Equal(5, table.RemoveById(0));
                AssertCounts(tr, eraseAll: 1, eraseCurrent: 5);
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
                    table.Insert(new DataSamePrefix {A = i % 2, B = i % 2, C = i});
                }

                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                var table = creator(tr);
                Assert.Equal(5, table.RemoveById(0));
                AssertCounts(tr, eraseAll: 2, eraseCurrent: 0);
                Assert.Equal(5, table.Count);
            }

            using (var tr = _db.StartTransaction())
            {
                var table = creator(tr);
                Assert.Equal(5, table.RemoveById(0, 0));
                AssertCounts(tr, eraseAll: 1, eraseCurrent: 5);
                Assert.Equal(5, table.Count);
            }
        }

        void AssertCounts(IObjectDBTransaction tr, int eraseAll = -1, int eraseCurrent = -1)
        {
            var ctr = GetCountingTransaction(tr);
            Assert.Equal(eraseAll, ctr.EraseAllCount);
            Assert.Equal(eraseCurrent, ctr.EraseCurrentCount);
        }

        class InMemoryKeyValueDBWithCount : IKeyValueDB
        {
            IKeyValueDB _keyValueDB;

            public InMemoryKeyValueDBWithCount()
            {
                _keyValueDB = new InMemoryKeyValueDB();
            }

            public bool DurableTransactions
            {
                get { return _keyValueDB.DurableTransactions; }
                set { _keyValueDB.DurableTransactions = value; }
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
                get { return _keyValueDB.PreserveHistoryUpToCommitUlong; }
                set { _keyValueDB.PreserveHistoryUpToCommitUlong = value; }
            }

            public IKeyValueDBLogger Logger
            {
                get { return _keyValueDB.Logger; }
                set { _keyValueDB.Logger = value; }
            }

            public uint CompactorRamLimitInMb { get; set; }
            public long MaxTrLogFileSize { get; set; }

            public void Dispose()
            {
                _keyValueDB.Dispose();
            }
        }

        class KeyValueDBTransactionWithCount : IKeyValueDBTransaction
        {
            IKeyValueDBTransaction _keyValueDBTransaction;

            public KeyValueDBTransactionWithCount(IKeyValueDBTransaction keyValueDBTransaction)
            {
                _keyValueDBTransaction = keyValueDBTransaction;
            }

            public IKeyValueDB Owner => _keyValueDBTransaction.Owner;

            public string DescriptionForLeaks
            {
                get { return _keyValueDBTransaction.DescriptionForLeaks; }
                set { _keyValueDBTransaction.DescriptionForLeaks = value; }
            }

            public int EraseAllCount { get; set; }
            public int EraseRangeCount { get; set; }
            public int EraseCurrentCount { get; set; }

            public bool RollbackAdvised
            {
                get => _keyValueDBTransaction.RollbackAdvised;
                set => _keyValueDBTransaction.RollbackAdvised = value;
            }

            public void SetKeyPrefix(ByteBuffer prefix)
            {
                _keyValueDBTransaction.SetKeyPrefix(prefix);
            }

            public bool FindFirstKey()
            {
                return _keyValueDBTransaction.FindFirstKey();
            }

            public bool FindLastKey()
            {
                return _keyValueDBTransaction.FindLastKey();
            }

            public bool FindPreviousKey()
            {
                return _keyValueDBTransaction.FindPreviousKey();
            }

            public bool FindNextKey()
            {
                return _keyValueDBTransaction.FindNextKey();
            }

            public FindResult Find(ByteBuffer key)
            {
                return _keyValueDBTransaction.Find(key);
            }

            public bool CreateOrUpdateKeyValue(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
            {
                return _keyValueDBTransaction.CreateOrUpdateKeyValue(key, value);
            }

            public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
            {
                return _keyValueDBTransaction.CreateOrUpdateKeyValue(key, value);
            }

            public long GetKeyValueCount()
            {
                return _keyValueDBTransaction.GetKeyValueCount();
            }

            public long GetKeyIndex()
            {
                return _keyValueDBTransaction.GetKeyIndex();
            }

            public bool SetKeyIndex(long index)
            {
                return _keyValueDBTransaction.SetKeyIndex(index);
            }

            public void InvalidateCurrentKey()
            {
                _keyValueDBTransaction.InvalidateCurrentKey();
            }

            public bool IsValidKey()
            {
                return _keyValueDBTransaction.IsValidKey();
            }

            public ByteBuffer GetKey()
            {
                return _keyValueDBTransaction.GetKey();
            }

            public ByteBuffer GetKeyIncludingPrefix()
            {
                return _keyValueDBTransaction.GetKeyIncludingPrefix();
            }

            public ByteBuffer GetValue()
            {
                return _keyValueDBTransaction.GetValue();
            }

            public ReadOnlySpan<byte> GetValueAsReadOnlySpan()
            {
                return _keyValueDBTransaction.GetValueAsReadOnlySpan();
            }

            public void SetValue(ByteBuffer value)
            {
                _keyValueDBTransaction.SetValue(value);
            }

            public void EraseCurrent()
            {
                EraseCurrentCount++;
                _keyValueDBTransaction.EraseCurrent();
            }

            public void EraseAll()
            {
                EraseAllCount++;
                _keyValueDBTransaction.EraseAll();
            }

            public void EraseRange(long firstKeyIndex, long lastKeyIndex)
            {
                EraseRangeCount++;
                _keyValueDBTransaction.EraseRange(firstKeyIndex, lastKeyIndex);
            }

            public bool IsWriting()
            {
                return _keyValueDBTransaction.IsWriting();
            }

            public bool IsReadOnly()
            {
                return _keyValueDBTransaction.IsReadOnly();
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

            public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
            {
                return _keyValueDBTransaction.GetStorageSizeOfCurrentKey();
            }

            public byte[] GetKeyPrefix()
            {
                return _keyValueDBTransaction.GetKeyPrefix();
            }

            public void Dispose()
            {
                _keyValueDBTransaction.Dispose();
            }
        }
    }
}
