using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BTDB.ODBLayer
{
    public class MidLevelDB : IMidLevelDB
    {
        ILowLevelDB _lowLevelDB;
        readonly Type2NameRegistry _type2Name = new Type2NameRegistry();
        TablesInfo _tablesInfo;
        bool _dispose;
        internal static readonly byte[] TableNamesPrefix = new byte[] { 0, 0 };
        internal static readonly byte[] TableVersionsPrefix = new byte[] { 0, 1 };
        internal static readonly byte[] AllObjectsPrefix = new byte[] { 1 };
        TableInfoResolver _tableInfoResolver;
        long _lastObjId;

        internal Type2NameRegistry Type2NameRegistry
        {
            get { return _type2Name; }
        }

        internal TablesInfo TablesInfo
        {
            get { return _tablesInfo; }
        }

        public void Open(ILowLevelDB lowLevelDB, bool dispose)
        {
            if (lowLevelDB == null) throw new ArgumentNullException("lowLevelDB");
            _lowLevelDB = lowLevelDB;
            _dispose = dispose;
            _tableInfoResolver = new TableInfoResolver(lowLevelDB);
            _tablesInfo = new TablesInfo(_tableInfoResolver);
            _lastObjId = 0;
            using (var tr = _lowLevelDB.StartTransaction())
            {
                tr.SetKeyPrefix(AllObjectsPrefix);
                if (tr.FindLastKey())
                {
                    _lastObjId = (long)new LowLevelDBKeyReader(tr).ReadVUInt64();
                }
                _tablesInfo.LoadTables(LoadTablesEnum(tr));
            }
        }

        static IEnumerable<string> LoadTablesEnum(ILowLevelDBTransaction tr)
        {
            tr.SetKeyPrefix(TableNamesPrefix);
            var valueReader = new LowLevelDBValueReader(tr);
            while (tr.Enumerate())
            {
                valueReader.Restart();
                yield return valueReader.ReadString();
            }
        }

        public IMidLevelDBTransaction StartTransaction()
        {
            return new MidLevelDBTransaction(this, _lowLevelDB.StartTransaction());
        }

        public Task<IMidLevelDBTransaction> StartWritingTransaction()
        {
            return _lowLevelDB.StartWritingTransaction()
                .ContinueWith<IMidLevelDBTransaction>(t => new MidLevelDBTransaction(this, t.Result), TaskContinuationOptions.ExecuteSynchronously);
        }

        public string RegisterType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            string name = type.Name;
            if (type.IsInterface && name.StartsWith("I")) name = name.Substring(1);
            return RegisterType(type, name);
        }

        public string RegisterType(Type type, string asName)
        {
            return Type2NameRegistry.RegisterType(type, asName);
        }

        public void Dispose()
        {
            if (_dispose)
            {
                _lowLevelDB.Dispose();
                _dispose = false;
            }
        }

        class TableInfoResolver : ITableInfoResolver
        {
            readonly ILowLevelDB _lowLevelDB;

            internal TableInfoResolver(ILowLevelDB lowLevelDB)
            {
                _lowLevelDB = lowLevelDB;
            }

            uint ITableInfoResolver.GetLastPesistedVersion(uint id)
            {
                using (var tr = _lowLevelDB.StartTransaction())
                {
                    tr.SetKeyPrefix(TableVersionsPrefix);
                    var key = new byte[PackUnpack.LengthVUInt(id) + 1];
                    var ofs = 0;
                    PackUnpack.PackVUInt(key, ref ofs, id);
                    key[ofs] = 0xff;
                    if (tr.FindKey(key, 0, key.Length, FindKeyStrategy.PreferPrevious) == FindKeyResult.NotFound)
                        return 0;
                    var key2 = tr.ReadKey();
                    if (key2.Length < ofs) return 0;
                    if (BitArrayManipulation.CompareByteArray(key, 0, ofs, key2, 0, ofs) != 0) return 0;
                    return checked((uint)PackUnpack.UnpackVUInt(key2, ref ofs));
                }
            }

            TableVersionInfo ITableInfoResolver.LoadTableVersionInfo(uint id, uint version)
            {
                using (var tr = _lowLevelDB.StartTransaction())
                {
                    tr.SetKeyPrefix(TableVersionsPrefix);
                    var key = new byte[PackUnpack.LengthVUInt(id) + PackUnpack.LengthVUInt(version)];
                    var ofs = 0;
                    PackUnpack.PackVUInt(key, ref ofs, id);
                    PackUnpack.PackVUInt(key, ref ofs, version);
                    if (!tr.FindExactKey(key))
                        throw new BTDBException(string.Format("Missing TableVersionInfo Id:{0} Version:{1}", id, version));
                    return TableVersionInfo.Load(new LowLevelDBValueReader(tr));
                }
            }
        }

        internal ulong AllocateNewOid()
        {
            return (ulong)System.Threading.Interlocked.Increment(ref _lastObjId);
        }

        internal ulong GetLastAllocatedOid()
        {
            return (ulong)System.Threading.Interlocked.Read(ref _lastObjId);
        }
    }
}