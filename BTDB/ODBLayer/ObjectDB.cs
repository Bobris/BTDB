using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ObjectDB : IObjectDB, IInstanceRegistry
    {
        IKeyValueDB _keyValueDB;
        readonly Type2NameRegistry _type2Name = new Type2NameRegistry();
        TablesInfo _tablesInfo;
        bool _dispose;
        internal static readonly byte[] TableNamesPrefix = new byte[] { 0, 0 };
        internal static readonly byte[] TableVersionsPrefix = new byte[] { 0, 1 };
        internal static readonly byte[] TableSingletonsPrefix = new byte[] { 0, 2 };
        internal static readonly byte[] LastDictIdKey = new byte[] { 0, 3 };
        internal static readonly byte[] AllObjectsPrefix = new byte[] { 1 };
        internal static readonly byte[] AllDictionariesPrefix = new byte[] { 2 };
        readonly IInstanceRegistry _instanceRegistry = new InstanceRegistry();
        TableInfoResolver _tableInfoResolver;
        long _lastObjId;
        ulong _lastDictId;

        public ObjectDB()
        {
            FieldHandlerFactory = new DefaultODBFieldHandlerFactory(this);
            TypeConvertorGenerator = new DefaultTypeConvertorGenerator();
        }

        internal ulong LastDictId
        {
            get { return _lastDictId; }
        }

        internal Type2NameRegistry Type2NameRegistry
        {
            get { return _type2Name; }
        }

        internal TablesInfo TablesInfo
        {
            get { return _tablesInfo; }
        }

        public void Open(IKeyValueDB keyValueDB, bool dispose)
        {
            if (keyValueDB == null) throw new ArgumentNullException("keyValueDB");
            _keyValueDB = keyValueDB;
            _dispose = dispose;
            _tableInfoResolver = new TableInfoResolver(keyValueDB, this);
            _tablesInfo = new TablesInfo(_tableInfoResolver);
            _lastObjId = 0;
            using (var tr = _keyValueDB.StartTransaction())
            {
                tr.SetKeyPrefix(AllObjectsPrefix);
                if (tr.FindLastKey())
                {
                    _lastObjId = (long)new KeyValueDBKeyReader(tr).ReadVUInt64();
                }
                _tablesInfo.LoadTables(LoadTablesEnum(tr));
                tr.SetKeyPrefix(null);
                if (tr.FindExactKey(LastDictIdKey))
                {
                    _lastDictId = new ByteArrayReader(tr.GetValueAsByteArray()).ReadVUInt64();
                }
            }
        }

        internal void CommitLastDictId(ulong newLastDictId, IKeyValueDBTransaction tr)
        {
            if (_lastDictId != newLastDictId)
            {
                tr.SetKeyPrefix(null);
                var w = new ByteBufferWriter();
                w.WriteVUInt64(newLastDictId);
                tr.CreateOrUpdateKeyValue(LastDictIdKey, w.Data.ToByteArray());
                _lastDictId = newLastDictId;
            }
        }

        static IEnumerable<KeyValuePair<uint, string>> LoadTablesEnum(IKeyValueDBTransaction tr)
        {
            tr.SetKeyPrefixUnsafe(TableNamesPrefix);
            var keyReader = new KeyValueDBKeyReader(tr);
            var valueReader = new KeyValueDBValueReader(tr);
            while (tr.FindNextKey())
            {
                keyReader.Restart();
                valueReader.Restart();
                yield return new KeyValuePair<uint, string>(keyReader.ReadVUInt32(), valueReader.ReadString());
            }
        }

        public IObjectDBTransaction StartTransaction()
        {
            return new ObjectDBTransaction(this, _keyValueDB.StartTransaction());
        }

        public IObjectDBTransaction StartReadOnlyTransaction()
        {
            return new ObjectDBTransaction(this, _keyValueDB.StartReadOnlyTransaction());
        }

        public Task<IObjectDBTransaction> StartWritingTransaction()
        {
            return _keyValueDB.StartWritingTransaction()
                .ContinueWith<IObjectDBTransaction>(t => new ObjectDBTransaction(this, t.Result), TaskContinuationOptions.ExecuteSynchronously);
        }

        public string RegisterType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var name = Type2NameRegistry.FindNameByType(type);
            if (name != null) return name;
            name = type.Name;
            if (type.IsInterface && name.StartsWith("I")) name = name.Substring(1);
            return RegisterType(type, name);
        }

        public string RegisterType(Type type, string asName)
        {
            return Type2NameRegistry.RegisterType(type, asName);
        }

        public Type TypeByName(string name)
        {
            return Type2NameRegistry.FindTypeByName(name);
        }

        public void Dispose()
        {
            if (_dispose)
            {
                _keyValueDB.Dispose();
                _dispose = false;
            }
        }

        class TableInfoResolver : ITableInfoResolver
        {
            readonly IKeyValueDB _keyValueDB;
            readonly ObjectDB _objectDB;

            internal TableInfoResolver(IKeyValueDB keyValueDB, ObjectDB objectDB)
            {
                _keyValueDB = keyValueDB;
                _objectDB = objectDB;
            }

            uint ITableInfoResolver.GetLastPesistedVersion(uint id)
            {
                using (var tr = _keyValueDB.StartTransaction())
                {
                    tr.SetKeyPrefix(TableVersionsPrefix);
                    var key = TableInfo.BuildKeyForTableVersions(id, uint.MaxValue);
                    if (tr.Find(ByteBuffer.NewSync(key)) == FindResult.NotFound)
                        return 0;
                    var key2 = tr.GetKeyAsByteArray();
                    var ofs = PackUnpack.LengthVUInt(id);
                    if (key2.Length < ofs) return 0;
                    if (BitArrayManipulation.CompareByteArray(key, 0, ofs, key2, 0, ofs) != 0) return 0;
                    return checked((uint)PackUnpack.UnpackVUInt(key2, ref ofs));
                }
            }

            TableVersionInfo ITableInfoResolver.LoadTableVersionInfo(uint id, uint version, string tableName)
            {
                using (var tr = _keyValueDB.StartTransaction())
                {
                    tr.SetKeyPrefix(TableVersionsPrefix);
                    var key = TableInfo.BuildKeyForTableVersions(id, version);
                    if (!tr.FindExactKey(key))
                        throw new BTDBException(string.Format("Missing TableVersionInfo Id:{0} Version:{1}", id, version));
                    return TableVersionInfo.Load(new KeyValueDBValueReader(tr), _objectDB.FieldHandlerFactory, tableName);
                }
            }

            public long GetSingletonOid(uint id)
            {
                using (var tr = _keyValueDB.StartTransaction())
                {
                    tr.SetKeyPrefix(TableSingletonsPrefix);
                    var key = new byte[PackUnpack.LengthVUInt(id)];
                    var ofs = 0;
                    PackUnpack.PackVUInt(key, ref ofs, id);
                    if (tr.FindExactKey(key))
                    {
                        return (long) new KeyValueDBValueReader(tr).ReadVUInt64();
                    }
                    return 0;
                }
            }

            public ulong AllocateNewOid()
            {
                return _objectDB.AllocateNewOid();
            }

            public IFieldHandlerFactory FieldHandlerFactory
            {
                get { return _objectDB.FieldHandlerFactory; }
            }

            public ITypeConvertorGenerator TypeConvertorGenerator
            {
                get { return _objectDB.TypeConvertorGenerator; }
            }
        }

        public ITypeConvertorGenerator TypeConvertorGenerator { get; set; }
        public IFieldHandlerFactory FieldHandlerFactory { get; set; }

        internal ulong AllocateNewOid()
        {
            return (ulong)System.Threading.Interlocked.Increment(ref _lastObjId);
        }

        internal ulong GetLastAllocatedOid()
        {
            return (ulong)System.Threading.Interlocked.Read(ref _lastObjId);
        }

        public int RegisterInstance(object content)
        {
            return _instanceRegistry.RegisterInstance(content);
        }

        public object FindInstance(int id)
        {
            return _instanceRegistry.FindInstance(id);
        }
    }
}