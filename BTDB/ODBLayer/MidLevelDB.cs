using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BTDB.ODBLayer
{
    public class MidLevelDB : IMidLevelDB
    {
        ILowLevelDB _lowLevelDB;
        readonly Type2NameRegistry _type2Name = new Type2NameRegistry();
        readonly TablesInfo _tablesInfo = new TablesInfo();
        bool _dispose;
        internal static readonly byte[] TableNamesPrefix = new byte[] { 0, 0 };
        internal static readonly byte[] TableVersionsPrefix = new byte[] { 0, 1 };

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
            _tablesInfo.LoadTables(LoadTablesEnum());
        }

        IEnumerable<string> LoadTablesEnum()
        {
            using (var tr = _lowLevelDB.StartTransaction())
            {
                tr.SetKeyPrefix(TableNamesPrefix);
                var valueReader = new LowLevelDBValueReader(tr);
                while (tr.Enumerate())
                {
                    valueReader.Restart();
                    yield return valueReader.ReadString();
                }
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
    }
}