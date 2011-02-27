using System;
using System.Collections.Concurrent;

namespace BTDB.ODBLayer
{
    internal class TableInfo
    {
        readonly uint _id;
        readonly string _name;
        uint _lastVersion;
        Type _clientType;
        ConcurrentDictionary<uint, TableVersionInfo> _tableVersions = new ConcurrentDictionary<uint, TableVersionInfo>();
        Func<MidLevelDBTransaction, object> _inserter;
        ConcurrentDictionary<uint, Func<MidLevelDBTransaction, object>> _loaders = new ConcurrentDictionary<uint, Func<MidLevelDBTransaction, object>>();

        internal TableInfo(uint id, string name)
        {
            _id = id;
            _name = name;
        }

        internal uint Id
        {
            get { return _id; }
        }

        internal string Name
        {
            get { return _name; }
        }

        internal Type ClientType
        {
            get { return _clientType; }
            set { _clientType = value; }
        }
    }
}