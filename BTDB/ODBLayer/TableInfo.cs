using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    internal class TableInfo
    {
        readonly uint _id;
        readonly string _name;
        uint _lastVersion;
        uint _clientTypeVersion;
        Type _clientType;
        Type _implType;
        readonly ConcurrentDictionary<uint, TableVersionInfo> _tableVersions = new ConcurrentDictionary<uint, TableVersionInfo>();
        Func<IMidLevelDBTransactionInternal, object> _inserter;
        ConcurrentDictionary<uint, Func<IMidLevelDBTransactionInternal, object>> _loaders = new ConcurrentDictionary<uint, Func<IMidLevelDBTransactionInternal, object>>();

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
            set
            {
                _clientType = value;
                _clientTypeVersion = 0;
            }
        }

        internal Func<IMidLevelDBTransactionInternal, object> Inserter
        {
            get { return _inserter; }
        }

        internal void EnsureClientTypeVersion()
        {
            if (_clientTypeVersion != 0) return;
            var props = _clientType.GetProperties();
            var fields = new List<TableFieldInfo>(props.Length);
            foreach (var pi in props)
            {
                if (!pi.CanRead || !pi.CanWrite) continue;
                FieldType ft;
                var pt = pi.PropertyType;
                if (pt == typeof(string))
                {
                    ft = FieldType.String;
                }
                else if (pt == typeof(Byte) || pt == typeof(UInt16) || pt == typeof(UInt32) || pt == typeof(UInt64))
                {
                    ft = FieldType.UInt;
                }
                else if (pt == typeof(SByte) || pt == typeof(Int16) || pt == typeof(Int32) || pt == typeof(Int64))
                {
                    ft = FieldType.Int;
                }
                else
                {
                    throw new BTDBException(string.Format("Type {0} is not supported field type", pt));
                }
                fields.Add(new TableFieldInfo(string.Intern(pi.Name), ft));
            }
            var tvi = new TableVersionInfo(fields.ToArray());
            _tableVersions.TryAdd(_lastVersion + 1, tvi);
            _clientTypeVersion = _lastVersion + 1;
        }
    }
}