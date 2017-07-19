using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    class TablesInfo
    {
        readonly ConcurrentDictionary<uint, TableInfo> _id2Table = new ConcurrentDictionary<uint, TableInfo>();
        readonly ConcurrentDictionary<string, TableInfo> _name2Table = new ConcurrentDictionary<string, TableInfo>(ReferenceEqualityComparer<string>.Instance);
        readonly ConcurrentDictionary<Type, TableInfo> _clientType2Table = new ConcurrentDictionary<Type, TableInfo>(ReferenceEqualityComparer<Type>.Instance);
        readonly object _lock = new object();
        readonly ITableInfoResolver _tableInfoResolver;

        public TablesInfo(ITableInfoResolver tableInfoResolver)
        {
            _tableInfoResolver = tableInfoResolver;
        }

        internal TableInfo FindByType(Type type)
        {
            TableInfo result;
            if (_clientType2Table.TryGetValue(type, out result)) return result;
            return null;
        }

        internal TableInfo FindById(uint id)
        {
            TableInfo result;
            if (_id2Table.TryGetValue(id, out result)) return result;
            return null;
        }

        internal TableInfo FindByName(string name)
        {
            name = string.Intern(name);
            TableInfo result;
            if (_name2Table.TryGetValue(name, out result)) return result;
            return null;
        }

        internal void LoadTables(IEnumerable<KeyValuePair<uint, string>> tableNames)
        {
            lock (_lock)
            {
                foreach (var tableName in tableNames)
                {
                    PrivateCreateTable(tableName.Key, tableName.Value);
                }
            }
        }

        internal TableInfo LinkType2Name(Type type, string name)
        {
            var t = FindByName(name);
            if (t == null)
            {
                lock (_lock)
                {
                    t = FindByName(name) ?? PrivateCreateTable(name);
                }
            }
            t.ClientType = type;
            if (!_clientType2Table.TryAdd(type, t))
            {
                if (FindByType(type).Name != name)
                {
                    throw new BTDBException($"Type {type} is already linked");
                }
            }
            return t;
        }

        TableInfo PrivateCreateTable(string name)
        {
            return PrivateCreateTable((_id2Table.Count == 0) ? 1 : (_id2Table.Keys.Max() + 1), name);
        }

        TableInfo PrivateCreateTable(uint id, string name)
        {
            name = string.Intern(name);
            var t = new TableInfo(id, name, _tableInfoResolver);
            _id2Table.TryAdd(id, t);
            _name2Table.TryAdd(name, t);
            return t;
        }

        public IEnumerable<TableInfo> EnumerateTableInfos()
        {
            lock (_lock)
            {
                foreach (var tableInfo in _name2Table.Values)
                {
                    yield return tableInfo;
                }
            }
        }
    }
}