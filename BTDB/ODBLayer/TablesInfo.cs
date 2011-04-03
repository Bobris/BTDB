using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    internal class TablesInfo
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

        internal void LoadTables(IEnumerable<string> tableNames)
        {
            lock (_lock)
            {
                foreach (var tableName in tableNames)
                {
                    PrivateCreateTable(tableName);
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
                    t = PrivateCreateTable(name);
                }
            }
            if (_clientType2Table.TryAdd(type, t))
            {
                t.ClientType = type;
            }
            else
            {
                if (FindByType(type).Name != name)
                {
                    throw new BTDBException(string.Format("Type {0} is already linked", type));
                }
            }
            return t;
        }

        TableInfo PrivateCreateTable(string name)
        {
            name = string.Intern(name);
            var t = new TableInfo((uint)(_id2Table.Count + 1), name, _tableInfoResolver);
            _id2Table.TryAdd(t.Id, t);
            _name2Table.TryAdd(t.Name, t);
            return t;
        }
    }
}