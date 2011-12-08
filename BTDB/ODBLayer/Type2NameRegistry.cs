using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    internal class Type2NameRegistry
    {
        readonly ConcurrentDictionary<string, Type> _name2Type = new ConcurrentDictionary<string, Type>(ReferenceEqualityComparer<string>.Instance);
        readonly ConcurrentDictionary<Type, string> _type2Name = new ConcurrentDictionary<Type, string>(ReferenceEqualityComparer<Type>.Instance);
        readonly object _lock = new object();

        internal string RegisterType(Type type, string asName)
        {
            asName = string.Intern(asName);
            lock (_lock)
            {
                var existing = FindNameByType(type);
                if (ReferenceEquals(existing, asName)) return existing;
                if (existing != null)
                {
                    throw new BTDBException(string.Format("Type {0} is already registered as {1}. Cannot reregister as {2}.", type, existing, asName));
                }
                _type2Name.TryAdd(type, asName);
                _name2Type.TryAdd(asName, type);
            }
            return asName;
        }

        internal Type FindTypeByName(string name)
        {
            Debug.Assert(string.IsInterned(name)!=null);
            Type result;
            if (_name2Type.TryGetValue(name, out result)) return result;
            return null;
        }

        internal string FindNameByType(Type type)
        {
            string result;
            if (_type2Name.TryGetValue(type, out result)) return result;
            return null;
        }
    }
}