using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer;

public class Type2NameRegistry : IType2NameRegistry
{
    readonly ConcurrentDictionary<string, Type> _name2Type = new ConcurrentDictionary<string, Type>(ReferenceEqualityComparer<string>.Instance);
    readonly ConcurrentDictionary<Type, string> _type2Name = new ConcurrentDictionary<Type, string>(ReferenceEqualityComparer<Type>.Instance);
    readonly object _lock = new object();

    public string RegisterType(Type type, string asName)
    {
        asName = string.Intern(asName);
        lock (_lock)
        {
            var existing = FindNameByType(type);
            if (ReferenceEquals(existing, asName)) return existing;
            if (existing != null)
            {
                throw new BTDBException($"Type {type} is already registered as {existing}. Cannot reregister as {asName}.");
            }
            _type2Name.TryAdd(type, asName);
            _name2Type.TryAdd(asName, type);
        }
        return asName;
    }

    public Type FindTypeByName(string name)
    {
        Debug.Assert(string.IsInterned(name) != null);
        Type result;
        if (_name2Type.TryGetValue(name, out result)) return result;
        return null;
    }

    public string FindNameByType(Type type)
    {
        string result;
        if (_type2Name.TryGetValue(type, out result)) return result;
        return null;
    }
}
