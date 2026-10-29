using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer;

public class Type2NameRegistry : IType2NameRegistry
{
    readonly ConcurrentDictionary<string, Type> _name2Type =
        new ConcurrentDictionary<string, Type>(ReferenceEqualityComparer<string>.Instance);

    readonly ConcurrentDictionary<Type, string> _type2Name =
        new ConcurrentDictionary<Type, string>(ReferenceEqualityComparer<Type>.Instance);

    readonly object _lock = new object();

    public string RegisterType(Type type, string asName)
    {
        asName = string.Intern(asName);
        lock (_lock)
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse - used only for testing
            if (type == null)
            {
                _name2Type.TryAdd(asName, type);
                return asName;
            }

            var existing = FindNameByType(type);
            if (ReferenceEquals(existing, asName)) return existing;
            if (existing != null)
            {
                throw new BTDBException(
                    $"Type {type} is already registered as {existing}. Cannot reregister as {asName}.");
            }

            // Objects stored under the name would be loaded as the type registered first. An interface or abstract
            // class, as a field's declared type, is never stored, so it may share the name of its implementation.
            if (_name2Type.TryGetValue(asName, out var other) && other != null && CanBeStored(other) &&
                CanBeStored(type))
            {
                throw new BTDBException(
                    $"Name {asName} is already registered for type {other}. Cannot register type {type} as it.");
            }

            _type2Name.TryAdd(type, asName);
            _name2Type.TryAdd(asName, type);
        }

        return asName;
    }

    static bool CanBeStored(Type type) => !type.IsInterface && !type.IsAbstract;

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
