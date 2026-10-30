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

    // Names objects are loaded by but not stored under, such as names types had before.
    readonly ConcurrentDictionary<string, Type> _alias2Type =
        new ConcurrentDictionary<string, Type>(ReferenceEqualityComparer<string>.Instance);

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
            // Objects stored under the name are of the storable type, also when its interface took the name first.
            if (!_name2Type.TryAdd(asName, type) && CanBeStored(type)) _name2Type[asName] = type;
        }

        return asName;
    }

    public bool RegisterAlias(Type type, string alias)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (!CanBeStored(type))
            throw new ArgumentException($"Type {type} is never stored, so it needs no alias.", nameof(type));
        alias = string.Intern(alias);
        lock (_lock)
        {
            return ReferenceEquals(_alias2Type.GetOrAdd(alias, type), type);
        }
    }

    static bool CanBeStored(Type type) => !type.IsInterface && !type.IsAbstract;

    public Type FindTypeByName(string name)
    {
        Debug.Assert(string.IsInterned(name) != null);
        // A storable type registered under the name is the one stored under it; an alias stands only for objects
        // stored before, so it precedes just an interface or abstract class.
        _name2Type.TryGetValue(name, out var result);
        if (result != null && CanBeStored(result)) return result;
        return _alias2Type.TryGetValue(name, out var aliased) ? aliased : result;
    }

    public string FindNameByType(Type type)
    {
        string result;
        if (_type2Name.TryGetValue(type, out result)) return result;
        return null;
    }
}
