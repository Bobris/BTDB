using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.Serialization;

namespace BTDB.ODBLayer;

class RelationInfoResolver : IRelationInfoResolver
{
    readonly ObjectDB _objectDB;

    public RelationInfoResolver(ObjectDB objectDB)
    {
        _objectDB = objectDB;
    }

    public IFieldHandlerFactory FieldHandlerFactory => _objectDB.FieldHandlerFactory;
    public ITypeConvertorGenerator TypeConvertorGenerator => _objectDB.TypeConvertorGenerator;
    public ITypeConverterFactory TypeConverterFactory => _objectDB.TypeConverterFactory;
    public IContainer? Container => _objectDB.ActualOptions.Container;
    public IFieldHandlerLogger? FieldHandlerLogger => _objectDB.FieldHandlerLogger;
    public DBOptions ActualOptions => _objectDB.ActualOptions;
}

public class RelationsInfo
{
    readonly Dictionary<string, uint> _name2Id = new(ReferenceEqualityComparer<string>.Instance);
    public readonly Dictionary<uint, RelationInfo> Id2Relation = new();
    uint _freeId = 1;
    readonly IRelationInfoResolver _relationInfoResolver;
    // InitializeRelations publishes a copy; it may replace this one only while nothing changed since it was copied,
    // and a replaced one takes no more relations, so no registration is lost or given an id twice.
    readonly object _lock = new();
    long _version;
    bool _retired;

    public RelationsInfo(IRelationInfoResolver relationInfoResolver)
    {
        _relationInfoResolver = relationInfoResolver;
    }

    internal RelationsInfo(RelationsInfo source, out long sourceVersion)
    {
        _relationInfoResolver = source._relationInfoResolver;
        lock (source._lock)
        {
            sourceVersion = source._version;
            _freeId = source._freeId;
            foreach (var pair in source._name2Id) _name2Id.Add(pair.Key, pair.Value);
            foreach (var pair in source.Id2Relation) Id2Relation.Add(pair.Key, pair.Value);
        }
    }

    internal bool TryRetire(long version)
    {
        lock (_lock)
        {
            if (_retired || _version != version) return false;
            _retired = true;
            return true;
        }
    }

    internal void Retire()
    {
        lock (_lock) _retired = true;
    }

    internal void Unretire()
    {
        lock (_lock) _retired = false;
    }

    /// Null when this registry was replaced; the caller retries with the current one.
    internal RelationInfo? CreateByName(IInternalObjectDBTransaction tr, string name, Type interfaceType,
        IRelationBuilder builder, bool initialize = true)
    {
        lock (_lock)
        {
            if (_retired) return null;
            _version++;
            return CreateByNameLocked(tr, name, interfaceType, builder, initialize);
        }
    }

    RelationInfo CreateByNameLocked(IInternalObjectDBTransaction tr, string name, Type interfaceType,
        IRelationBuilder builder, bool initialize)
    {
        name = string.Intern(name);
        if (!_name2Id.TryGetValue(name, out var id))
        {
            id = _freeId++;
            _name2Id[name] = id;
        }

        if (Id2Relation.TryGetValue(id, out var relation))
        {
            _relationInfoResolver.ActualOptions.ThrowBTDBException(
                $"Relation with name '{name}' was already initialized");
        }

        relation = new(id, name, builder, tr, false);
        if (initialize)
        {
            var needsInitialization = relation.NeedsInitialization(tr);
            if (tr.KeyValueDBTransaction.IsReadOnly())
            {
                relation.DeferredInitialization = needsInitialization;
                Id2Relation[id] = relation;
                return relation;
            }
            relation.Initialize(tr);
            if (needsInitialization)
            {
                tr.RegisterRollbackAction(() =>
                {
                    lock (_lock)
                    {
                        _version++;
                        Id2Relation.Remove(id);
                    }

                    ((ObjectDB)tr.Owner).UnregisterRelation(interfaceType);
                });
            }
        }
        Id2Relation[id] = relation;
        return relation;
    }

    internal void LoadRelations(IEnumerable<KeyValuePair<uint, string>> relationNames)
    {
        lock (_lock)
        {
            foreach (var name in relationNames)
            {
                _name2Id[string.Intern(name.Value)] = name.Key;
                if (name.Key >= _freeId) _freeId = name.Key + 1;
            }
        }
    }

    public IEnumerable<RelationInfo> EnumerateRelationInfos()
    {
        lock (_lock) return Id2Relation.Values.ToArray();
    }
}
