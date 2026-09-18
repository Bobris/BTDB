using System;
using System.Collections.Generic;
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

    public RelationsInfo(IRelationInfoResolver relationInfoResolver)
    {
        _relationInfoResolver = relationInfoResolver;
    }

    internal RelationsInfo(RelationsInfo source)
    {
        _relationInfoResolver = source._relationInfoResolver;
        _freeId = source._freeId;
        foreach (var pair in source._name2Id) _name2Id.Add(pair.Key, pair.Value);
        foreach (var pair in source.Id2Relation) Id2Relation.Add(pair.Key, pair.Value);
    }

    internal RelationInfo CreateByName(IInternalObjectDBTransaction tr, string name, Type interfaceType,
        IRelationBuilder builder, bool initialize = true)
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
            relation.Initialize(tr);
            if (needsInitialization)
            {
                tr.RegisterRollbackAction(() =>
                {
                    Id2Relation.Remove(id);
                    ((ObjectDB)tr.Owner).UnregisterRelation(interfaceType);
                });
            }
        }
        Id2Relation[id] = relation;
        return relation;
    }

    internal void LoadRelations(IEnumerable<KeyValuePair<uint, string>> relationNames)
    {
        foreach (var name in relationNames)
        {
            _name2Id[string.Intern(name.Value)] = name.Key;
            if (name.Key >= _freeId) _freeId = name.Key + 1;
        }
    }

    public IEnumerable<RelationInfo> EnumerateRelationInfos()
    {
        return Id2Relation.Values;
    }
}
