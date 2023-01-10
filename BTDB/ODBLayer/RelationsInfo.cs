using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

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

    internal RelationInfo CreateByName(IInternalObjectDBTransaction tr, string name, Type interfaceType,
        RelationBuilder builder)
    {
        name = string.Intern(name);
        if (!_name2Id.TryGetValue(name, out var id))
        {
            id = _freeId++;
            _name2Id[name] = id;
            var nameWriter = new SpanWriter();
            nameWriter.WriteBlock(ObjectDB.RelationNamesPrefix);
            nameWriter.WriteString(name);
            var idWriter = new SpanWriter();
            idWriter.WriteVUInt32(id);
            tr.KeyValueDBTransaction.CreateOrUpdateKeyValue(nameWriter.GetSpan(), idWriter.GetSpan());
        }

        if (Id2Relation.TryGetValue(id, out var relation))
        {
            _relationInfoResolver.ActualOptions.ThrowBTDBException($"Relation with name '{name}' was already initialized");
        }
        relation = new(id, name, builder, tr);
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
