using System;
using System.Collections.Generic;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class RelationInfoResolver : IRelationInfoResolver
    {
        readonly IFieldHandlerFactory _fieldHandlerFactory;
        readonly ITypeConvertorGenerator _typeConvertorGenerator;

        public RelationInfoResolver(ObjectDB objectDB)
        {
            _fieldHandlerFactory = objectDB.FieldHandlerFactory;
            _typeConvertorGenerator = objectDB.TypeConvertorGenerator;
        }

        public IFieldHandlerFactory FieldHandlerFactory => _fieldHandlerFactory;

        public ITypeConvertorGenerator TypeConvertorGenerator => _typeConvertorGenerator;
    }

    class RelationsInfo
    {
        readonly Dictionary<string, uint> _name2Id = new Dictionary<string, uint>(ReferenceEqualityComparer<string>.Instance);
        readonly Dictionary<uint, RelationInfo> _id2Relation = new Dictionary<uint, RelationInfo>();
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
                tr.KeyValueDBTransaction.SetKeyPrefixUnsafe(ObjectDB.RelationNamesPrefix);
                var nameWriter = new ByteBufferWriter();
                nameWriter.WriteString(name);
                var idWriter = new ByteBufferWriter();
                idWriter.WriteVUInt32(id);
                tr.KeyValueDBTransaction.CreateOrUpdateKeyValue(nameWriter.Data, idWriter.Data);
            }

            if (_id2Relation.TryGetValue(id, out var relation))
            {
                throw new BTDBException($"Relation with name '{name}' was already initialized");
            }
            relation = new RelationInfo(id, name, builder, tr);
            _id2Relation[id] = relation;
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
            return _id2Relation.Values;
        }
    }
}
