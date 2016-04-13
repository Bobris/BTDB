using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class RelationInfoResolver : IRelationInfoResolver
    {
        readonly IKeyValueDB _keyValueDB;
        readonly ObjectDB _objectDB;

        public RelationInfoResolver(IKeyValueDB keyValueDB, ObjectDB objectDB)
        {
            _keyValueDB = keyValueDB;
            _objectDB = objectDB;
        }

        public RelationVersionInfo LoadRelationVersionInfo(uint id, uint version, string relationName)
        {
            using (var tr = _keyValueDB.StartTransaction())
            {
                tr.SetKeyPrefix(ObjectDB.RelationVersionsPrefix);
                var key = TableInfo.BuildKeyForTableVersions(id, version);
                if (!tr.FindExactKey(key))
                    throw new BTDBException($"Missing RelationVersionInfo Id:{id} Version:{version}");
                return RelationVersionInfo.Load(new KeyValueDBValueReader(tr), _objectDB.FieldHandlerFactory, relationName);
            }
        }

        public IFieldHandlerFactory FieldHandlerFactory => _objectDB.FieldHandlerFactory;

        public ITypeConvertorGenerator TypeConvertorGenerator => _objectDB.TypeConvertorGenerator;
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

        internal RelationInfo CreateByName(IKeyValueDBTransaction tr, string name, Type interfaceType)
        {
            name = string.Intern(name);
            uint id;
            if (!_name2Id.TryGetValue(name, out id))
            {
                id = _freeId++;
                _name2Id[name] = id;
                tr.SetKeyPrefixUnsafe(ObjectDB.RelationNamesPrefix);
                var nameWriter = new ByteBufferWriter();
                nameWriter.WriteString(name);
                var idWriter = new ByteBufferWriter();
                idWriter.WriteVUInt32(id);
                tr.CreateOrUpdateKeyValue(nameWriter.Data, idWriter.Data);
            }
            RelationInfo relation;
            if (_id2Relation.TryGetValue(id, out relation))
            {
                throw new BTDBException($"Relation with name '{name}' was already initialized");
            }
            var clientType = FindClientType(interfaceType);
            var lastPersistedVersion = GetLastPersistedVersion(tr, id);
            relation = new RelationInfo(id, name, _relationInfoResolver, interfaceType, clientType, lastPersistedVersion);
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
            foreach (var relationInfo in _id2Relation)
            {
                yield return relationInfo.Value;
            }
        }

        static Type FindClientType(Type interfaceType)
        {
            var methods = interfaceType.GetMethods();
            foreach (var method in methods)
            {
                if (method.Name != "Insert" && method.Name != "Update" && method.Name != "Upsert")
                    continue;
                var @params = method.GetParameters();
                if (@params.Length != 1)
                    continue;
                return @params[0].ParameterType;
            }
            throw new BTDBException($"Cannot deduce client type from interface {interfaceType.Name}");
        }

        static uint GetLastPersistedVersion(IKeyValueDBTransaction tr, uint id)
        {
            tr.SetKeyPrefix(ObjectDB.RelationVersionsPrefix);
            var key = TableInfo.BuildKeyForTableVersions(id, uint.MaxValue);
            if (tr.Find(ByteBuffer.NewSync(key)) == FindResult.NotFound)
                return 0;
            var key2 = tr.GetKeyAsByteArray();
            var ofs = PackUnpack.LengthVUInt(id);
            if (key2.Length < ofs) return 0;
            if (BitArrayManipulation.CompareByteArray(key, ofs, key2, ofs) != 0) return 0;
            return checked((uint)PackUnpack.UnpackVUInt(key2, ref ofs));
        }
    }
}
