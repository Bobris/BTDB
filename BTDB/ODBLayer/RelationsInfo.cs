using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using BTDB.Buffer;
using BTDB.KVDBLayer;

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

        public uint GetLastPersistedVersion(uint id)
        {
            using (var tr = _keyValueDB.StartTransaction())
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
    }

    class RelationsInfo
    {
        readonly ConcurrentDictionary<string, RelationInfo> _name2Relation = new ConcurrentDictionary<string, RelationInfo>(ReferenceEqualityComparer<string>.Instance);
        readonly ConcurrentDictionary<uint, RelationInfo> _id2Relation = new ConcurrentDictionary<uint, RelationInfo>();
        readonly object _lock = new object();
        readonly IRelationInfoResolver _relationInfoResolver;

        public RelationsInfo(IRelationInfoResolver relationInfoResolver)
        {
            _relationInfoResolver = relationInfoResolver;
        }

        internal RelationInfo FindByName(string name)
        {
            name = string.Intern(name);
            RelationInfo result;
            if (_name2Relation.TryGetValue(name, out result)) return result;
            return null;
        }

        internal void LoadRelations(IEnumerable<KeyValuePair<uint, string>> relationNames)
        {
            lock (_lock)
            {
                foreach (var name in relationNames)
                {
                    PrivateCreateRelation(name.Key, name.Value);
                }
            }
        }

        RelationInfo PrivateCreateRelation(string name)
        {
            return PrivateCreateRelation((_id2Relation.Count == 0) ? 1 : (_id2Relation.Keys.Max() + 1), name);
        }

        RelationInfo PrivateCreateRelation(uint id, string name)
        {
            name = string.Intern(name);
            var t = new RelationInfo(id, name, _relationInfoResolver);
            _id2Relation.TryAdd(id, t);
            _name2Relation.TryAdd(name, t);
            return t;
        }

        public IEnumerable<RelationInfo> EnumerateRelationInfos()
        {
            lock (_lock)
            {
                foreach (var relationInfo in _name2Relation.Values)
                {
                    yield return relationInfo;
                }
            }
        }

        internal RelationInfo LinkType2Name(Type type, string name)
        {
            var t = FindByName(name);
            if (t == null)
            {
                lock (_lock)
                {
                    t = FindByName(name) ?? PrivateCreateRelation(name);
                }
            }
            t.ClientType = type;
            return t;
        }
    }
}