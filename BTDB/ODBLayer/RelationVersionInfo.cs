using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    internal struct FieldId : IEquatable<FieldId>
    {
        readonly bool _isFromPrimaryKey;
        readonly uint _index;

        public bool IsFromPrimaryKey => _isFromPrimaryKey;
        public uint Index => _index;

        public FieldId(bool isFromPrimaryKey, uint index)
        {
            _isFromPrimaryKey = isFromPrimaryKey;
            _index = index;
        }

        public bool Equals(FieldId other)
        {
            return _isFromPrimaryKey == other.IsFromPrimaryKey && _index == other.Index;
        }
    }

    internal class SecondaryKeyInfo
    {
        public IList<FieldId> Fields { get; set; }
        public string Name { get; set; }

        public static bool Equal(SecondaryKeyInfo a, SecondaryKeyInfo b)
        {
            if (a.Name != b.Name)
                return false;
            if (a.Fields.Count != b.Fields.Count)
                return false;
            for (int i = 0; i < a.Fields.Count; i++)
            {
                if (!a.Fields[i].Equals(b.Fields[i]))
                    return false;
            }
            return true;
        }
    }

    class RelationVersionInfo
    {
        readonly IList<TableFieldInfo> _primaryKeyFields;
        readonly IList<TableFieldInfo> _secondaryKeyFields;
        IDictionary<string, uint> _secondaryKeysNames;
        IDictionary<uint, SecondaryKeyInfo> _secondaryKeys;

        readonly TableFieldInfo[] _fields;


        public RelationVersionInfo(Dictionary<uint, TableFieldInfo> primaryKeyFields,  //order -> info
                                   List<Tuple<int, IList<SecondaryKeyAttribute>>> secondaryKeys,  //positive: sec key field idx, negative: pk order, attrs
                                   TableFieldInfo[] secondaryKeyFields,
                                   TableFieldInfo[] fields, RelationVersionInfo prevVersion)
        {
            _primaryKeyFields = primaryKeyFields.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToList();
            _secondaryKeyFields = secondaryKeyFields;
            CreateSecondaryKeyInfo(secondaryKeys, primaryKeyFields, prevVersion);
            _fields = fields;
        }

        void CreateSecondaryKeyInfo(List<Tuple<int, IList<SecondaryKeyAttribute>>> attributes,
                                    Dictionary<uint, TableFieldInfo> primaryKeyFields,
                                    RelationVersionInfo prevVersion)
        {
            _secondaryKeys = new Dictionary<uint, SecondaryKeyInfo>();
            _secondaryKeysNames = new Dictionary<string, uint>();
            var skIndexNames = attributes.SelectMany(t => t.Item2).Select(a => a.Name).Distinct();
            foreach (var indexName in skIndexNames)
            {
                var indexFields = new List<Tuple<int, SecondaryKeyAttribute>>(); //fieldIndex, attribute
                foreach (var kv in attributes)
                {
                    var attr = kv.Item2.FirstOrDefault(a => a.Name == indexName);
                    if (attr == null)
                        continue;
                    indexFields.Add(Tuple.Create(kv.Item1, attr));
                }
                var orderedAttrs = indexFields.OrderBy(a => a.Item2.Order).ToList();
                var info = new SecondaryKeyInfo
                {
                    Name = indexName,
                    Fields = new List<FieldId>()
                };
                var usedPKFields = new Dictionary<uint, object>();
                foreach (var attr in orderedAttrs)
                {
                    for (uint i = 1; i <= attr.Item2.IncludePrimaryKeyOrder; i++)
                    {
                        usedPKFields.Add(i, null);
                        var pi = _primaryKeyFields.IndexOf(primaryKeyFields[i]);
                        info.Fields.Add(new FieldId(true, (uint)pi));
                    }
                    if (attr.Item1 < 0)
                    {
                        var pkOrder = (uint)-attr.Item1;
                        usedPKFields.Add(pkOrder, null);
                        var pi = _primaryKeyFields.IndexOf(primaryKeyFields[pkOrder]);
                        info.Fields.Add(new FieldId(true, (uint)pi));
                    }
                    else
                    {
                        info.Fields.Add(new FieldId(false, (uint)attr.Item1));
                    }
                }
                //fill all not present parts of primary key
                foreach (var pk in primaryKeyFields)
                {
                    if (!usedPKFields.ContainsKey(pk.Key))
                        info.Fields.Add(new FieldId(true, (uint)_primaryKeyFields.IndexOf(primaryKeyFields[pk.Key])));
                }
                var skIndex = SelectSecondaryKeyIndex(info, prevVersion);
                _secondaryKeysNames[indexName] = skIndex;
                _secondaryKeys[skIndex] = info;
            }
        }

        uint SelectSecondaryKeyIndex(SecondaryKeyInfo info, RelationVersionInfo prevVersion)
        {
            uint index = 1;
            if (prevVersion != null)
            {
                if (prevVersion._secondaryKeysNames.TryGetValue(info.Name, out index))
                {
                    var prevFields = prevVersion.GetSecondaryKeyFields(index);
                    var currFields = GetSecondaryKeyFields(info);
                    if (SecondaryIndexHasSameDefinition(currFields, prevFields))
                        return index;
                }
                index = 0;
                while (prevVersion._secondaryKeys.ContainsKey(index))
                    index++;
            }
            while (_secondaryKeys.ContainsKey(index))
                index++;
            return index; //use fresh one
        }

        bool SecondaryIndexHasSameDefinition(IReadOnlyCollection<TableFieldInfo> currFields, IReadOnlyCollection<TableFieldInfo> prevFields)
        {
            if (currFields.Count != prevFields.Count)
                return false;
            var curr = currFields.GetEnumerator();
            var prev = prevFields.GetEnumerator();
            while (curr.MoveNext() && prev.MoveNext())
            {
                if (!UnresolvedTableFieldInfo.Equal(curr.Current, prev.Current as UnresolvedTableFieldInfo))
                    return false;
            }
            return true;
        }

        RelationVersionInfo(IList<TableFieldInfo> primaryKeyFields,
                            Dictionary<uint, SecondaryKeyInfo> secondaryKeys,
                            Dictionary<string, uint> secondaryKeysNames,
                            IList<TableFieldInfo> secondaryKeyFields,
                            TableFieldInfo[] fields)
        {
            _primaryKeyFields = primaryKeyFields;
            _secondaryKeys = secondaryKeys;
            _secondaryKeysNames = secondaryKeysNames;
            _secondaryKeyFields = secondaryKeyFields;
            _fields = fields;
        }

        internal TableFieldInfo this[string name]
        {
            get { return _fields.Concat(_primaryKeyFields).FirstOrDefault(tfi => tfi.Name == name); }
        }

        internal IReadOnlyCollection<TableFieldInfo> GetValueFields()
        {
            return _fields;
        }

        internal IList<TableFieldInfo> GetPrimaryKeyFields()
        {
            return _primaryKeyFields;
        }

        internal IReadOnlyCollection<TableFieldInfo> GetAllFields()
        {
            return _primaryKeyFields.Concat(_fields).ToList();
        }

        internal TableFieldInfo GetSecondaryKeyField(int index)
        {
            return _secondaryKeyFields[index];
        }

        internal bool HasSecondaryIndexes => _secondaryKeys.Count > 0;

        internal IDictionary<uint, SecondaryKeyInfo> SecondaryKeys => _secondaryKeys;

        internal IReadOnlyCollection<TableFieldInfo> GetSecondaryKeyFields(uint secondaryKeyIndex)
        {
            SecondaryKeyInfo info;
            if (!_secondaryKeys.TryGetValue(secondaryKeyIndex, out info))
                throw new BTDBException($"Unknown secondary key {secondaryKeyIndex}.");
            return GetSecondaryKeyFields(info);
        }

        IReadOnlyCollection<TableFieldInfo> GetSecondaryKeyFields(SecondaryKeyInfo info)
        {
            var fields = new List<TableFieldInfo>();
            foreach (var field in info.Fields)
            {
                fields.Add(field.IsFromPrimaryKey
                    ? _primaryKeyFields[(int)field.Index]
                    : _secondaryKeyFields[(int)field.Index]);
            }
            return fields;
        }

        public IReadOnlyCollection<TableFieldInfo> GetSecondaryKeyValueKeys(uint secondaryKeyIndex)
        {
            SecondaryKeyInfo info;
            if (!_secondaryKeys.TryGetValue(secondaryKeyIndex, out info))
                throw new BTDBException($"Unknown secondary key {secondaryKeyIndex}.");
            var fields = new List<TableFieldInfo>();
            for (int i = 0; i < _primaryKeyFields.Count; i++)
            {
                if (info.Fields.Any(f => f.IsFromPrimaryKey && f.Index == i))
                    continue; //do not put again into value fields present in secondary key index
                fields.Add(_primaryKeyFields[i]);
            }
            return fields;
        }

        internal uint GetSecondaryKeyIndex(string name)
        {
            uint index;
            if (!_secondaryKeysNames.TryGetValue(name, out index))
                throw new BTDBException($"Unknown secondary key {name}.");
            return index;
        }

        internal void Save(AbstractBufferedWriter writer)
        {
            writer.WriteVUInt32((uint)_primaryKeyFields.Count);
            foreach (var field in _primaryKeyFields)
            {
                field.Save(writer);
            }
            writer.WriteVUInt32((uint)_secondaryKeyFields.Count);
            foreach (var field in _secondaryKeyFields)
            {
                field.Save(writer);
            }
            writer.WriteVUInt32((uint)_secondaryKeys.Count);
            foreach (var key in _secondaryKeys)
            {
                writer.WriteVUInt32(key.Key);
                var info = key.Value;
                writer.WriteVUInt32(0); //unused
                writer.WriteString(info.Name);
                writer.WriteVUInt32((uint)info.Fields.Count);
                foreach (var fi in info.Fields)
                {
                    writer.WriteBool(fi.IsFromPrimaryKey);
                    writer.WriteVUInt32(fi.Index);
                }
            }
            writer.WriteVUInt32((uint)_fields.Length);
            for (var i = 0; i < _fields.Length; i++)
            {
                _fields[i].Save(writer);
            }
        }

        public static RelationVersionInfo LoadUnresolved(AbstractBufferedReader reader, string relationName)
        {
            var pkCount = reader.ReadVUInt32();
            var primaryKeyFields = new List<TableFieldInfo>((int)pkCount);
            for (var i = 0u; i < pkCount; i++)
            {
                primaryKeyFields.Add(UnresolvedTableFieldInfo.Load(reader, relationName, FieldHandlerOptions.Orderable));
            }
            var skFieldCount = reader.ReadVUInt32();
            var secondaryKeyFields = new TableFieldInfo[skFieldCount];
            for (var i = 0; i < skFieldCount; i++)
            {
                secondaryKeyFields[i] = UnresolvedTableFieldInfo.Load(reader, relationName, FieldHandlerOptions.Orderable);
            }
            var skCount = reader.ReadVUInt32();
            var secondaryKeys = new Dictionary<uint, SecondaryKeyInfo>((int)skCount);
            var secondaryKeysNames = new Dictionary<string, uint>((int)skCount);
            for (var i = 0; i < skCount; i++)
            {
                var skIndex = reader.ReadVUInt32();
                var info = new SecondaryKeyInfo();
                reader.SkipVUInt32(); //unused
                info.Name = reader.ReadString();
                var cnt = reader.ReadVUInt32();
                info.Fields = new List<FieldId>((int)cnt);
                for (var j = 0; j < cnt; j++)
                {
                    var fromPrimary = reader.ReadBool();
                    var index = reader.ReadVUInt32();
                    info.Fields.Add(new FieldId(fromPrimary, index));
                }
                secondaryKeys.Add(skIndex, info);
                secondaryKeysNames.Add(info.Name, skIndex);
            }

            var fieldCount = reader.ReadVUInt32();
            var fieldInfos = new TableFieldInfo[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                fieldInfos[i] = UnresolvedTableFieldInfo.Load(reader, relationName, FieldHandlerOptions.None);
            }

            return new RelationVersionInfo(primaryKeyFields, secondaryKeys, secondaryKeysNames, secondaryKeyFields, fieldInfos);
        }

        public void ResolveFieldHandlers(IFieldHandlerFactory fieldHandlerFactory)
        {
            for (var i = 0; i < _primaryKeyFields.Count; i++)
                _primaryKeyFields[i] = ((UnresolvedTableFieldInfo)_primaryKeyFields[i]).Resolve(fieldHandlerFactory);
            for (var i = 0; i < _secondaryKeyFields.Count; i++)
                _secondaryKeyFields[i] = ((UnresolvedTableFieldInfo)_secondaryKeyFields[i]).Resolve(fieldHandlerFactory);
            for (var i = 0; i < _fields.Length; i++)
                _fields[i] = ((UnresolvedTableFieldInfo)_fields[i]).Resolve(fieldHandlerFactory);
        }

        internal bool NeedsCtx()
        {
            return _fields.Any(tfi => tfi.Handler.NeedsCtx());
        }

        internal bool NeedsInit()
        {
            return _fields.Any(tfi => tfi.Handler is IFieldHandlerWithInit);
        }

        internal static bool Equal(RelationVersionInfo a, RelationVersionInfo b)
        {
            //PKs
            if (a._primaryKeyFields.Count != b._primaryKeyFields.Count) return false;
            for (int i = 0; i < a._primaryKeyFields.Count; i++)
            {
                if (!TableFieldInfo.Equal(a._primaryKeyFields[i], b._primaryKeyFields[i])) return false;
            }
            //SKs
            if (a._secondaryKeys.Count != b._secondaryKeys.Count) return false;
            foreach (var key in a._secondaryKeys)
            {
                SecondaryKeyInfo bInfo;
                if (!b._secondaryKeys.TryGetValue(key.Key, out bInfo)) return false;
                if (!SecondaryKeyInfo.Equal(key.Value, bInfo)) return false;
            }
            //Fields
            if (a._fields.Length != b._fields.Length) return false;
            for (int i = 0; i < a._fields.Length; i++)
            {
                if (!TableFieldInfo.Equal(a._fields[i], b._fields[i])) return false;
            }
            return true;
        }
    }
}