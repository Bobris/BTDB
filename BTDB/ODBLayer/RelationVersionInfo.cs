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
        public uint Index { get; set; }
        public string Name { get; set; }
    }

    class RelationVersionInfo
    {
        readonly IList<TableFieldInfo> _primaryKeyFields;
        readonly IList<TableFieldInfo> _secondaryKeyFields;
        IDictionary<string, uint> _secondaryKeysNames;
        IDictionary<uint, SecondaryKeyInfo> _secondaryKeys; 

        readonly TableFieldInfo[] _fields;


        public RelationVersionInfo(Dictionary<uint, TableFieldInfo> primaryKeyFields,  //order -> info
                                   Dictionary<uint, IList<SecondaryKeyAttribute>> secondaryKeys,  //sec key field idx -> attrs
                                   TableFieldInfo[] secondaryKeyFields,
                                   TableFieldInfo[] fields, RelationVersionInfo prevVersion)
        {
            _primaryKeyFields = primaryKeyFields.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToList();
            _secondaryKeyFields = secondaryKeyFields;
            CreateSecondaryKeyInfo(secondaryKeys, primaryKeyFields, prevVersion);
            _fields = fields;
        }

        void CreateSecondaryKeyInfo(Dictionary<uint, IList<SecondaryKeyAttribute>> attributes, 
                                    Dictionary<uint, TableFieldInfo> primaryKeyFields, RelationVersionInfo prevVersion)
        {
            var idx = 0u;
            _secondaryKeys = new Dictionary<uint, SecondaryKeyInfo>();
            _secondaryKeysNames = new Dictionary<string, uint>();
            var skIndexNames = attributes.SelectMany(kv => kv.Value).Select(a => a.Name).Distinct();
            foreach (var indexName in skIndexNames)
            {
                var indexFields = new List<Tuple<uint, SecondaryKeyAttribute>>();  //fieldIndex, attribute
                foreach (var kv in attributes)
                {
                    var attr = kv.Value.FirstOrDefault(a => a.Name == indexName);
                    if (attr == null)
                        continue;
                    indexFields.Add(Tuple.Create(kv.Key, attr));
                }
                var orderedAttrs = indexFields.OrderBy(a => a.Item2.Order).ToList();
                var info = new SecondaryKeyInfo
                {
                    Name = indexName,
                    Fields = new List<FieldId>(),
                    Index = SelectSecondaryKeyIndex(indexName, prevVersion)
                };
                var usedPKFields = new Dictionary<uint, object>();
                foreach (var attr in orderedAttrs)
                {
                    for (uint i=1; i<=attr.Item2.IncludePrimaryKeyOrder; i++)
                    {
                        usedPKFields.Add(i, null);
                        var pi = _primaryKeyFields.IndexOf(primaryKeyFields[i]);
                        info.Fields.Add(new FieldId(true, (uint)pi));
                    }
                    info.Fields.Add(new FieldId(false, attr.Item1));
                }
                //fill all not present parts of primary key
                foreach (var pk in primaryKeyFields)
                {
                    if (!usedPKFields.ContainsKey(pk.Key))
                        info.Fields.Add(new FieldId(true, (uint)_primaryKeyFields.IndexOf(primaryKeyFields[pk.Key])));
                }
                _secondaryKeysNames[indexName] = idx;
                _secondaryKeys[idx++] = info;
            }
        }

        uint SelectSecondaryKeyIndex(string indexName, RelationVersionInfo prevVersion)
        {
            uint index = 1;
            if (prevVersion != null)
            {
                if (prevVersion._secondaryKeysNames.TryGetValue(indexName, out index))
                    return index;
                index = 0;
                while (prevVersion._secondaryKeys.ContainsKey(index))
                    index++;
            }
            while (_secondaryKeys.ContainsKey(index))
                index++;
            return index; //use fresh one
        }

        RelationVersionInfo(IList<TableFieldInfo> primaryKeyFields,
                            Dictionary<uint, SecondaryKeyInfo> secondaryKeys,
                            Dictionary<string, uint> secondaryKeysNames,
                            TableFieldInfo[] fields)
        {
            _primaryKeyFields = primaryKeyFields;
            _secondaryKeys = secondaryKeys;
            _secondaryKeysNames = secondaryKeysNames;
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
            var fields = new List<TableFieldInfo>();
            foreach (var field in info.Fields)
            {
                if (field.IsFromPrimaryKey)
                    fields.Add(_primaryKeyFields[(int)field.Index]);
                else
                    fields.Add(_secondaryKeyFields[(int)field.Index]);
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
            writer.WriteVUInt32((uint)_secondaryKeys.Count);
            foreach (var field in _secondaryKeyFields)
            {
                field.Save(writer);
            }
            foreach (var key in _secondaryKeys)
            {
                writer.WriteVUInt32(key.Key);
                var info = key.Value;
                writer.WriteVUInt32(info.Index);
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

        public static RelationVersionInfo Load(AbstractBufferedReader reader, IFieldHandlerFactory fieldHandlerFactory, string relationName)
        {
            var pkCount = reader.ReadVUInt32();
            var primaryKeys = new List<TableFieldInfo>((int)pkCount);
            for (var i = 0u; i < pkCount; i++)
            {
                primaryKeys.Add(TableFieldInfo.Load(reader, fieldHandlerFactory, relationName, FieldHandlerOptions.Orderable));
            }
            var skCount = reader.ReadVUInt32();
            var secondaryKeys = new Dictionary<uint, SecondaryKeyInfo>((int)skCount);
            var secondaryKeysNames = new Dictionary<string, uint>((int)skCount);
            var secondaryKeyFields = new TableFieldInfo[skCount];
            for (var i = 0; i < skCount; i++)
            {
                secondaryKeyFields[i] = TableFieldInfo.Load(reader, fieldHandlerFactory, relationName,
                    FieldHandlerOptions.Orderable);
            }
            for (var i = 0; i < skCount; i++)
            {
                var skIndex = reader.ReadVUInt32();
                var info = new SecondaryKeyInfo();
                info.Index = reader.ReadVUInt32();
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
                fieldInfos[i] = TableFieldInfo.Load(reader, fieldHandlerFactory, relationName, FieldHandlerOptions.None);
            }

            return new RelationVersionInfo(primaryKeys, secondaryKeys, secondaryKeysNames, fieldInfos);
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
                if (!key.Value.Equals(bInfo)) return false;
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