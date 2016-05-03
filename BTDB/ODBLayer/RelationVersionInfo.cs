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
    }

    class RelationVersionInfo
    {
        readonly IList<TableFieldInfo> _primaryKeyFields;  //field info
        readonly IDictionary<string, SecondaryKeyInfo> _secondaryKeys;

        readonly TableFieldInfo[] _fields;


        public RelationVersionInfo(Dictionary<uint, TableFieldInfo> primaryKeyFields, //order -> info
                                   Dictionary<uint, IList<SecondaryKeyAttribute>> secondaryKeys,  //value field idx -> attrs
                                   TableFieldInfo[] fields)
        {
            _primaryKeyFields = primaryKeyFields.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToList();
            _secondaryKeys = CreateSecondaryKeyInfo(secondaryKeys);
            _fields = fields;
        }

        static IDictionary<string, SecondaryKeyInfo> CreateSecondaryKeyInfo(
                                Dictionary<uint, IList<SecondaryKeyAttribute>> attributes)
        {
            var secondaryKeyInfos = new Dictionary<string, SecondaryKeyInfo>();
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
                var orderedAttrs = indexFields.OrderBy(a => a.Item2.Order != default(uint) ? a.Item2.Order : 1).ToList();
                var info = new SecondaryKeyInfo { Fields = new List<FieldId>() };
                foreach (var attr in orderedAttrs)
                {
                    info.Fields.Add(new FieldId(false, attr.Item1));
                    if (attr.Item2.IncludePrimaryKeyOrder != default(uint))
                        info.Fields.Add(new FieldId(true, attr.Item2.IncludePrimaryKeyOrder));
                }
                secondaryKeyInfos[indexName] = info;
            }
            return secondaryKeyInfos;
        }

        RelationVersionInfo(IList<TableFieldInfo> primaryKeyFields,
                            Dictionary<string, SecondaryKeyInfo> secondaryKeys,
                            TableFieldInfo[] fields)
        {
            _primaryKeyFields = primaryKeyFields;
            _secondaryKeys = secondaryKeys;
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

        internal IReadOnlyCollection<TableFieldInfo> GetPrimaryKeyFields()
        {
            return (IReadOnlyCollection<TableFieldInfo>)_primaryKeyFields;
        }

        internal IReadOnlyCollection<TableFieldInfo> GetAllFields()
        {
            return _primaryKeyFields.Concat(_fields).ToList();
        }

        internal IReadOnlyCollection<TableFieldInfo> GetSecondaryKeyFields(string name)
        {
            SecondaryKeyInfo info;
            if (!_secondaryKeys.TryGetValue(name, out info))
                throw new BTDBException($"Unknown key {name}.");
            var fields = new List<TableFieldInfo>();
            foreach (var field in info.Fields)
            {
                if (field.IsFromPrimaryKey)
                    fields.Add(_primaryKeyFields[(int)field.Index]);
                else
                    fields.Add(fields[(int)field.Index]);
            }
            return fields;
        }

        internal void Save(AbstractBufferedWriter writer)
        {
            writer.WriteVUInt32((uint)_primaryKeyFields.Count);
            foreach (var field in _primaryKeyFields)
            {
                field.Save(writer);
            }

            writer.WriteVUInt32((uint)_secondaryKeys.Count);
            foreach (var key in _secondaryKeys)
            {
                writer.WriteString(key.Key);
                var info = key.Value;
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
                primaryKeys.Add(TableFieldInfo.Load(reader, fieldHandlerFactory, relationName));
            }

            var skCount = reader.ReadVUInt32();
            var secondaryKeys = new Dictionary<string, SecondaryKeyInfo>();
            for (var i = 0; i < skCount; i++)
            {
                var skName = reader.ReadString();
                var info = new SecondaryKeyInfo();
                var cnt = reader.ReadVUInt32();
                info.Fields = new List<FieldId>((int)cnt);
                for (var j = 0; i < cnt; i++)
                {
                    var fromPrimary = reader.ReadBool();
                    var index = reader.ReadVUInt32();
                    info.Fields.Add(new FieldId(fromPrimary, index));
                }
                secondaryKeys.Add(skName, info);
            }

            var fieldCount = reader.ReadVUInt32();
            var fieldInfos = new TableFieldInfo[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                fieldInfos[i] = TableFieldInfo.Load(reader, fieldHandlerFactory, relationName);
            }

            return new RelationVersionInfo(primaryKeys, secondaryKeys, fieldInfos);
        }

        internal bool NeedsCtx()
        {
            return _fields.Any(tfi => tfi.Handler.NeedsCtx());
        }

        internal bool NeedsInit()
        {
            return _fields.Any(tfi => tfi.Handler is IFieldHandlerWithInit);
        }

        internal bool NeedsFreeContent()
        {
            return _fields.Any(tfi => tfi.Handler is ODBDictionaryFieldHandler);
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