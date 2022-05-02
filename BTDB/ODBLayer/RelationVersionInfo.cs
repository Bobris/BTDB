using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

struct FieldId : IEquatable<FieldId>
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

class SecondaryKeyInfo
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

public class RelationVersionInfo
{
    public ReadOnlyMemory<TableFieldInfo> Fields;
    public ReadOnlyMemory<TableFieldInfo> PrimaryKeyFields;
    ReadOnlyMemory<TableFieldInfo> _secondaryKeyFields;
    IDictionary<string, uint> _secondaryKeysNames;
    IDictionary<uint, SecondaryKeyInfo> _secondaryKeys;

    public RelationVersionInfo(Dictionary<uint, TableFieldInfo> primaryKeyFields,  //order -> info
                               List<Tuple<int, IList<SecondaryKeyAttribute>>> secondaryKeys,  //positive: sec key field idx, negative: pk order, attrs
                               ReadOnlyMemory<TableFieldInfo> secondaryKeyFields,
                               ReadOnlyMemory<TableFieldInfo> fields)
    {
        PrimaryKeyFields = primaryKeyFields.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToArray();
        _secondaryKeyFields = secondaryKeyFields;
        CreateSecondaryKeyInfo(secondaryKeys, primaryKeyFields);
        Fields = fields;
    }

    void CreateSecondaryKeyInfo(List<Tuple<int, IList<SecondaryKeyAttribute>>> attributes,
                                Dictionary<uint, TableFieldInfo> primaryKeyFields)
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
                    var pi = PrimaryKeyFields.Span.IndexOf(primaryKeyFields[i]);
                    info.Fields.Add(new FieldId(true, (uint)pi));
                }
                if (attr.Item1 < 0)
                {
                    var pkOrder = (uint)-attr.Item1;
                    usedPKFields.Add(pkOrder, null);
                    var pi = PrimaryKeyFields.Span.IndexOf(primaryKeyFields[pkOrder]);
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
                    info.Fields.Add(new FieldId(true, (uint)PrimaryKeyFields.Span.IndexOf(primaryKeyFields[pk.Key])));
            }
            var skIndex = SelectSecondaryKeyIndex(info);
            _secondaryKeysNames[indexName] = skIndex;
            _secondaryKeys[skIndex] = info;
        }
    }

    uint SelectSecondaryKeyIndex(SecondaryKeyInfo info)
    {
        var index = 0u;
        while (_secondaryKeys.ContainsKey(index))
            index++;
        return index; //use fresh one
    }

    internal RelationVersionInfo(ReadOnlyMemory<TableFieldInfo> primaryKeyFields,
                        Dictionary<uint, SecondaryKeyInfo> secondaryKeys,
                        ReadOnlyMemory<TableFieldInfo> secondaryKeyFields,
                        ReadOnlyMemory<TableFieldInfo> fields)
    {
        PrimaryKeyFields = primaryKeyFields;
        _secondaryKeys = secondaryKeys;
        _secondaryKeysNames = new Dictionary<string, uint>(secondaryKeys.Count);
        foreach (var secondaryKeyInfo in secondaryKeys)
        {
            _secondaryKeysNames.Add(secondaryKeyInfo.Value.Name, secondaryKeyInfo.Key);
        }
        _secondaryKeyFields = secondaryKeyFields;
        Fields = fields;
    }

    internal TableFieldInfo? this[string name]
    {
        get
        {
            foreach (var fieldInfo in Fields.Span)
            {
                if (fieldInfo.Name == name) return fieldInfo;
            }
            foreach (var fieldInfo in PrimaryKeyFields.Span)
            {
                if (fieldInfo.Name == name) return fieldInfo;
            }

            return null;
        }
    }

    internal ReadOnlyMemory<TableFieldInfo> GetAllFields()
    {
        var res = new TableFieldInfo[PrimaryKeyFields.Length + Fields.Length];
        PrimaryKeyFields.CopyTo(res);
        Fields.CopyTo(res.AsMemory(PrimaryKeyFields.Length));
        return res;
    }

    internal TableFieldInfo GetSecondaryKeyField(int index)
    {
        return _secondaryKeyFields.Span[index];
    }

    internal bool HasSecondaryIndexes => _secondaryKeys.Count > 0;

    internal IDictionary<uint, SecondaryKeyInfo> SecondaryKeys => _secondaryKeys;

    public ReadOnlyMemory<TableFieldInfo> SecondaryKeyFields
    {
        get => _secondaryKeyFields;
        set => _secondaryKeyFields = value;
    }

    public IDictionary<string, uint> SecondaryKeysNames
    {
        get => _secondaryKeysNames;
        set => _secondaryKeysNames = value;
    }

    internal ReadOnlySpan<TableFieldInfo> GetSecondaryKeyFields(uint secondaryKeyIndex)
    {
        if (!_secondaryKeys.TryGetValue(secondaryKeyIndex, out var info))
            throw new BTDBException($"Unknown secondary key {secondaryKeyIndex}.");
        return GetSecondaryKeyFields(info);
    }

    ReadOnlySpan<TableFieldInfo> GetSecondaryKeyFields(SecondaryKeyInfo info)
    {
        var fields = new StructList<TableFieldInfo>();
        foreach (var field in info.Fields)
        {
            fields.Add(field.IsFromPrimaryKey
                ? PrimaryKeyFields.Span[(int)field.Index]
                : _secondaryKeyFields.Span[(int)field.Index]);
        }
        return fields;
    }

    internal TableFieldInfo GetFieldInfo(FieldId fi)
    {
        return fi.IsFromPrimaryKey ? PrimaryKeyFields.Span[(int)fi.Index] : _secondaryKeyFields.Span[(int)fi.Index];
    }

    internal uint GetSecondaryKeyIndex(string name)
    {
        if (!_secondaryKeysNames.TryGetValue(name, out var index))
            throw new BTDBException($"Unknown secondary key {name}.");
        return index;
    }

    internal void Save(ref SpanWriter writer)
    {
        writer.WriteVUInt32((uint)PrimaryKeyFields.Length);
        foreach (var field in PrimaryKeyFields.Span)
        {
            field.Save(ref writer);
        }
        writer.WriteVUInt32((uint)_secondaryKeyFields.Length);
        foreach (var field in _secondaryKeyFields.Span)
        {
            field.Save(ref writer);
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

        var fields = Fields.Span;
        writer.WriteVUInt32((uint)fields.Length);
        foreach (var tfi in fields)
        {
            tfi.Save(ref writer);
        }
    }

    public static RelationVersionInfo LoadUnresolved(ref SpanReader reader, string relationName)
    {
        var pkCount = reader.ReadVUInt32();
        var primaryKeyFields = new StructList<TableFieldInfo>();
        primaryKeyFields.Reserve(pkCount);
        for (var i = 0u; i < pkCount; i++)
        {
            primaryKeyFields.Add(UnresolvedTableFieldInfo.Load(ref reader, relationName, FieldHandlerOptions.Orderable));
        }
        var skFieldCount = reader.ReadVUInt32();
        var secondaryKeyFields = new TableFieldInfo[skFieldCount];
        for (var i = 0; i < skFieldCount; i++)
        {
            secondaryKeyFields[i] = UnresolvedTableFieldInfo.Load(ref reader, relationName, FieldHandlerOptions.Orderable);
        }
        var skCount = reader.ReadVUInt32();
        var secondaryKeys = new Dictionary<uint, SecondaryKeyInfo>((int)skCount);
        for (var i = 0; i < skCount; i++)
        {
            var skIndex = reader.ReadVUInt32();
            var info = new SecondaryKeyInfo();
            reader.SkipVUInt32(); //unused
            info.Name = reader.ReadString()!;
            var cnt = reader.ReadVUInt32();
            info.Fields = new List<FieldId>((int)cnt);
            for (var j = 0; j < cnt; j++)
            {
                var fromPrimary = reader.ReadBool();
                var index = reader.ReadVUInt32();
                info.Fields.Add(new FieldId(fromPrimary, index));
            }
            secondaryKeys.Add(skIndex, info);
        }

        var fieldCount = reader.ReadVUInt32();
        var fieldInfos = new TableFieldInfo[fieldCount];
        for (var i = 0; i < fieldCount; i++)
        {
            fieldInfos[i] = UnresolvedTableFieldInfo.Load(ref reader, relationName, FieldHandlerOptions.None);
        }

        return new RelationVersionInfo(primaryKeyFields, secondaryKeys, secondaryKeyFields, fieldInfos);
    }

    public void ResolveFieldHandlers(IFieldHandlerFactory fieldHandlerFactory)
    {
        var resolvedPrimaryKeyFields = new TableFieldInfo[PrimaryKeyFields.Length];
        for (var i = 0; i < PrimaryKeyFields.Length; i++)
            resolvedPrimaryKeyFields[i] = ((UnresolvedTableFieldInfo)PrimaryKeyFields.Span[i]).Resolve(fieldHandlerFactory);
        PrimaryKeyFields = resolvedPrimaryKeyFields;

        var resolvedSecondaryKeyFields = new TableFieldInfo[_secondaryKeyFields.Length];
        for (var i = 0; i < _secondaryKeyFields.Length; i++)
            resolvedSecondaryKeyFields[i] = ((UnresolvedTableFieldInfo)_secondaryKeyFields.Span[i]).Resolve(fieldHandlerFactory);
        _secondaryKeyFields = resolvedSecondaryKeyFields;

        var resolvedFields = new TableFieldInfo[Fields.Length];
        for (var i = 0; i < Fields.Length; i++)
            resolvedFields[i] = ((UnresolvedTableFieldInfo)Fields.Span[i]).Resolve(fieldHandlerFactory);
        Fields = resolvedFields;
    }

    internal bool NeedsCtx()
    {
        foreach (var fieldInfo in Fields.Span)
        {
            if (fieldInfo.Handler!.NeedsCtx()) return true;
        }

        return false;
    }

    internal bool NeedsInit()
    {
        foreach (var fieldInfo in Fields.Span)
        {
            if (fieldInfo.Handler is IFieldHandlerWithInit) return true;
        }

        return false;
    }

    internal static bool Equal(RelationVersionInfo a, RelationVersionInfo b)
    {
        //PKs
        if (a.PrimaryKeyFields.Length != b.PrimaryKeyFields.Length) return false;
        for (int i = 0; i < a.PrimaryKeyFields.Length; i++)
        {
            if (!TableFieldInfo.Equal(a.PrimaryKeyFields.Span[i], b.PrimaryKeyFields.Span[i])) return false;
        }
        //SKs
        if (a._secondaryKeys.Count != b._secondaryKeys.Count) return false;
        foreach (var key in a._secondaryKeys)
        {
            if (!b._secondaryKeys.TryGetValue(key.Key, out var bInfo)) return false;
            if (!SecondaryKeyInfo.Equal(key.Value, bInfo)) return false;
        }
        //Fields
        if (a.Fields.Length != b.Fields.Length) return false;
        for (var i = 0; i < a.Fields.Length; i++)
        {
            if (!TableFieldInfo.Equal(a.Fields.Span[i], b.Fields.Span[i])) return false;
        }
        return true;
    }
}
