using System;
using System.Collections.Generic;
using System.IO;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;

namespace BTDB.EventStore2Layer;

/// <summary>
/// Shared rules of the Layer2 metadata record encoding.
/// A record is its <see cref="TypeCategory"/> byte and a payload. A type reference is a varint: 0–3 tag an inline
/// list, dictionary, nullable or tuple whose payload follows, any other value is a type ID + 4 (built-in or stored).
/// Payloads: class: name, field count, (field name, reference)*; enum: name, header (signed | flags &lt;&lt; 1 |
/// count &lt;&lt; 2), (member name, value)*; list and nullable: item reference; dictionary: key and value references;
/// tuple: count, item references. Names are string IDs.
/// </summary>
static class Layer2Encoding
{
    public const int InlineTags = 4;
    public const int MaxInlineDepth = 64;

    static readonly Dictionary<ITypeDescriptor, int> BuiltInIds = CreateBuiltInIds();

    static Dictionary<ITypeDescriptor, int> CreateBuiltInIds()
    {
        var ids = new Dictionary<ITypeDescriptor, int>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < BasicSerializersFactory.TypeDescriptors.Length; i++)
            ids[BasicSerializersFactory.TypeDescriptors[i]] = i + 2;
        return ids;
    }

    public static bool TryGetBuiltInId(ITypeDescriptor descriptor, out int id) =>
        BuiltInIds.TryGetValue(descriptor, out id);

    public static ITypeDescriptor? BuiltIn(int id) =>
        id >= 2 && id - 2 < BasicSerializersFactory.TypeDescriptors.Length
            ? BasicSerializersFactory.TypeDescriptors[id - 2]
            : null;

    public static bool IsStructural(ITypeDescriptor descriptor) =>
        descriptor is ListTypeDescriptor or DictionaryTypeDescriptor or NullableTypeDescriptor or TupleTypeDescriptor;

    public static TypeCategory Category(ITypeDescriptor descriptor) => descriptor switch
    {
        ObjectTypeDescriptor => TypeCategory.Class,
        EnumTypeDescriptor => TypeCategory.Enum,
        ListTypeDescriptor => TypeCategory.List,
        DictionaryTypeDescriptor => TypeCategory.Dictionary,
        NullableTypeDescriptor => TypeCategory.Nullable,
        TupleTypeDescriptor => TypeCategory.Tuple,
        _ => throw new BTDBException("Layer2 metadata cannot store descriptor " + descriptor.GetType().Name + ".")
    };

    public static int Tag(TypeCategory category) => category switch
    {
        TypeCategory.List => 0,
        TypeCategory.Dictionary => 1,
        TypeCategory.Nullable => 2,
        TypeCategory.Tuple => 3,
        _ => throw new InvalidDataException("Layer2 metadata has a wrong type category.")
    };

    public static TypeCategory CategoryOfTag(int tag) => tag switch
    {
        0 => TypeCategory.List,
        1 => TypeCategory.Dictionary,
        2 => TypeCategory.Nullable,
        3 => TypeCategory.Tuple,
        _ => throw new InvalidDataException("Layer2 metadata has a wrong inline type tag.")
    };

    /// <summary>The nested types of a structural descriptor, in payload order.</summary>
    public static int ChildCount(ITypeDescriptor descriptor) => descriptor switch
    {
        DictionaryTypeDescriptor => 2,
        TupleTypeDescriptor tuple => tuple.ItemCount,
        _ => 1
    };

    public static ITypeDescriptor Child(ITypeDescriptor descriptor, int index) => descriptor switch
    {
        ListTypeDescriptor list => list.ItemDescriptor,
        NullableTypeDescriptor nullable => nullable.ItemDescriptor,
        DictionaryTypeDescriptor dictionary => index == 0 ? dictionary.KeyDescriptor : dictionary.ValueDescriptor,
        TupleTypeDescriptor tuple => tuple.Item(index),
        _ => throw new ArgumentOutOfRangeException(nameof(descriptor))
    };

    public static uint EnumHeader(EnumTypeDescriptor descriptor) =>
        (descriptor.Signed ? 1u : 0) + (descriptor.Flags ? 2u : 0) + 4u * (uint)descriptor.Pairs.Count;
}
