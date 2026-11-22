using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using Xunit;

namespace BTDBTest;

public class Layer2MetadataTest
{
    public enum Color { Red, Green = 5 }

    [Flags]
    public enum Rights : long
    {
        None = 0,
        Read = 1,
        Write = 2,
        Negative = -4
    }

    public class Nested
    {
        public string? Name { get; set; }
    }

    public class Everything
    {
        public int Number { get; set; }
        public string? Text { get; set; }
        public Color Color { get; set; }
        public Rights Rights { get; set; }
        public List<int>? Numbers { get; set; }
        public Dictionary<string, List<int>>? Map { get; set; }
        public int? Maybe { get; set; }
        public (int, string) Pair { get; set; }
        public Guid Id { get; set; }
        public byte[]? Data { get; set; }
        public DateTime When { get; set; }
        public Nested? Child { get; set; }
        public List<Nested>? Children { get; set; }
    }

    public class Node
    {
        public string? Name { get; set; }
        public Node? Parent { get; set; }
        public List<Node>? Children { get; set; }
    }

    public class A
    {
        public B? Other { get; set; }
        public int Value { get; set; }
    }

    public class B
    {
        public A? Other { get; set; }
        public string? Value { get; set; }
    }

    public class HasList1
    {
        public List<string>? Items { get; set; }
    }

    public class HasList2
    {
        public List<string>? Others { get; set; }
        public Dictionary<int, List<string>>? Nested { get; set; }
    }

    public class Simple
    {
        public int Value { get; set; }
    }

    public class EventV1
    {
        public int Amount { get; set; }
    }

    public class EventV2
    {
        public int Amount { get; set; }
        public string? Note { get; set; }
    }

    sealed class VersionsMapper : ITypeNameMapper
    {
        readonly FullNameTypeMapper _inner = new();

        public string ToName(Type type) =>
            type == typeof(EventV1) || type == typeof(EventV2) ? "Shop.Event" : _inner.ToName(type);

        public Type? ToType(string name) => _inner.ToType(name);
    }

    static ITypeDescriptor Descriptor(Type type, ITypeNameMapper? mapper = null) =>
        new EventSerializer(mapper).Create(type)!;

    static (Layer2Metadata Metadata, int[] Ids) Store(Layer2Metadata basis, params Type[] types)
    {
        var resolver = new Layer2TypeResolver(basis);
        var ids = types.Select(type => resolver.Resolve(Descriptor(type))).ToArray();
        return (Layer2Metadata.Load(resolver.Build().Bytes.ToArray()), ids);
    }

    static Layer2DescriptorDecoder NewDecoder() => new(new EventDeserializer());

    static byte[] WithChecksum(byte[] file)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(8), XxHash64.Hash(file.AsSpan(16)));
        return file;
    }

    // Offset of a type's record in the file, from the layout in Layer2Metadata's remarks.
    static int RecordOffset(Layer2Metadata metadata, int typeId)
    {
        var bytes = metadata.Bytes.Span;
        int s = metadata.StringCount, t = metadata.TypeCount;
        var stringLength = BinaryPrimitives.ReadInt32LittleEndian(bytes[(24 + 4 * (s - 1))..]);
        var index = typeId - Layer2Metadata.FirstTypeId;
        var start = index == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(bytes[(24 + 4 * s + 4 * (index - 1))..]);
        return 24 + 4 * s + 12 * t + stringLength + start;
    }

    [Fact]
    public void EmptyMetadataHoldsOnlyItsBindingName()
    {
        var metadata = Layer2Metadata.Load(Layer2Metadata.Create("orders").Bytes.ToArray());
        Assert.Equal("orders", metadata.BindingName);
        Assert.Equal(1, metadata.StringCount);
        Assert.Equal(0, metadata.TypeCount);
        Assert.Equal(24 + 4 + "orders".Length, metadata.Bytes.Length);
        Assert.False(metadata.ContainsType(Layer2Metadata.FirstTypeId));
    }

    [Fact]
    public void EveryCategoryRoundTripsAndIsFoundAgain()
    {
        var empty = Layer2Metadata.Create("orders");
        var (metadata, ids) = Store(empty, typeof(Everything));
        // Classes and enums get entries; lists, dictionaries, nullables and tuples are inline.
        Assert.Equal(4, metadata.TypeCount);
        Assert.True(metadata.Extends(empty));
        var again = new Layer2TypeResolver(metadata);
        Assert.Equal(ids[0], again.Resolve(Descriptor(typeof(Everything))));
        Assert.False(again.HasNewTypes);
        Assert.Same(metadata, again.Build());
        var decoded = NewDecoder().Get(metadata, ids[0]);
        Assert.True(decoded.Equals(Descriptor(typeof(Everything))));
        Assert.Equal(new FullNameTypeMapper().ToName(typeof(Everything)), decoded.Name);
    }

    [Fact]
    public void CyclicTypesGiveTheSameFileFromAnyEntry()
    {
        var empty = Layer2Metadata.Create("orders");
        var (fromA, idsA) = Store(empty, typeof(A));
        var (fromB, idsB) = Store(empty, typeof(B));
        Assert.Equal(fromA.Bytes.ToArray(), fromB.Bytes.ToArray());
        var resolver = new Layer2TypeResolver(fromA);
        Assert.Equal(idsA[0], resolver.Resolve(Descriptor(typeof(A))));
        Assert.Equal(idsB[0], resolver.Resolve(Descriptor(typeof(B))));
        Assert.False(resolver.HasNewTypes);
        var decoder = NewDecoder();
        var a = (ObjectTypeDescriptor)decoder.Get(fromA, idsA[0]);
        var b = decoder.Get(fromA, idsB[0]);
        Assert.Same(b, a.Fields.Single(f => f.Key == "Other").Value);
        Assert.Same(a, ((ObjectTypeDescriptor)b).Fields.Single(f => f.Key == "Other").Value);
        Assert.True(a.Equals(Descriptor(typeof(A))));
    }

    [Fact]
    public void SelfReferenceThroughAnInlineListIsOneRecord()
    {
        var (metadata, ids) = Store(Layer2Metadata.Create("orders"), typeof(Node));
        Assert.Equal(1, metadata.TypeCount);
        var resolver = new Layer2TypeResolver(metadata);
        Assert.Equal(ids[0], resolver.Resolve(Descriptor(typeof(Node))));
        Assert.False(resolver.HasNewTypes);
        var node = (ObjectTypeDescriptor)NewDecoder().Get(metadata, ids[0]);
        Assert.Same(node, node.Fields.Single(f => f.Key == "Parent").Value);
        var children = (ListTypeDescriptor)node.Fields.Single(f => f.Key == "Children").Value;
        Assert.Same(node, children.ItemDescriptor);
        Assert.Equal("List<" + node.Name + ">", children.Name);
        Assert.True(node.Equals(Descriptor(typeof(Node))));
    }

    [Fact]
    public void EqualInlineUsesShareOneDescriptor()
    {
        var resolver = new Layer2TypeResolver(Layer2Metadata.Create("orders"));
        var first = resolver.Resolve(Descriptor(typeof(HasList1)));
        var second = resolver.Resolve(Descriptor(typeof(HasList2)));
        var standalone = resolver.Resolve(Descriptor(typeof(List<string>)));
        var metadata = Layer2Metadata.Load(resolver.Build().Bytes.ToArray());
        Assert.Equal(3, metadata.TypeCount);
        var decoder = NewDecoder();
        var items = ((ObjectTypeDescriptor)decoder.Get(metadata, first)).Fields.Single().Value;
        var fields = ((ObjectTypeDescriptor)decoder.Get(metadata, second)).Fields.ToDictionary(f => f.Key, f => f.Value);
        Assert.Same(items, fields["Others"]);
        Assert.Same(items, ((DictionaryTypeDescriptor)fields["Nested"]).ValueDescriptor);
        Assert.Same(items, decoder.Get(metadata, standalone));
        Assert.Equal(2, decoder.StructuralCount); // List<string> and Dictionary<int, List<string>>
    }

    [Fact]
    public void VersionsOfOneNameGetTheirOwnTypes()
    {
        var mapper = new VersionsMapper();
        var resolver = new Layer2TypeResolver(Layer2Metadata.Create("orders"));
        var v1 = resolver.Resolve(Descriptor(typeof(EventV1), mapper));
        var v2 = resolver.Resolve(Descriptor(typeof(EventV2), mapper));
        Assert.NotEqual(v1, v2);
        var metadata = Layer2Metadata.Load(resolver.Build().Bytes.ToArray());
        var again = new Layer2TypeResolver(metadata);
        Assert.Equal(v2, again.Resolve(Descriptor(typeof(EventV2), mapper)));
        Assert.Equal(v1, again.Resolve(Descriptor(typeof(EventV1), mapper)));
        Assert.False(again.HasNewTypes);
        var decoder = NewDecoder();
        Assert.Equal("Shop.Event", decoder.Get(metadata, v1).Name);
        Assert.Equal("Shop.Event", decoder.Get(metadata, v2).Name);
        // "Shop.Event" and "Amount" are stored once.
        Assert.Equal(1 + 3, metadata.StringCount);
    }

    [Fact]
    public void AppendingKeepsEveryEarlierTypeAndString()
    {
        var empty = Layer2Metadata.Create("orders");
        var (first, firstIds) = Store(empty, typeof(Nested));
        var (second, secondIds) = Store(first, typeof(Everything), typeof(Nested));
        Assert.True(second.Extends(first));
        Assert.True(second.Extends(empty));
        Assert.False(first.Extends(second));
        Assert.Equal(firstIds[0], secondIds[1]);
        var decoder = NewDecoder();
        Assert.Same(decoder.Get(first, firstIds[0]), decoder.Get(second, firstIds[0]));
        var unrelated = Store(Layer2Metadata.Create("orders"), typeof(Simple)).Metadata;
        Assert.False(second.Extends(unrelated));
        Assert.False(first.Extends(Layer2Metadata.Create("other")));
    }

    [Fact]
    public void AnyChangedByteIsRejectedOnLoad()
    {
        var file = Store(Layer2Metadata.Create("orders"), typeof(A), typeof(Color)).Metadata.Bytes.ToArray();
        for (var i = 0; i < file.Length; i++)
        {
            var damaged = file.ToArray();
            damaged[i] ^= 0x10;
            Assert.Throws<InvalidDataException>(() => Layer2Metadata.Load(damaged));
        }
        Assert.Throws<InvalidDataException>(() => Layer2Metadata.Load(file.AsMemory(0, file.Length - 1)));
        Assert.Throws<InvalidDataException>(() => Layer2Metadata.Load(file.Concat(new byte[] { 0 }).ToArray()));
        Assert.Throws<InvalidDataException>(() => Layer2Metadata.Load(file.AsMemory(0, 23)));
    }

    [Fact]
    public void DamageWithAValidChecksumFailsOnlyWithInvalidData()
    {
        var types = new[] { typeof(Everything), typeof(Node), typeof(A), typeof(HasList2) };
        var file = Store(Layer2Metadata.Create("orders"), types).Metadata.Bytes.ToArray();
        var random = new Random(42);
        var loaded = 0;
        for (var round = 0; round < 3000; round++)
        {
            var damaged = file.ToArray();
            for (var changes = random.Next(1, 4); changes > 0; changes--)
                damaged[random.Next(16, damaged.Length)] = (byte)random.Next(256);
            Layer2Metadata metadata;
            try
            {
                metadata = Layer2Metadata.Load(WithChecksum(damaged));
            }
            catch (InvalidDataException)
            {
                continue;
            }
            loaded++;
            var decoder = NewDecoder();
            for (var id = Layer2Metadata.FirstTypeId; metadata.ContainsType(id); id++)
                try { decoder.Get(metadata, id); }
                catch (InvalidDataException) { }
            var resolver = new Layer2TypeResolver(metadata);
            foreach (var type in types)
                try { resolver.Resolve(Descriptor(type)); }
                catch (InvalidDataException) { }
        }
        Assert.True(loaded > 100, $"Only {loaded} damaged files loaded; the test exercises too little.");
    }

    [Fact]
    public void CollidingKeyIsVerifiedNotTrusted()
    {
        var stored = Store(Layer2Metadata.Create("orders"), typeof(Nested)).Metadata;
        var other = Store(Layer2Metadata.Create("orders"), typeof(Simple)).Metadata;
        var file = stored.Bytes.ToArray();
        // One type, so the key index is a single entry: give Nested the key of Simple.
        var keyIndex = 24 + 4 * stored.StringCount + 4 * stored.TypeCount;
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(keyIndex), other.KeyEntry(0).Key);
        var patched = Layer2Metadata.Load(WithChecksum(file));
        var resolver = new Layer2TypeResolver(patched);
        Assert.Equal(Layer2Metadata.FirstTypeId + 1, resolver.Resolve(Descriptor(typeof(Simple))));
        Assert.True(resolver.HasNewTypes);
    }

    public class TupleWithNamedItem
    {
        public (Color, int)? MaybePair { get; set; }
        public List<(Color, string)>? Pairs { get; set; }
    }

    [Fact]
    public void NullableAndListOfTupleWithNamedItemDecodeWithFullNameAndPreferredType()
    {
        var (metadata, ids) = Store(Layer2Metadata.Create("orders"), typeof(TupleWithNamedItem));
        var decoded = (ObjectTypeDescriptor)NewDecoder().Get(metadata, ids[0]);
        var fields = decoded.Fields.ToDictionary(f => f.Key, f => f.Value);
        var colorName = new FullNameTypeMapper().ToName(typeof(Color));
        Assert.Equal($"Nullable<Tuple<{colorName},VInt32>>", fields["MaybePair"].Name);
        Assert.True(fields["MaybePair"].Sealed);
        Assert.Equal(typeof((Color, int)?), fields["MaybePair"].GetPreferredType());
        Assert.Equal($"List<Tuple<{colorName},String>>", fields["Pairs"].Name);
        Assert.True(fields["Pairs"].Sealed);
    }

    [Fact]
    public void CyclicStandaloneStructuralAndEmptyTupleRecordsThrowInvalidData()
    {
        static Layer2Metadata BuildRaw(params byte[][] records)
        {
            var builder = new Layer2MetadataBuilder("orders");
            for (var i = 0; i < records.Length; i++)
            {
                var id = builder.ReserveType();
                builder.SetType(id, records[i], (uint)(i + 1));
            }
            return Layer2Metadata.Load(builder.Build().Bytes.ToArray());
        }

        // Self-cycle: type 50 = List<type 50> (reference 50 + 4 = 54)
        var selfCycle = BuildRaw([(byte)TypeCategory.List, 54]);
        var decoder = NewDecoder();
        Assert.Throws<InvalidDataException>(() => decoder.Get(selfCycle, Layer2Metadata.FirstTypeId));
        Assert.Equal(0, decoder.StructuralCount);

        // Mutual cycle: type 50 = List<type 51>, type 51 = List<type 50>
        var mutualCycle = BuildRaw([(byte)TypeCategory.List, 55], [(byte)TypeCategory.List, 54]);
        decoder = NewDecoder();
        Assert.Throws<InvalidDataException>(() => decoder.Get(mutualCycle, Layer2Metadata.FirstTypeId));
        Assert.Equal(0, decoder.StructuralCount);

        // Empty tuple: type 50 = Tuple with 0 items
        var emptyTuple = BuildRaw([(byte)TypeCategory.Tuple, 0]);
        decoder = NewDecoder();
        Assert.Throws<InvalidDataException>(() => decoder.Get(emptyTuple, Layer2Metadata.FirstTypeId));
        Assert.Equal(0, decoder.StructuralCount);
    }

    [Fact]
    public void StringBoundarySplittingUtf8CodePointAndEmptyBindingNameAreRejected()
    {
        // Two strings whose combined bytes are valid UTF-8 ("é" = 0xC3 0xA9), split between the two bytes.
        var split = new byte[24 + 4 * 2 + 2];
        BinaryPrimitives.WriteUInt32LittleEndian(split.AsSpan(0), Layer2Metadata.Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(split.AsSpan(4), Layer2Metadata.FormatVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(split.AsSpan(16), 2); // string count
        BinaryPrimitives.WriteUInt32LittleEndian(split.AsSpan(20), 0); // type count
        BinaryPrimitives.WriteUInt32LittleEndian(split.AsSpan(24), 1); // string 0 end
        BinaryPrimitives.WriteUInt32LittleEndian(split.AsSpan(28), 2); // string 1 end
        split[32] = 0xC3;
        split[33] = 0xA9;
        Assert.Throws<InvalidDataException>(() => Layer2Metadata.Load(WithChecksum(split)));

        // Empty binding name (string 0 has length 0).
        var emptyBinding = new byte[24 + 4];
        BinaryPrimitives.WriteUInt32LittleEndian(emptyBinding.AsSpan(0), Layer2Metadata.Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(emptyBinding.AsSpan(4), Layer2Metadata.FormatVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(emptyBinding.AsSpan(16), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(emptyBinding.AsSpan(20), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(emptyBinding.AsSpan(24), 0);
        Assert.Throws<InvalidDataException>(() => Layer2Metadata.Load(WithChecksum(emptyBinding)));
    }

    [Fact]
    public void DamagedRecordLeavesNothingHalfBuilt()
    {
        var (metadata, ids) = Store(Layer2Metadata.Create("orders"), typeof(A));
        var file = metadata.Bytes.ToArray();
        // B's last byte is its reference to string, a one-byte built-in ID + 4; make it refer to a missing type.
        var b = ids[0] + 1;
        file[RecordOffset(metadata, b) + metadata.Record(b).Length - 1] = 0x7f;
        var damaged = Layer2Metadata.Load(WithChecksum(file));
        var decoder = NewDecoder();
        Assert.Throws<InvalidDataException>(() => decoder.Get(damaged, ids[0]));
        Assert.Throws<InvalidDataException>(() => decoder.Get(damaged, ids[0]));
        Assert.Throws<InvalidDataException>(() => decoder.Get(damaged, b));
        Assert.Equal(0, decoder.StructuralCount);
    }
}
