using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB;
using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

public class EventStore2CoverageTest
{
    [Generate]
    public class Holder
    {
        public string? Name { get; set; }
        public List<int>? Numbers { get; set; }
        public object? Any { get; set; }
    }

    [Generate]
    public class TupleRoot
    {
        public (int, Holder) Pair { get; set; }
    }

    [Fact]
    public void RootValueTupleWithObjectCanBeSerialized()
    {
        // Ensure metadata for ValueTuple<int, Holder> exists
        Assert.NotNull(new TupleRoot());
        object value = (5, new Holder { Name = "h" });
        var result = RoundTrip2(value);
        var tuple = Assert.IsType<ValueTuple<int, Holder>>(result);
        Assert.Equal(5, tuple.Item1);
        Assert.Equal("h", tuple.Item2.Name);
    }

    [Fact]
    public void RootSimpleValuesCanBeSerialized()
    {
        Assert.Equal(42, RoundTrip2(42));
        Assert.Equal("str", RoundTrip2("str"));
        Assert.Null(RoundTrip2(null));
        Assert.Equal(TupleTypeDescriptorCoverageTest.Color.Blue, RoundTrip2(TupleTypeDescriptorCoverageTest.Color.Blue));
    }

    [Fact]
    public void SerializerDescriptorOfKnowsOnlySerializedTypes()
    {
        var serializer = new EventSerializer();
        Assert.Null(serializer.DescriptorOf((object?)null));
        Assert.Null(serializer.DescriptorOf(new Holder()));
        Assert.Null(serializer.DescriptorOf(typeof(Holder)));
        Assert.Equal("String", serializer.DescriptorOf("text")!.Name);
        Assert.Equal("String", serializer.DescriptorOf(typeof(string))!.Name);
        Assert.Throws<InvalidOperationException>(() => serializer.IsSafeToLoad(typeof(int)));
        var meta = serializer.Serialize(out var hasMetadata, new Holder()).ToAsyncSafe();
        Assert.True(hasMetadata);
        serializer.ProcessMetadataLog(meta);
        serializer.Serialize(out hasMetadata, new Holder());
        Assert.False(hasMetadata);
        var descriptor = serializer.DescriptorOf(new Holder());
        Assert.Equal(new FullNameTypeMapper().ToName(typeof(Holder)), descriptor!.Name);
        Assert.Same(descriptor, serializer.DescriptorOf(typeof(Holder)));
        Assert.True(serializer.PreserveDescriptors);
        Assert.Equal(typeof(Holder), serializer.LoadAsType(descriptor));
        Assert.Equal(typeof(Holder), serializer.LoadAsType(descriptor, typeof(Holder)));
    }

    [Fact]
    public void DeserializerDescriptorOfAndIsSafeToLoad()
    {
        var deserializer = new EventDeserializer();
        Assert.Null(deserializer.DescriptorOf((object?)null));
        Assert.Null(deserializer.DescriptorOf(new Holder()));
        Assert.Equal("String", deserializer.DescriptorOf("x")!.Name);
        Assert.Equal("String", deserializer.DescriptorOf(typeof(string))!.Name);
        Assert.Null(deserializer.DescriptorOf(typeof(Holder)));
        Assert.True(deserializer.IsSafeToLoad(typeof(int)));
        Assert.True(deserializer.IsSafeToLoad(typeof(List<int>)));
        Assert.False(deserializer.IsSafeToLoad(typeof(IIndirect<Holder>)));
        Assert.False(deserializer.IsSafeToLoad(typeof(List<IIndirect<Holder>>)));

        var serializer = new EventSerializer();
        var value = new Holder { Numbers = [1] };
        var meta = serializer.Serialize(out _, value).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, value).ToAsyncSafe();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out var result, data));
        var holder = Assert.IsType<Holder>(result);
        // loaded list remembers its descriptor, so it can be found
        Assert.Equal("List<VInt32>", deserializer.DescriptorOf(holder.Numbers)!.Name);
        // Deserializer does not map CLR types of user classes to descriptors
        Assert.Null(deserializer.DescriptorOf(holder));

        var noPreserve = new EventDeserializer { PreserveDescriptors = false };
        noPreserve.ProcessMetadataLog(meta);
        Assert.True(noPreserve.Deserialize(out result, data));
        Assert.Null(noPreserve.DescriptorOf(((Holder)result!).Numbers));
    }

    [Fact]
    public void SerializerWithoutInputDescriptorsIgnoresRememberedDescriptors()
    {
        var first = RoundTrip2(new Holder { Name = "a", Numbers = [1, 2] });
        var serializer = new EventSerializer(useInputDescriptors: false);
        Assert.False(serializer.PreserveDescriptors);
        var meta = serializer.Serialize(out _, first!).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, first!).ToAsyncSafe();
        var deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out var result, data));
        Assert.Equal([1, 2], ((Holder)result!).Numbers);
    }

    [Fact]
    public void CorruptedMetadataIsDetected()
    {
        var writer = new MemWriter();
        writer.WriteVInt32(-1);
        writer.WriteUInt8((byte)TypeCategory.List);
        writer.WriteVInt32(1000); // unknown nested type
        writer.WriteVInt32(0);
        var buffer = writer.GetByteBufferAndReset();
        var e = Assert.Throws<BTDBException>(() => new EventDeserializer().ProcessMetadataLog(buffer));
        Assert.Equal("Metadata corrupted", e.Message);
        e = Assert.Throws<BTDBException>(() => new EventSerializer().ProcessMetadataLog(buffer));
        Assert.Equal("Metadata corrupted", e.Message);

        writer = new MemWriter();
        writer.WriteVInt32(-1);
        writer.WriteUInt8((byte)TypeCategory.List);
        writer.WriteVInt32(45); // reserved but unused built-in id
        writer.WriteVInt32(0);
        buffer = writer.GetByteBufferAndReset();
        e = Assert.Throws<BTDBException>(() => new EventDeserializer().ProcessMetadataLog(buffer));
        Assert.Equal("Metadata corrupted", e.Message);
        e = Assert.Throws<BTDBException>(() => new EventSerializer().ProcessMetadataLog(buffer));
        Assert.Equal("Metadata corrupted", e.Message);

        writer = new MemWriter();
        writer.WriteVInt32(-1);
        writer.WriteUInt8((byte)TypeCategory.BuildIn);
        buffer = writer.GetByteBufferAndReset();
        Assert.Throws<BTDBException>(() => new EventDeserializer().ProcessMetadataLog(buffer));
        Assert.Throws<BTDBException>(() => new EventSerializer().ProcessMetadataLog(buffer));

        writer = new MemWriter();
        writer.WriteVInt32(-1);
        writer.WriteUInt8(100);
        buffer = writer.GetByteBufferAndReset();
        Assert.Throws<BTDBException>(() => new EventDeserializer().ProcessMetadataLog(buffer));
        Assert.Throws<BTDBException>(() => new EventSerializer().ProcessMetadataLog(buffer));
    }

    [Fact]
    public void DeserializeReturnsFalseWhenMetadataAreMissingEvenInSkippedField()
    {
        var serializer = new EventSerializer(new RenamingTypeMapper().Map(typeof(Holder), "H"));
        var meta1 = serializer.Serialize(out _, new Holder()).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta1);
        var value = new Holder { Any = new TupleRoot() };
        var meta2 = serializer.Serialize(out var hasMetadata, value).ToAsyncSafe();
        Assert.True(hasMetadata);
        serializer.ProcessMetadataLog(meta2);
        var data = serializer.Serialize(out _, value).ToAsyncSafe();

        // Reader does not have "Any" field so it is skipped, but its type is still unknown
        var deserializer = new EventDeserializer(new RenamingTypeMapper().Map(typeof(OnlyName), "H"));
        deserializer.ProcessMetadataLog(meta1);
        Assert.False(deserializer.Deserialize(out _, data));
        deserializer.ProcessMetadataLog(meta2);
        Assert.True(deserializer.Deserialize(out var result, data));
        Assert.IsType<OnlyName>(result);
    }

    [Generate]
    public class OnlyName
    {
        public string? Name { get; set; }
    }

    // Before the fix a back reference to no object read before failed with ArgumentOutOfRangeException.
    [Fact]
    public void DamagedBackReferenceIsReported()
    {
        var writer = new MemWriter();
        writer.WriteVUInt32(1);
        writer.WriteVUInt32(99);
        var deserializer = new EventDeserializer();
        Assert.Throws<InvalidDataException>(() => deserializer.Deserialize(out _, ByteBuffer.NewAsync(writer.GetSpan().ToArray())));
    }

    [Generate]
    public class MetadataProbe
    {
        public string? Name { get; set; }
        public List<Holder>? Items { get; set; }
    }

    // Before the fix a damaged first type id made processing fill a table up to it, exhausting memory for a large one,
    // or failed with ArgumentOutOfRangeException or NullReferenceException; and a failed log left its types behind.
    [Theory]
    [InlineData(-1_000_000_000)]
    [InlineData(5)]
    [InlineData(0)]
    public void DamagedMetadataTypeIdIsReported(int firstId)
    {
        var value = new MetadataProbe { Name = "n", Items = [new()] };
        var serializer = new EventSerializer();
        var meta = serializer.Serialize(out var hasMetadata, value).ToByteArray();
        Assert.True(hasMetadata);
        serializer.ProcessMetadataLog(ByteBuffer.NewAsync(meta));
        var data = serializer.Serialize(out _, value).ToByteArray();

        var reader = MemReader.CreateFromPinnedSpan(meta);
        var originalFirstId = reader.ReadVInt32();
        var rest = meta.AsSpan((int)reader.GetCurrentPosition()).ToArray();
        // 0 stands for an id that skips two ids and leaves a gap.
        var writer = new MemWriter();
        writer.WriteVInt32(firstId == 0 ? originalFirstId - 2 : firstId);
        writer.WriteBlock(rest);
        var damaged = writer.GetSpan().ToArray();

        var deserializer = new EventDeserializer();
        Assert.Throws<BTDBException>(() => deserializer.ProcessMetadataLog(ByteBuffer.NewAsync(damaged)));
        deserializer.ProcessMetadataLog(ByteBuffer.NewAsync(meta));
        Assert.True(deserializer.Deserialize(out var result, ByteBuffer.NewAsync(data)));
        Assert.Equal("n", Assert.IsType<MetadataProbe>(result).Name);
    }

    // A type argument that maps to no type, as after it was renamed, makes the generic name map to none; before the fix
    // MakeGenericType failed with ArgumentNullException. Damaged, unbalanced names map to none too.
    [Theory]
    [InlineData("System.Collections.Generic.List<Gone.RenamedType>")]
    [InlineData("Gone.Generic<System.Int32>")]
    [InlineData("System.Collections.Generic.List<System.Int32>>")]
    [InlineData("System.Collections.Generic.List<System.Int32<")]
    [InlineData("System.Collections.Generic.Dictionary<System.Int32<System.String>")]
    public void UnmappableGenericTypeNameMapsToNoType(string name)
    {
        Assert.Null(new FullNameTypeMapper().ToType(name));
    }

    // Before the fix an unknown type category failed with ArgumentOutOfRangeException.
    [Fact]
    public void DamagedMetadataTypeCategoryIsReported()
    {
        var writer = new MemWriter();
        writer.WriteVInt32(-1);
        writer.WriteUInt8(200);
        var deserializer = new EventDeserializer();
        Assert.Throws<BTDBException>(() => deserializer.ProcessMetadataLog(ByteBuffer.NewAsync(writer.GetSpan().ToArray())));
    }

    // Before the fix a damaged type id made the version 1 store grow its type table to it, exhausting memory for a large
    // one.
    [Fact]
    public void DamagedTypeIdOfVersion1StoreIsReported()
    {
        var mapping = new TypeSerializers().CreateMapping();
        var writer = new MemWriter();
        writer.WriteVUInt32(3_000_000_000);
        writer.WriteUInt8((byte)TypeCategory.Enum);
        var bytes = writer.GetSpan().ToArray();
        Assert.Throws<BTDBException>(() =>
        {
            var reader = MemReader.CreateFromPinnedSpan(bytes);
            mapping.LoadTypeDescriptors(ref reader);
        });
    }

    [Generate]
    public class WithReadOnlyCollections
    {
        public IReadOnlyList<int>? List { get; set; }
        public IEnumerable<string>? Single { get; set; }
        public IEnumerable<int>? Many { get; set; }
    }

    // Before the fix the list a one-item collection expression makes for IEnumerable<T> failed to save.
    [Fact]
    public void CollectionExpressionsForReadOnlyInterfacesRoundTrip()
    {
        var value = new WithReadOnlyCollections { List = [1], Single = ["a"], Many = [2, 3] };
        var serializer = new EventSerializer();
        var meta = serializer.Serialize(out _, value).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, value);
        var deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out var result, data));
        var read = (WithReadOnlyCollections)result!;
        Assert.Equal([1], read.List);
        Assert.Equal(["a"], read.Single);
        Assert.Equal([2, 3], read.Many);
    }

    [Generate]
    public class WideEventV1
    {
        public DateTimeOffset? Offset { get; set; }
        public decimal? Amount { get; set; }
    }

    [Generate]
    public class WideEventV2
    {
        public DateTimeOffset Offset { get; set; }
        public decimal Amount { get; set; }
    }

    // Before the fix a member changed from a value type larger than Int128, like DateTimeOffset?, failed to load with
    // NotImplementedException.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void WideNullableMemberLoadsAsPlain(EventLayer layer)
    {
        var offset = new DateTimeOffset(2020, 1, 2, 3, 4, 5, TimeSpan.FromHours(2));
        var read = (WideEventV2)RoundTrip(layer, new WideEventV1 { Offset = offset, Amount = 1.5m },
            new RenamingTypeMapper().Map(typeof(WideEventV1), "Wide"),
            new RenamingTypeMapper().Map(typeof(WideEventV2), "Wide"))!;
        Assert.Equal(offset, read.Offset);
        Assert.Equal(offset.Offset, read.Offset.Offset);
        Assert.Equal(1.5m, read.Amount);
    }

    [Generate]
    public class WithUntypedAddresses
    {
        public object? Any { get; set; }
        public List<object>? Many { get; set; }
    }

    // Before the fix IPAddress.Loopback and the like, instances of an internal read-only subclass, failed to store as
    // an object.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void ReadOnlyIPAddressStoresAsObject(EventLayer layer)
    {
        var read = (WithUntypedAddresses)RoundTrip(layer, new WithUntypedAddresses
        {
            Any = System.Net.IPAddress.Loopback,
            Many = [System.Net.IPAddress.IPv6Any, System.Net.IPAddress.Parse("1.2.3.4")]
        })!;
        Assert.Equal(System.Net.IPAddress.Loopback, read.Any);
        Assert.Equal(System.Net.IPAddress.IPv6Any, read.Many![0]);
        Assert.Equal(System.Net.IPAddress.Parse("1.2.3.4"), read.Many[1]);
    }

    [Generate]
    public class WithReadOnlyViews
    {
        public IReadOnlyList<int?>? Numbers { get; set; }
        public IList<string>? Names { get; set; }
        public IList<WithReadOnlyViews>? Children { get; set; }
    }

    // Before the fix lists other than arrays, List, HashSet and Collection, like a ReadOnlyCollection from AsReadOnly or
    // an ImmutableList, failed to store with "Cannot save type".
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void ReadOnlyAndImmutableListsStore(EventLayer layer)
    {
        var read = (WithReadOnlyViews)RoundTrip(layer, new WithReadOnlyViews
        {
            Numbers = new List<int?> { 1, null, 3 }.AsReadOnly(),
            Names = System.Collections.Immutable.ImmutableArray.Create("a", "b"),
            Children = System.Collections.Immutable.ImmutableList.Create(new WithReadOnlyViews { Names = ["x"] })
        })!;
        Assert.Equal([1, null, 3], read.Numbers);
        Assert.Equal(["a", "b"], read.Names);
        Assert.Equal(["x"], read.Children![0].Names);
    }

    [Generate]
    public class ReadOnlyOnlyKid
    {
        public string? Name { get; set; }
    }

    [Generate]
    public class ReadOnlyOnlyParent
    {
        public IReadOnlyList<ReadOnlyOnlyKid>? Children { get; set; }
        public IReadOnlySet<ReadOnlyOnlyKid>? Others { get; set; }
    }

    // Before the fix a list of objects declared only as IReadOnlyList<T> or IReadOnlySet<T> failed to load with
    // "Cannot find collection metadata", as the collection types tried for it left out the read-only ones.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void ListDeclaredOnlyAsReadOnlyLoads(EventLayer layer)
    {
        var read = (ReadOnlyOnlyParent)RoundTrip(layer, new ReadOnlyOnlyParent
        {
            Children = new List<ReadOnlyOnlyKid> { new() { Name = "x" } },
            Others = new HashSet<ReadOnlyOnlyKid> { new() { Name = "y" } }
        })!;
        Assert.Equal("x", read.Children![0].Name);
        Assert.Equal("y", read.Others!.Single().Name);
    }

    [Generate]
    public class DictionaryValue
    {
        public string? Name { get; set; }
    }

    [Generate]
    public class WithOtherDictionaries
    {
        public IDictionary<int, DictionaryValue>? Sorted { get; set; }
        public IReadOnlyDictionary<string, int?>? ReadOnly { get; set; }
        public IDictionary<string, string>? Immutable { get; set; }
    }

    // Before the fix dictionaries other than Dictionary, like SortedDictionary, ReadOnlyDictionary or
    // ImmutableDictionary, failed to store with "Cannot save ... as Dictionary".
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void OtherDictionariesStore(EventLayer layer)
    {
        var read = (WithOtherDictionaries)RoundTrip(layer, new WithOtherDictionaries
        {
            Sorted = new SortedDictionary<int, DictionaryValue> { [2] = new() { Name = "b" }, [1] = new() { Name = "a" } },
            ReadOnly = new System.Collections.ObjectModel.ReadOnlyDictionary<string, int?>(
                new Dictionary<string, int?> { ["x"] = 1, ["y"] = null }),
            Immutable = System.Collections.Immutable.ImmutableDictionary.CreateRange(
                [new KeyValuePair<string, string>("k", "v")])
        })!;
        Assert.Equal("a", read.Sorted![1].Name);
        Assert.Equal("b", read.Sorted[2].Name);
        Assert.Equal(1, read.ReadOnly!["x"]);
        Assert.Null(read.ReadOnly["y"]);
        Assert.Equal("v", read.Immutable!["k"]);
    }

    [Generate]
    public class WithDictionaryInObject
    {
        public Dictionary<int, double>? Typed { get; set; }
        public object? Any { get; set; }
    }

    // Before the fix a dictionary in an object member failed to load with "Cannot find collection metadata for
    // IDictionary", the type preferred for it, although the generator registered the Dictionary of another member.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void DictionaryInObjectMemberLoads(EventLayer layer)
    {
        var read = (WithDictionaryInObject)RoundTrip(layer,
            new WithDictionaryInObject { Any = new Dictionary<int, double> { [1] = 1.5 } })!;
        Assert.Equal(1.5, Assert.IsAssignableFrom<IDictionary<int, double>>(read.Any)[1]);
    }

    [Generate]
    public class WithNestedListInObject
    {
        public List<List<int>>? Typed { get; set; }
        public object? Any { get; set; }
    }

    // Before the fix a nested list in an object member failed to load: its preferred ICollection<ICollection<int>> has
    // no collection metadata, as no member is declared so. It now loads as the lists of objects it holds.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void NestedListInObjectMemberLoads(EventLayer layer)
    {
        var read = (WithNestedListInObject)RoundTrip(layer,
            new WithNestedListInObject { Any = new List<List<int>> { new() { 1, 2 } } })!;
        var outer = Assert.IsAssignableFrom<System.Collections.IList>(read.Any);
        Assert.Equal([1, 2], Assert.IsAssignableFrom<System.Collections.IEnumerable>(outer[0]).Cast<object>().Select(Convert.ToInt32));
    }
}
