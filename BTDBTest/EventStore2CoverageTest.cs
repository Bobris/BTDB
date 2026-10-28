using System;
using System.Collections.Generic;
using System.IO;
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
}
