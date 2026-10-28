using System;
using System.Collections.Generic;
using System.Text;
using BTDB;
using BTDB.Bon;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class BonSerializerCoverageTest
{
    public enum SmallEnum : byte
    {
        A = 1,
        B = 2
    }

    public interface IMarker
    {
    }

    [Generate]
    public class Nested : IMarker
    {
        public int Value;
    }

    [Generate]
    public class AllFieldKinds
    {
        public string? Str;
        public byte[]? Bytes;
        public bool Bool;
        public sbyte Int8;
        public short Int16;
        public int Int32;
        public long Int64;
        public byte UInt8;
        public ushort UInt16;
        public uint UInt32;
        public ulong UInt64;
        public DateTime DateTime;
        public Guid Guid;
        public Half Float16;
        public float Float32;
        public double Float64;
        public double? NullableFloat64;
        public SmallEnum Enum;
        public (int A, long B) ValueTuple;
        public Tuple<long, string>? Tuple;
        public int[]? IntArray;
        public List<int>? IntList;
        public HashSet<int>? IntSet;
        public Dictionary<int, string>? Dict;
        public Nested? Nested;
        public IMarker? Marker;
        public object? Obj;
        public Type? TypeField;
    }

    static AllFieldKinds CreateFull() => new()
    {
        Str = "s", Bytes = [1, 2], Bool = true, Int8 = -8, Int16 = -16, Int32 = -32, Int64 = -64, UInt8 = 8,
        UInt16 = 16, UInt32 = 32, UInt64 = 64, DateTime = new(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        Guid = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"), Float16 = (Half)1.5, Float32 = 2.5f,
        Float64 = 3.5, NullableFloat64 = 4.5, Enum = SmallEnum.B, ValueTuple = (1, 2), Tuple = new(3, "t"),
        IntArray = [1, 2, 3], IntList = [4, 5], IntSet = [6], Dict = new() { { 7, "seven" } },
        Nested = new() { Value = 9 }, Marker = new Nested { Value = 10 }, Obj = "obj", TypeField = typeof(int)
    };

    static string PersistedName()
    {
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, CreateFull());
        var bon = new Bon(builder.FinishAsMemory());
        Assert.True(bon.PeekClass(out var name));
        return Encoding.UTF8.GetString(name);
    }

    static readonly string[] FieldNames =
    [
        nameof(AllFieldKinds.Str), nameof(AllFieldKinds.Bytes), nameof(AllFieldKinds.Bool), nameof(AllFieldKinds.Int8),
        nameof(AllFieldKinds.Int16), nameof(AllFieldKinds.Int32), nameof(AllFieldKinds.Int64),
        nameof(AllFieldKinds.UInt8), nameof(AllFieldKinds.UInt16), nameof(AllFieldKinds.UInt32),
        nameof(AllFieldKinds.UInt64), nameof(AllFieldKinds.DateTime), nameof(AllFieldKinds.Guid),
        nameof(AllFieldKinds.Float16), nameof(AllFieldKinds.Float32), nameof(AllFieldKinds.Float64),
        nameof(AllFieldKinds.NullableFloat64), nameof(AllFieldKinds.Enum), nameof(AllFieldKinds.ValueTuple),
        nameof(AllFieldKinds.Tuple), nameof(AllFieldKinds.IntArray), nameof(AllFieldKinds.IntList),
        nameof(AllFieldKinds.IntSet), nameof(AllFieldKinds.Dict), nameof(AllFieldKinds.Nested),
        nameof(AllFieldKinds.Marker), nameof(AllFieldKinds.Obj), nameof(AllFieldKinds.TypeField)
    ];

    [Fact]
    public void AllFieldKindsRoundTrip()
    {
        var original = CreateFull();
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, original);
        var bon = new Bon(builder.FinishAsMemory());
        var res = Assert.IsType<AllFieldKinds>(BonSerializerFactory.Deserialize(ref bon));
        Assert.Equal(original.Str, res.Str);
        Assert.Equal(original.Bytes, res.Bytes);
        Assert.Equal(original.Bool, res.Bool);
        Assert.Equal(original.Int8, res.Int8);
        Assert.Equal(original.Int16, res.Int16);
        Assert.Equal(original.Int32, res.Int32);
        Assert.Equal(original.Int64, res.Int64);
        Assert.Equal(original.UInt8, res.UInt8);
        Assert.Equal(original.UInt16, res.UInt16);
        Assert.Equal(original.UInt32, res.UInt32);
        Assert.Equal(original.UInt64, res.UInt64);
        Assert.Equal(original.DateTime, res.DateTime);
        Assert.Equal(original.Guid, res.Guid);
        Assert.Equal(original.Float16, res.Float16);
        Assert.Equal(original.Float32, res.Float32);
        Assert.Equal(original.Float64, res.Float64);
        Assert.Equal(original.NullableFloat64, res.NullableFloat64);
        Assert.Equal(original.Enum, res.Enum);
        Assert.Equal(original.ValueTuple, res.ValueTuple);
        Assert.Equal(original.Tuple, res.Tuple);
        Assert.Equal(original.IntArray, res.IntArray);
        Assert.Equal(original.IntList, res.IntList);
        Assert.Equal(original.IntSet, res.IntSet);
        Assert.Equal(original.Dict, res.Dict);
        Assert.Equal(9, res.Nested!.Value);
        Assert.Equal(10, Assert.IsType<Nested>(res.Marker).Value);
        Assert.Equal("obj", res.Obj);
        Assert.Equal(typeof(int), res.TypeField);
    }

    static AllFieldKinds DeserializeCrafted(Action<string, BonBuilderWrapper> writeValue)
    {
        var builder = new BonBuilderWrapper();
        builder.Builder.StartClass(PersistedName());
        foreach (var field in FieldNames)
        {
            builder.Builder.WriteKey(field);
            writeValue(field, builder);
        }

        builder.Builder.WriteKey("UnknownField");
        builder.Builder.Write(42L);
        builder.Builder.FinishClass();
        var bon = new Bon(builder.Builder.FinishAsMemory());
        return Assert.IsType<AllFieldKinds>(BonSerializerFactory.Deserialize(ref bon));
    }

    public sealed class BonBuilderWrapper
    {
        public BonBuilder Builder = new();
    }

    static void AssertAllDefault(AllFieldKinds res, bool nullableEnumAndObjects)
    {
        Assert.Null(res.Str);
        Assert.Null(res.Bytes);
        Assert.False(res.Bool);
        Assert.Equal(0, res.Int8);
        Assert.Equal(0, res.Int16);
        Assert.Equal(0, res.Int32);
        Assert.Equal(0, res.Int64);
        Assert.Equal(0, res.UInt8);
        Assert.Equal(0, res.UInt16);
        Assert.Equal(0u, res.UInt32);
        Assert.Equal(0ul, res.UInt64);
        Assert.Equal(default, res.DateTime);
        Assert.Equal(Guid.Empty, res.Guid);
        Assert.Equal(default, res.Float16);
        Assert.Equal(0f, res.Float32);
        Assert.Equal(0d, res.Float64);
        Assert.Null(res.NullableFloat64);
        Assert.Equal(default, res.Enum);
        Assert.Equal(default, res.ValueTuple);
        Assert.Null(res.Tuple);
        Assert.Null(res.IntArray);
        Assert.Null(res.IntList);
        Assert.Null(res.IntSet);
        Assert.Null(res.Dict);
        Assert.Null(res.Nested);
        Assert.Null(res.Marker);
        if (nullableEnumAndObjects) Assert.Null(res.Obj);
        Assert.Null(res.TypeField);
    }

    [Fact]
    public void FieldsWithMismatchedBonTypesAreLeftDefault()
    {
        var res = DeserializeCrafted((field, b) =>
        {
            switch (field)
            {
                case nameof(AllFieldKinds.Str):
                case nameof(AllFieldKinds.Obj):
                    b.Builder.Write(new byte[] { 1 });
                    break;
                case nameof(AllFieldKinds.Marker):
                    // valid class, but not implementing interface
                    b.Builder.StartArray();
                    b.Builder.FinishArray();
                    break;
                default:
                    b.Builder.Write("mismatch");
                    break;
            }
        });
        AssertAllDefault(res, false);
        Assert.Equal(new byte[] { 1 }, res.Obj);
    }

    [Fact]
    public void FieldsWithNullBonAreDefault()
    {
        var res = DeserializeCrafted((_, b) => b.Builder.WriteNull());
        AssertAllDefault(res, true);
    }

    [Fact]
    public void TuplesWithTooFewItemsAreNotDeserialized()
    {
        var res = DeserializeCrafted((field, b) =>
        {
            if (field is nameof(AllFieldKinds.ValueTuple) or nameof(AllFieldKinds.Tuple))
            {
                b.Builder.StartTuple();
                b.Builder.Write(1L);
                b.Builder.FinishTuple();
            }
            else if (field is nameof(AllFieldKinds.NullableFloat64))
            {
                b.Builder.Write("not a number");
            }
            else
            {
                b.Builder.WriteUndefined();
            }
        });
        Assert.Equal(default, res.ValueTuple);
        Assert.Null(res.Tuple);
        Assert.Null(res.NullableFloat64);
    }

    [Fact]
    public void TuplesWithMismatchedItemsAreNotDeserialized()
    {
        var res = DeserializeCrafted((field, b) =>
        {
            if (field is nameof(AllFieldKinds.ValueTuple) or nameof(AllFieldKinds.Tuple))
            {
                b.Builder.StartTuple();
                b.Builder.Write("bad");
                b.Builder.Write("bad");
                b.Builder.FinishTuple();
            }
            else
            {
                b.Builder.WriteNull();
            }
        });
        Assert.Equal(default, res.ValueTuple);
        Assert.Null(res.Tuple);
    }

    [Fact]
    public void UntypedBonIsDeserializedIntoPlainObjects()
    {
        var builder = new BonBuilder();
        builder.StartArray();
        builder.StartDictionary();
        builder.Write("k");
        builder.Write(1.5);
        builder.WriteNull();
        builder.Write("ignored because of null key");
        builder.FinishDictionary();
        builder.StartObject();
        builder.WriteKey("b");
        builder.Write(true);
        builder.WriteKey("d");
        builder.Write(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        builder.WriteKey("g");
        builder.Write(Guid.Empty);
        builder.WriteKey("bytes");
        builder.Write(new byte[] { 1, 2 });
        builder.WriteKey("neg");
        builder.Write(-5L);
        builder.WriteKey("pos");
        builder.Write(5L);
        builder.WriteKey("undef");
        builder.WriteUndefined();
        builder.FinishObject();
        builder.StartTuple();
        builder.Write("t");
        builder.FinishTuple();
        builder.StartClass("Some.Unknown.Class.Name");
        builder.WriteKey("x");
        builder.Write(1L);
        builder.FinishClass();
        builder.FinishArray();
        var bon = new Bon(builder.FinishAsMemory());
        var res = Assert.IsType<object?[]>(BonSerializerFactory.Deserialize(ref bon));
        var dict = Assert.IsType<Dictionary<object?, object?>>(res[0]);
        Assert.Single(dict);
        Assert.Equal(1.5, dict["k"]);
        var obj = Assert.IsType<Dictionary<string, object?>>(res[1]);
        Assert.Equal(true, obj["b"]);
        Assert.Equal(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc), obj["d"]);
        Assert.Equal(Guid.Empty, obj["g"]);
        Assert.Equal(new byte[] { 1, 2 }, obj["bytes"]);
        Assert.Equal(-5L, obj["neg"]);
        Assert.Equal(5ul, obj["pos"]);
        Assert.Null(obj["undef"]);
        var tuple = Assert.IsType<Tuple<object?>>(res[2]);
        Assert.Equal("t", tuple.Item1);
        Assert.Null(res[3]);
    }

    [Fact]
    public void ErrorBonCannotBeDeserialized()
    {
        var bon = new Bon(new byte[] { 0, 1 });
        Assert.Throws<System.IO.InvalidDataException>(() => Deserialize(bon));
    }

    static object? Deserialize(Bon bon) => BonSerializerFactory.Deserialize(ref bon);

    [Fact]
    public void TuplesOfAllSupportedArityDeserializeUntyped()
    {
        for (var count = 1; count <= 8; count++)
        {
            var builder = new BonBuilder();
            builder.StartTuple();
            for (var i = 0; i < count; i++) builder.Write((long)i);
            builder.FinishTuple();
            var bon = new Bon(builder.FinishAsMemory());
            if (count == 8)
            {
                Assert.Throws<NotSupportedException>(() => Deserialize(bon));
                continue;
            }

            var res = Deserialize(bon)!;
            Assert.Equal(count, res.GetType().GetGenericArguments().Length);
        }
    }

    public class NotGenerated
    {
        public int X;
    }

    [Fact]
    public void SerializingUnsupportedTypeThrows()
    {
        var builder = new BonBuilder();
        Assert.Throws<NotSupportedException>(() => Serialize(builder, new NotGenerated()));
    }

    public interface IHasName
    {
        string Name { get; set; }
    }

    [Generate]
    public class ExplicitName : IHasName
    {
        string IHasName.Name { get; set; } = "";
        public int Count { get; set; }
    }

    // Before the fix the generated metadata accessed a backing field named after the interface property, which the
    // explicit implementation does not have, so loading the assembly failed with MissingFieldException.
    [Fact]
    public void ExplicitlyImplementedPropertyRoundTrips()
    {
        var value = new ExplicitName { Count = 3 };
        ((IHasName)value).Name = "x";
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, value);
        var bon = new Bon(builder.FinishAsMemory());
        var read = (ExplicitName)BonSerializerFactory.Deserialize(ref bon)!;
        Assert.Equal(3, read.Count);
        Assert.Equal("x", ((IHasName)read).Name);
    }

    static void Serialize(BonBuilder builder, object value) => BonSerializerFactory.Serialize(ref builder, value);
}
