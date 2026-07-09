using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BTDB;
using BTDB.Bon;
using BTDB.Serialization;
using BTDBTest;
using Xunit;

namespace BTDBTest.SerializationTests;

public class BonSerializerTest
{
    [Generate]
    public class AllSupportedTypes
    {
        public string Str;
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
        public (int A, long B) ValueTupleIntLong;
        public (long A, string B) ValueTupleLongString;
        public (uint A, uint B) ValueTupleUintUint;
        public Tuple<long, string> TupleLongString;
        public AllSupportedTypes? Self;
        public double[]? DoubleArray;
        public List<int>? IntList;
        public HashSet<ushort>? UShortSet;
        public Dictionary<long, int>? LongIntDict;
    }

    [Generate]
    public class TupleArityTypes
    {
        public (int A, string B, long C) ValueTuple3;
        public (byte A, long B, string C, int D) ValueTuple4;
        public (string A, byte B, double C, int D, short E) ValueTuple5;
        public (DateTime A, Guid B, string C) ValueTupleDateTimeGuid;
        public Tuple<int, string, long>? Tuple3;
        public Tuple<byte, long, string, int>? Tuple4;
        public Tuple<string, byte, double, int, short>? Tuple5;
        public Tuple<DateTime, Guid, string>? TupleDateTimeGuid;
    }

    [Fact]
    public async Task SerializeDeserializeAllSupportedTypes()
    {
        var obj = new AllSupportedTypes
        {
            Str = "Hello", Bool = true, Int8 = 42, Int16 = 1234, Int32 = 12345678, Int64 = long.MaxValue,
            UInt8 = 42, UInt16 = 1234, UInt32 = 12345678, UInt64 = ulong.MaxValue,
            DateTime = new(2024, 2, 11, 14, 4, 30),
            Guid = Guid.Parse("9e251065-0873-49bc-8fd9-266cc9aa39d3"), Float16 = (Half)3.14, Float32 = 3.14f,
            Float64 = Math.PI, NullableFloat64 = Math.PI, ValueTupleIntLong = (42, 4242424242),
            ValueTupleLongString = (424242424242, "B"), ValueTupleUintUint = (1, 2),
            TupleLongString = new Tuple<long, string>(123456, "BB"),
            Self = new(), DoubleArray = [Math.E, Math.PI], IntList = [1, 20, 300], UShortSet = [666, 12345],
            LongIntDict = new() { { 1111, 2 }, { 3333, 4 } }
        };
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var bon = new Bon(builder.FinishAsMemory());
        var str = bon.DumpToJson();
        await this.VerifyApproval(str);
        bon = new Bon(builder.FinishAsMemory());
        var deserialized = BonSerializerFactory.Deserialize(ref bon);
        builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, deserialized);
        bon = new Bon(builder.FinishAsMemory());
        Assert.Equal(str, bon.DumpToJson());
    }

    [Fact]
    public async Task SerializeDeserializeBoxedValue()
    {
        var obj = (object)(42u, 424242424242L);
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var bon = new Bon(builder.FinishAsMemory());
        await this.VerifyApproval(bon.DumpToJson());
        bon = new Bon(builder.FinishAsMemory());
        var deserialized = BonSerializerFactory.Deserialize(ref bon);
        Assert.IsType<Tuple<object, object>>(deserialized);
        var (a, b) = (Tuple<object, object>)deserialized;
        Assert.Equal(42u, (ulong)a);
        Assert.Equal(424242424242L, (long)(ulong)b);
    }

    [Fact]
    public void SerializeDeserializeTuplesWithMoreThanTwoItems()
    {
        var obj = new TupleArityTypes
        {
            ValueTuple3 = (11, "two", 333L),
            ValueTuple4 = (1, 22L, "three", 4444),
            ValueTuple5 = ("one", 2, 3.5, 4, 5),
            ValueTupleDateTimeGuid = (new(2025, 5, 25, 14, 30, 0),
                Guid.Parse("d1f4fd89-f6cd-427e-a703-61c60ea46f76"), "dt-guid"),
            Tuple3 = new(11, "two", 333L),
            Tuple4 = new(1, 22L, "three", 4444),
            Tuple5 = new("one", 2, 3.5, 4, 5),
            TupleDateTimeGuid = new(new(2026, 1, 2, 3, 4, 5),
                Guid.Parse("364b9a4f-207a-44d1-aebb-3bbffb24e5c8"), "tuple-dt-guid")
        };
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var bon = new Bon(builder.FinishAsMemory());
        var deserialized = Assert.IsType<TupleArityTypes>(BonSerializerFactory.Deserialize(ref bon));

        Assert.Equal(obj.ValueTuple3, deserialized.ValueTuple3);
        Assert.Equal(obj.ValueTuple4, deserialized.ValueTuple4);
        Assert.Equal(obj.ValueTuple5, deserialized.ValueTuple5);
        Assert.Equal(obj.ValueTupleDateTimeGuid, deserialized.ValueTupleDateTimeGuid);
        Assert.Equal(obj.Tuple3, deserialized.Tuple3);
        Assert.Equal(obj.Tuple4, deserialized.Tuple4);
        Assert.Equal(obj.Tuple5, deserialized.Tuple5);
        Assert.Equal(obj.TupleDateTimeGuid, deserialized.TupleDateTimeGuid);
    }

    [Fact]
    public void DeserializeBoxedTupleWithMoreThanTwoItems()
    {
        var obj = (object)(11, "two", 333L, 4.5);
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var bon = new Bon(builder.FinishAsMemory());
        var deserialized =
            Assert.IsType<Tuple<object?, object?, object?, object?>>(BonSerializerFactory.Deserialize(ref bon));

        Assert.Equal(11L, Convert.ToInt64(deserialized.Item1));
        Assert.Equal("two", deserialized.Item2);
        Assert.Equal(333L, Convert.ToInt64(deserialized.Item3));
        Assert.Equal(4.5, deserialized.Item4);
    }

    [Fact]
    public async Task TypeCouldBeSerializedAsWell()
    {
        Type[] obj = [typeof(int), typeof(AllSupportedTypes)];
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var bon = new Bon(builder.FinishAsMemory());
        await this.VerifyApproval(bon.DumpToJson());
        bon = new Bon(builder.FinishAsMemory());
        var deserialized = BonSerializerFactory.Deserialize(ref bon);
        Assert.IsType<object[]>(deserialized);
        var deserializedArray = (object[])deserialized;
        Assert.Equal(obj.Length, deserializedArray.Length);
        for (var i = 0; i < obj.Length; i++)
        {
            Assert.Equal(obj[i], deserializedArray[i]);
        }
    }
}
