using System;
using System.Collections.Generic;
using Assent;
using BTDB;
using BTDB.Bon;
using BTDB.Serialization;
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

    [Fact]
    public void SerializeDeserializeAllSupportedTypes()
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
        this.Assent(str);
        bon = new Bon(builder.FinishAsMemory());
        var deserialized = BonSerializerFactory.Deserialize(ref bon);
        builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, deserialized);
        bon = new Bon(builder.FinishAsMemory());
        Assert.Equal(str, bon.DumpToJson());
    }

    [Fact]
    public void SerializeDeserializeBoxedValue()
    {
        var obj = (object)(42u, 424242424242L);
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var bon = new Bon(builder.FinishAsMemory());
        this.Assent(bon.DumpToJson());
        bon = new Bon(builder.FinishAsMemory());
        var deserialized = BonSerializerFactory.Deserialize(ref bon);
        Assert.IsType<Tuple<object, object>>(deserialized);
        var (a, b) = (Tuple<object, object>)deserialized;
        Assert.Equal(42u, (ulong)a);
        Assert.Equal(424242424242L, (long)(ulong)b);
    }
}
