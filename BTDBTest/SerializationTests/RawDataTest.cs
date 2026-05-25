using System;
using System.Runtime.CompilerServices;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class RawDataTest
{
    [Fact]
    public void ValueTupleOffsetsReadExpectedValues()
    {
        var tuple3 = (Item1: 11, Item2: "two", Item3: 333L);
        var offsets3 = RawData.GetOffsets(typeof(int), typeof(string), typeof(long));
        AssertOffsets(offsets3,
            Offset(ref tuple3, ref tuple3.Item1),
            Offset(ref tuple3, ref tuple3.Item2),
            Offset(ref tuple3, ref tuple3.Item3));
        ref var tuple3Start = ref Bytes(ref tuple3);
        Assert.Equal(11, Read<int>(ref tuple3Start, offsets3[0]));
        Assert.Same("two", Read<string>(ref tuple3Start, offsets3[1]));
        Assert.Equal(333L, Read<long>(ref tuple3Start, offsets3[2]));

        var tuple4 = (Item1: (byte)1, Item2: 22L, Item3: "three", Item4: 4444);
        var offsets4 = RawData.GetOffsets(typeof(byte), typeof(long), typeof(string), typeof(int));
        AssertOffsets(offsets4,
            Offset(ref tuple4, ref tuple4.Item1),
            Offset(ref tuple4, ref tuple4.Item2),
            Offset(ref tuple4, ref tuple4.Item3),
            Offset(ref tuple4, ref tuple4.Item4));
        ref var tuple4Start = ref Bytes(ref tuple4);
        Assert.Equal((byte)1, Read<byte>(ref tuple4Start, offsets4[0]));
        Assert.Equal(22L, Read<long>(ref tuple4Start, offsets4[1]));
        Assert.Same("three", Read<string>(ref tuple4Start, offsets4[2]));
        Assert.Equal(4444, Read<int>(ref tuple4Start, offsets4[3]));

        var tuple5 = (Item1: "one", Item2: (byte)2, Item3: 3.5, Item4: 4, Item5: (short)5);
        var offsets5 = RawData.GetOffsets(typeof(string), typeof(byte), typeof(double), typeof(int), typeof(short));
        AssertOffsets(offsets5,
            Offset(ref tuple5, ref tuple5.Item1),
            Offset(ref tuple5, ref tuple5.Item2),
            Offset(ref tuple5, ref tuple5.Item3),
            Offset(ref tuple5, ref tuple5.Item4),
            Offset(ref tuple5, ref tuple5.Item5));
        ref var tuple5Start = ref Bytes(ref tuple5);
        Assert.Same("one", Read<string>(ref tuple5Start, offsets5[0]));
        Assert.Equal((byte)2, Read<byte>(ref tuple5Start, offsets5[1]));
        Assert.Equal(3.5, Read<double>(ref tuple5Start, offsets5[2]));
        Assert.Equal(4, Read<int>(ref tuple5Start, offsets5[3]));
        Assert.Equal((short)5, Read<short>(ref tuple5Start, offsets5[4]));
    }

    [Fact]
    public void TupleOffsetsReadExpectedValues()
    {
        var tuple3 = new Tuple<int, string, long>(11, "two", 333L);
        var offsets3 = AddObjectDataOffset(RawData.GetOffsets(typeof(int), typeof(string), typeof(long)));
        Assert.Equal(11, Read<int>(ref RawData.Ref(tuple3), offsets3[0]));
        Assert.Same("two", Read<string>(ref RawData.Ref(tuple3), offsets3[1]));
        Assert.Equal(333L, Read<long>(ref RawData.Ref(tuple3), offsets3[2]));

        var tuple4 = new Tuple<byte, long, string, int>(1, 22L, "three", 4444);
        var offsets4 = AddObjectDataOffset(RawData.GetOffsets(typeof(byte), typeof(long), typeof(string), typeof(int)));
        Assert.Equal((byte)1, Read<byte>(ref RawData.Ref(tuple4), offsets4[0]));
        Assert.Equal(22L, Read<long>(ref RawData.Ref(tuple4), offsets4[1]));
        Assert.Same("three", Read<string>(ref RawData.Ref(tuple4), offsets4[2]));
        Assert.Equal(4444, Read<int>(ref RawData.Ref(tuple4), offsets4[3]));

        var tuple5 = new Tuple<string, byte, double, int, short>("one", 2, 3.5, 4, 5);
        var offsets5 = AddObjectDataOffset(
            RawData.GetOffsets(typeof(string), typeof(byte), typeof(double), typeof(int), typeof(short)));
        Assert.Same("one", Read<string>(ref RawData.Ref(tuple5), offsets5[0]));
        Assert.Equal((byte)2, Read<byte>(ref RawData.Ref(tuple5), offsets5[1]));
        Assert.Equal(3.5, Read<double>(ref RawData.Ref(tuple5), offsets5[2]));
        Assert.Equal(4, Read<int>(ref RawData.Ref(tuple5), offsets5[3]));
        Assert.Equal((short)5, Read<short>(ref RawData.Ref(tuple5), offsets5[4]));
    }

    [Fact]
    public void PairOffsetOverloadMatchesArrayOverload()
    {
        var pair = RawData.GetOffsets(typeof(byte), typeof(string));
        Assert.Equal([pair.Item1, pair.Item2], RawData.GetOffsets([typeof(byte), typeof(string)]));
    }

    static uint[] AddObjectDataOffset(uint[] offsets)
    {
        for (var i = 0; i < offsets.Length; i++)
        {
            offsets[i] += (uint)Unsafe.SizeOf<nint>();
        }

        return offsets;
    }

    static void AssertOffsets(uint[] actual, params uint[] expected)
    {
        Assert.Equal(expected, actual);
    }

    static ref byte Bytes<T>(ref T value)
    {
        return ref Unsafe.As<T, byte>(ref value);
    }

    static uint Offset<TTuple, TField>(ref TTuple tuple, ref TField field)
    {
        return (uint)Unsafe.ByteOffset(ref Bytes(ref tuple), ref Unsafe.As<TField, byte>(ref field));
    }

    static T Read<T>(ref byte start, uint offset)
    {
        return Unsafe.As<byte, T>(ref Unsafe.AddByteOffset(ref start, offset));
    }
}
