using System;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using Xunit;

namespace BTDB.LSM.Test.Format;

public class LsmEncodingTest
{
    static byte[] Encode(Action<LsmEncoder> write)
    {
        var encoder = new LsmEncoder();
        write(encoder);
        return encoder.WrittenSpan.ToArray();
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(127UL)]
    [InlineData(128UL)]
    [InlineData(int.MaxValue)]
    [InlineData(ulong.MaxValue)]
    public void UnsignedValuesRoundTrip(ulong value)
    {
        var data = Encode(e => e.WriteVUInt64(value));
        Assert.Equal(value, new LsmDecoder(data).ReadVUInt64());
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(int.MinValue)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void SignedValuesRoundTrip(long value)
    {
        var data = Encode(e => e.WriteVInt64(value));
        Assert.Equal(value, new LsmDecoder(data).ReadVInt64());
    }

    // Before the fix a length out of the range of int threw OverflowException instead of reporting damage.
    [Fact]
    public void ValuesOutOfIntRangeAreDamage()
    {
        Assert.Equal(int.MaxValue, new LsmDecoder(Encode(e => e.WriteVUInt64(int.MaxValue))).ReadVUInt32());
        Assert.Throws<BTDBException>(() => new LsmDecoder(Encode(e => e.WriteVUInt64((ulong)int.MaxValue + 1))).ReadVUInt32());
        Assert.Equal(int.MinValue, new LsmDecoder(Encode(e => e.WriteVInt64(int.MinValue))).ReadVInt32());
        Assert.Throws<BTDBException>(() => new LsmDecoder(Encode(e => e.WriteVInt64((long)int.MinValue - 1))).ReadVInt32());
        Assert.Throws<BTDBException>(() => new LsmDecoder(Encode(e => e.WriteVInt64((long)int.MaxValue + 1))).ReadVInt32());
    }

    // Before the fix a tenth byte above 1 lost its high bits instead of being reported.
    [Fact]
    public void OverlongValueIsDamage()
    {
        var data = new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02 };
        Assert.Throws<BTDBException>(() => new LsmDecoder(data).ReadVUInt64());
        var tooLong = new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00 };
        Assert.Throws<BTDBException>(() => new LsmDecoder(tooLong).ReadVUInt64());
    }

    // Before the fix the end of a length near int.MaxValue wrapped negative and passed the check, so the slice threw
    // ArgumentOutOfRangeException instead of reporting damage.
    [Fact]
    public void LengthPastTheEndIsDamageEvenNearIntMaxValue()
    {
        var data = Encode(e =>
        {
            e.WriteVUInt64(1);
            e.WriteVUInt64(int.MaxValue);
        });
        Assert.Throws<BTDBException>(() =>
        {
            var decoder = new LsmDecoder(data);
            decoder.ReadVUInt64();
            decoder.ReadByteArray();
        });
    }
}
