using System.Collections.Generic;
using BTDB.Buffer;
using Xunit;

namespace BTDBTest;

public class PackUnpackTest
{
    [Fact]
    public void PackVUIntIsOrderable()
    {
        foreach (var ul in GenerateULongs())
            PackVUIntIsOrderableCore(ul);
    }

    void PackVUIntIsOrderableCore(ulong t)
    {
        var buf1 = new byte[9];
        var o1 = 0;
        PackUnpack.PackVUInt(buf1, ref o1, t - 1);
        var buf2 = new byte[9];
        var o2 = 0;
        PackUnpack.PackVUInt(buf2, ref o2, t);
        if (t <= uint.MaxValue)
        {
            Assert.Equal((uint)o2, PackUnpack.LengthVUInt((uint)t));
        }

        Assert.Equal((uint)o2, PackUnpack.LengthVUInt(t));
        Assert.Equal((uint)o2, PackUnpack.LengthVUInt(buf2, 0));
        Assert.True(0 > BitArrayManipulation.CompareByteArray(buf1, o1, buf2, o2));
        var o1A = 0;
        Assert.Equal(t - 1, PackUnpack.UnpackVUInt(buf1, ref o1A));
        Assert.Equal(o1, o1A);
        var o2A = 0;
        Assert.Equal(t, PackUnpack.UnpackVUInt(buf2, ref o2A));
        Assert.Equal(o2, o2A);
    }

    static IEnumerable<ulong> GenerateULongs()
    {
        yield return 1;
        yield return 2;
        for (ulong t = 4; t != 0; t *= 2)
        {
            yield return t - 1;
            yield return t;
            yield return t + 1;
        }

        yield return ulong.MaxValue;
        yield return 123456789UL;
        yield return 123456789123456789UL;
    }

    [Fact]
    public void PackVIntIsOrderableForPositive()
    {
        foreach (var l in GeneratePositiveLongs())
            PackVIntIsOrderableForPositiveCore(l);
    }

    void PackVIntIsOrderableForPositiveCore(long t)
    {
        var buf1 = new byte[9];
        var o1 = 0;
        PackUnpack.PackVInt(buf1, ref o1, t - 1);
        var buf2 = new byte[9];
        var o2 = 0;
        PackUnpack.PackVInt(buf2, ref o2, t);
        if (t is >= int.MinValue and <= int.MaxValue)
        {
            Assert.Equal((uint)o2, PackUnpack.LengthVInt((int)t));
        }

        Assert.Equal((uint)o2, PackUnpack.LengthVInt(t));
        Assert.Equal((uint)o2, PackUnpack.LengthVInt(buf2, 0));
        Assert.True(0 > BitArrayManipulation.CompareByteArray(buf1, o1, buf2, o2));
        var o1A = 0;
        Assert.Equal(t - 1, PackUnpack.UnpackVInt(buf1, ref o1A));
        Assert.Equal(o1, o1A);
        var o2A = 0;
        Assert.Equal(t, PackUnpack.UnpackVInt(buf2, ref o2A));
        Assert.Equal(o2, o2A);
    }

    static IEnumerable<long> GeneratePositiveLongs()
    {
        yield return 0;
        yield return 1;
        yield return 2;
        for (long t = 4; t > 0; t *= 2)
        {
            yield return t - 1;
            yield return t;
            yield return t + 1;
        }

        yield return long.MaxValue;
        yield return 123456789L;
        yield return 123456789123456789L;
    }

    [Fact]
    public void PackVIntIsOrderableForNegative()
    {
        foreach (var l in GenerateNegativeLongs())
            PackVIntIsOrderableForNegativeCore(l);
    }

    void PackVIntIsOrderableForNegativeCore(long t)
    {
        var buf1 = new byte[9];
        var o1 = 0;
        PackUnpack.PackVInt(buf1, ref o1, t - 1);
        var buf2 = new byte[9];
        var o2 = 0;
        PackUnpack.PackVInt(buf2, ref o2, t);
        if (t >= int.MinValue && t <= int.MaxValue)
        {
            Assert.Equal((uint)o2, PackUnpack.LengthVInt((int)t));
        }

        Assert.Equal((uint)o2, PackUnpack.LengthVInt(t));
        Assert.Equal((uint)o2, PackUnpack.LengthVInt(buf2, 0));
        Assert.True(0 > BitArrayManipulation.CompareByteArray(buf1, o1, buf2, o2), $"{t - 1} is not before {t}");
        var o1A = 0;
        Assert.Equal(t - 1, PackUnpack.UnpackVInt(buf1, ref o1A));
        Assert.Equal(o1, o1A);
        var o2A = 0;
        Assert.Equal(t, PackUnpack.UnpackVInt(buf2, ref o2A));
        Assert.Equal(o2, o2A);
    }

    static IEnumerable<long> GenerateNegativeLongs()
    {
        yield return -1;
        yield return -2;
        for (long t = -4; t - 1 < 0; t *= 2)
        {
            yield return t - 1;
            yield return t;
            yield return t + 1;
        }

        yield return long.MinValue + 1;
        yield return -123456789L;
        yield return -123456789123456789L;
    }

    [Fact]
    public void DetectLengthOfSimpleCharactersUintWorks()
    {
        Assert.Equal((4u, false), PackUnpack.DetectLengthOfSimpleCharacters(0x12345678u));
        Assert.Equal((3u, false), PackUnpack.DetectLengthOfSimpleCharacters(0x82345678u));
        Assert.Equal((2u, false), PackUnpack.DetectLengthOfSimpleCharacters(0x12845678u));
        Assert.Equal((1u, false), PackUnpack.DetectLengthOfSimpleCharacters(0x12348678u));
        Assert.Equal((0u, false), PackUnpack.DetectLengthOfSimpleCharacters(0x12345688u));
        Assert.Equal((1u, true), PackUnpack.DetectLengthOfSimpleCharacters(0x12345600u));
        Assert.Equal((1u, true), PackUnpack.DetectLengthOfSimpleCharacters(0x12348600u));
        Assert.Equal((2u, true), PackUnpack.DetectLengthOfSimpleCharacters(0x12340078u));
        Assert.Equal((2u, true), PackUnpack.DetectLengthOfSimpleCharacters(0x12840078u));
        Assert.Equal((3u, true), PackUnpack.DetectLengthOfSimpleCharacters(0x12005678u));
        Assert.Equal((3u, true), PackUnpack.DetectLengthOfSimpleCharacters(0x82005678u));
        Assert.Equal((4u, true), PackUnpack.DetectLengthOfSimpleCharacters(0x00345678u));
    }
}
