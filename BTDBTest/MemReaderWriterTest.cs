using System;
using System.Linq;
using System.Net;
using System.Text;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class MemReaderWriterTest
{
    delegate void MemReaderAction(ref MemReader reader);

    delegate void MemWriterAction(ref MemWriter writer);

    static void TestWriteRead(MemWriterAction writeAction, byte[] checkResult,
        MemReaderAction readAction, MemReaderAction? skipAction)
    {
        Span<byte> buf = stackalloc byte[1];
        var sw = MemWriter.CreateFromStackAllocatedSpan(buf);
        writeAction(ref sw);
        Assert.Equal(checkResult, sw.GetSpan().ToArray());
        if (checkResult.Length > 1)
        {
            sw = new();
            writeAction(ref sw);
            Assert.Equal(checkResult, sw.GetSpanAndReset().ToArray());
            writeAction(ref sw);
            Assert.Equal(checkResult, sw.GetSpan().ToArray());
        }

        MemReader sr = MemReader.CreateFromReadOnlyMemory(checkResult);
        readAction(ref sr);
        Assert.True(sr.Eof);
        sr.Dispose();

        sw = new();
        writeAction(ref sw);
        writeAction(ref sw);
        Assert.Equal(checkResult.Concat(checkResult).ToArray(), sw.GetByteBufferAndReset().ToByteArray());
        sr = MemReader.CreateFromReadOnlyMemory(checkResult.Concat(checkResult).ToArray());
        readAction(ref sr);
        readAction(ref sr);
        sr.Dispose();
        if (skipAction != null)
        {
            sr = MemReader.CreateFromReadOnlyMemory(checkResult.Concat(checkResult).ToArray());
            skipAction(ref sr);
            readAction(ref sr);
            Assert.True(sr.Eof);
            sr.Dispose();
            sr = MemReader.CreateFromReadOnlyMemory(checkResult.Concat(checkResult).ToArray());
            readAction(ref sr);
            skipAction(ref sr);
            Assert.True(sr.Eof);
            sr.Dispose();
        }
    }

    static void TestWriteRead(MemWriterAction writeAction,
        MemReaderAction readAction, MemReaderAction skipAction)
    {
        var sw = new MemWriter();
        writeAction(ref sw);
        MemReader sr = MemReader.CreateFromPinnedSpan(sw.GetSpan());
        readAction(ref sr);
        Assert.True(sr.Eof);
        sr = MemReader.CreateFromPinnedSpan(sw.GetSpan());
        skipAction(ref sr);
        Assert.True(sr.Eof);
        sw = new();
        writeAction(ref sw);
        writeAction(ref sw);
        sr = MemReader.CreateFromPinnedSpan(sw.GetSpan());
        skipAction(ref sr);
        readAction(ref sr);
        Assert.True(sr.Eof);
    }

    [Fact]
    public void DateTimeTest()
    {
        var d = new DateTime(1976, 2, 2);
        TestDateTime(d, [0x08, 0xa6, 0x52, 0xcd, 0x43, 0xff, 0xc0, 0x00]);
        d = new(d.Ticks, DateTimeKind.Utc);
        TestDateTime(d, [0x48, 0xa6, 0x52, 0xcd, 0x43, 0xff, 0xc0, 0x00]);
    }

    static void TestDateTime(DateTime value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteDateTime(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadDateTime()), (ref MemReader s) => s.Skip8Bytes());
    }

    [Fact]
    public void DateTimeOffsetTest()
    {
        var date = new DateTime(1976, 2, 2, 2, 2, 2, DateTimeKind.Utc);
        TestDateOffsetTime(new(date), [0xff, 0x8, 0xa6, 0x52, 0xde, 0x50, 0x40, 0x49, 0x0, 0x80]);
        var dateWithOffset = new DateTimeOffset(date).ToOffset(TimeSpan.FromHours(4));
        TestDateOffsetTime(dateWithOffset,
            [0xff, 0x8, 0xa6, 0x52, 0xff, 0xd7, 0x51, 0xe9, 0x0, 0xfc, 0x21, 0x87, 0x11, 0xa0, 0x0]);
    }

    static void TestDateOffsetTime(DateTimeOffset value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteDateTimeOffset(value), checkResult, (ref MemReader r) =>
        {
            var readValue = r.ReadDateTimeOffset();
            Assert.Equal(value, readValue);
            Assert.Equal(value.Offset, readValue.Offset);
        }, (ref MemReader s) => s.SkipDateTimeOffset());
    }

    [Fact]
    public void TimeSpanTest()
    {
        TestTimeSpan(new(1), [0x81]);
        TestTimeSpan(new(1, 0, 0), [0xfc, 0x08, 0x61, 0xc4, 0x68, 0x00]);
    }

    static void TestTimeSpan(TimeSpan value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteTimeSpan(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadTimeSpan()), (ref MemReader s) => s.SkipVInt64());
    }

    [Fact]
    public void StringTest()
    {
        TestString(null, [0]);
        TestString("", [1]);
        TestString("A", [2, 0x41]);
        TestString("β", [2, 0x83, 0xB2]);
        TestString("A" + (Char)0xD800 + (Char)0xDC01 + "B" + (Char)0xD812,
            [6, 0x41, 0xC1, 0, 1, 0x42, 0xC0, 0xD8, 0x12]);
    }

    static void TestString(string value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteString(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadString()), (ref MemReader s) => s.SkipString());
    }

    [Fact]
    public void StringTest2()
    {
        for (var i = 0; i < 64; i++)
        {
            var sb = new StringBuilder();
            for (var k = 0; k < i; k++)
            {
                sb.Append((char)('A' + k));
            }

            TestString(sb.ToString());
            TestStringOrdered(sb.ToString());

            sb.Append((char)0x80);
            var first = sb.Length;
            for (var j = 0; j < 64; j++)
            {
                sb.Remove(first, sb.Length - first);
                for (var k = 0; k < j; k++)
                {
                    sb.Append((char)(' ' + k));
                }

                TestString(sb.ToString());
                TestStringOrdered(sb.ToString());
            }

            sb.Remove(first - 1, 1);
            sb.Append((char)0x7f);

            for (var j = 0; j < 64; j++)
            {
                sb.Remove(first, sb.Length - first);
                for (var k = 0; k < j; k++)
                {
                    sb.Append((char)('a' + k));
                }

                TestString(sb.ToString());
                TestStringOrdered(sb.ToString());
            }
        }
    }

    static void TestString(string value)
    {
        TestWriteRead((ref MemWriter w) => w.WriteString(value),
            (ref MemReader r) => Assert.Equal(value, r.ReadString()), (ref MemReader s) => s.SkipString());
    }

    static void TestStringOrdered(string value)
    {
        TestWriteRead((ref MemWriter w) => w.WriteStringOrdered(value),
            (ref MemReader r) => Assert.Equal(value, r.ReadStringOrdered()),
            (ref MemReader s) => s.SkipStringOrdered());
    }

    [Fact]
    public void StringOrderedTest()
    {
        TestStringOrdered(null, [0xd1, 0x0, 0x1]);
        TestStringOrdered("", [0]);
        TestStringOrdered("A", [0x42, 0x0]);
        TestStringOrdered("β", [0x83, 0xB3, 0x0]);
        TestStringOrdered("A" + (Char)0xD800 + (Char)0xDC01 + "B" + (Char)0xD812,
            [0x42, 0xC1, 0, 2, 0x43, 0xC0, 0xD8, 0x13, 0x0]);
    }

    static void TestStringOrdered(string value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteStringOrdered(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadStringOrdered()),
            (ref MemReader s) => s.SkipStringOrdered());
    }

    [Fact]
    public void UInt8Test()
    {
        TestWriteRead((ref MemWriter w) => w.WriteUInt8(42), [42],
            (ref MemReader r) => Assert.Equal(42, r.ReadUInt8()), (ref MemReader s) => s.Skip1Byte());
    }

    [Fact]
    public void Int8Test()
    {
        TestWriteRead((ref MemWriter w) => w.WriteInt8(-42), [0xD6],
            (ref MemReader r) => Assert.Equal(-42, r.ReadInt8()), (ref MemReader s) => s.Skip1Byte());
    }

    [Fact]
    public void GuidTest()
    {
        TestWriteRead((ref MemWriter w) => w.WriteGuid(Guid.Empty), Guid.Empty.ToByteArray(),
            (ref MemReader r) => Assert.Equal(Guid.Empty, r.ReadGuid()), (ref MemReader s) => s.SkipGuid());
        var g = Guid.NewGuid();
        TestWriteRead((ref MemWriter w) => w.WriteGuid(g), g.ToByteArray(),
            (ref MemReader r) => Assert.Equal(g, r.ReadGuid()), (ref MemReader s) => s.SkipGuid());
    }

    [Fact]
    public void BlockTest()
    {
        var b = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        TestWriteRead((ref MemWriter w) => w.WriteBlock(b), b, (ref MemReader r) =>
        {
            var b2 = new byte[b.Length];
            r.ReadBlock(b2);
            Assert.Equal(b, b2);
        }, (ref MemReader s) => s.SkipBlock((uint)b.Length));
        var bExpect = new byte[] { 2, 3, 4, 5, 6 };
        var b2Expect = new byte[] { 0, 2, 3, 4, 5, 6, 0 };
        TestWriteRead((ref MemWriter w) => w.WriteBlock(b, 1, 5), bExpect, (ref MemReader r) =>
        {
            var b2 = new byte[b.Length];
            r.ReadBlock(b2.AsSpan(1, 5));
            Assert.Equal(b2Expect, b2);
        }, (ref MemReader s) => s.SkipBlock(5u));
    }

    [Fact]
    public void Int64Test()
    {
        TestWriteRead((ref MemWriter w) => w.WriteInt64BE(0x1234567890ABCDEFL),
            [0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF],
            (ref MemReader r) => Assert.Equal(0x1234567890ABCDEFL, r.ReadInt64BE()),
            (ref MemReader s) => s.Skip8Bytes());
        TestWriteRead((ref MemWriter w) => w.WriteInt64LE(0x1234567890ABCDEFL),
            new byte[] { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF }.Reverse().ToArray(),
            (ref MemReader r) => Assert.Equal(0x1234567890ABCDEFL, r.ReadInt64LE()),
            (ref MemReader s) => s.Skip8Bytes());
    }

    [Fact]
    public void Int32Test()
    {
        TestWriteRead((ref MemWriter w) => w.WriteInt32BE(0x12345678), [0x12, 0x34, 0x56, 0x78],
            (ref MemReader r) => Assert.Equal(0x12345678, r.ReadInt32BE()), (ref MemReader s) => s.Skip4Bytes());
        TestWriteRead((ref MemWriter w) => w.WriteInt32LE(0x12345678), [0x78, 0x56, 0x34, 0x12],
            (ref MemReader r) => Assert.Equal(0x12345678, r.ReadInt32LE()), (ref MemReader s) => s.Skip4Bytes());
    }

    [Fact]
    public void VUInt32Test()
    {
        TestVUInt32(0, [0]);
        TestVUInt32(1, [1]);
        TestVUInt32(127, [127]);
        TestVUInt32(128, [128, 128]);
        TestVUInt32(0x12345678, [0xf0, 0x12, 0x34, 0x56, 0x78]);
    }

    static void TestVUInt32(uint value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteVUInt32(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadVUInt32()), (ref MemReader s) => s.SkipVUInt32());
    }

    [Fact]
    public void VUInt64Test()
    {
        TestVUInt64(0, [0]);
        TestVUInt64(1, [1]);
        TestVUInt64(127, [127]);
        TestVUInt64(128, [128, 128]);
        TestVUInt64(0x1234567890ABCDEFUL, [255, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF]);
    }

    static void TestVUInt64(ulong value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteVUInt64(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadVUInt64()), (ref MemReader s) => s.SkipVUInt64());
    }

    [Fact]
    public void VInt32Test()
    {
        TestVInt32(0, [0x80]);
        TestVInt32(1, [0x81]);
        TestVInt32(-1, [0x7F]);
        TestVInt32(int.MinValue, [0x07, 0x80, 0x00, 0x00, 0x00]);
        TestVInt32(int.MaxValue, [0xF8, 0x7F, 0xFF, 0xFF, 0xFF]);
    }

    static void TestVInt32(int value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteVInt32(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadVInt32()), (ref MemReader s) => s.SkipVInt64());
    }

    [Fact]
    public void VInt64Test()
    {
        TestVInt64(0, [0x80]);
        TestVInt64(1, [0x81]);
        TestVInt64(-1, [0x7F]);
        TestVInt64(int.MinValue, [0x07, 0x80, 0x00, 0x00, 0x00]);
        TestVInt64(int.MaxValue, [0xF8, 0x7F, 0xFF, 0xFF, 0xFF]);
        TestVInt64(long.MinValue, [0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        TestVInt64(long.MaxValue, [0xFF, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    static void TestVInt64(long value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteVInt64(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadVInt64()), (ref MemReader s) => s.SkipVInt64());
    }

    [Fact]
    public void DecimalTest()
    {
        TestDecimal(0M, [0]);
        TestDecimal(1M, [32, 1]);
        TestDecimal(-1M, [160, 1]);
        TestDecimal(0.0002M, [32 + 4, 2]);
        TestDecimal(1000000000000M, [32, 248, 232, 212, 165, 16, 0]);
        TestDecimal(1000000000000000000000M, [64, 54, 53, 201, 173, 197, 222, 160, 0, 0]);
        TestDecimal(decimal.MaxValue - 1,
            [96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]);
        TestDecimal(decimal.MaxValue,
            [96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        TestDecimal(decimal.MinValue + 1,
            [128 + 96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]);
        TestDecimal(decimal.MinValue,
            [128 + 96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    static void TestDecimal(decimal value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteDecimal(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadDecimal()), (ref MemReader s) => s.SkipDecimal());
    }

    [Fact]
    public void SingleTest()
    {
        TestSingle(0, [0, 0, 0, 0]);
        TestSingle(1, [63, 128, 0, 0]);
        TestSingle(float.MinValue, [255, 127, 255, 255]);
        TestSingle(float.MaxValue, [127, 127, 255, 255]);
        TestSingle(float.NaN, [255, 192, 0, 0]);
    }

    static void TestSingle(float value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteSingle(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadSingle()), (ref MemReader s) => s.Skip4Bytes());
    }

    [Fact]
    public void DoubleTest()
    {
        TestDouble(0, [0, 0, 0, 0, 0, 0, 0, 0]);
        TestDouble(1, [63, 240, 0, 0, 0, 0, 0, 0]);
        TestDouble(double.MinValue, [255, 239, 255, 255, 255, 255, 255, 255]);
        TestDouble(double.MaxValue, [127, 239, 255, 255, 255, 255, 255, 255]);
        TestDouble(double.NaN, [255, 248, 0, 0, 0, 0, 0, 0]);
    }

    static void TestDouble(double value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteDouble(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadDouble()), (ref MemReader s) => s.Skip8Bytes());
    }

    [Fact]
    public void DoubleOrderedTest()
    {
        TestDoubleOrdered(0, [128, 0, 0, 0, 0, 0, 0, 0]);
        TestDoubleOrdered(1, [191, 240, 0, 0, 0, 0, 0, 0]);
        TestDoubleOrdered(double.MinValue, [0, 16, 0, 0, 0, 0, 0, 0]);
        TestDoubleOrdered(double.MaxValue, [255, 239, 255, 255, 255, 255, 255, 255]);
        TestDoubleOrdered(double.NaN, [0, 7, 255, 255, 255, 255, 255, 255]);
    }

    static void TestDoubleOrdered(double value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteDoubleOrdered(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadDoubleOrdered()), (ref MemReader s) => s.Skip8Bytes());
    }

    [Fact]
    public void IPAddressTest()
    {
        TestIPAddress(IPAddress.Loopback, [0, 127, 0, 0, 1]);
        TestIPAddress(IPAddress.Broadcast, [0, 255, 255, 255, 255]);
        TestIPAddress(IPAddress.IPv6Loopback, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        TestIPAddress(IPAddress.IPv6Any, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        TestIPAddress(null, [3]);
        var ip = new IPAddress(IPAddress.IPv6Loopback.GetAddressBytes(), 1);
        TestIPAddress(ip, [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1]);
    }

    static void TestIPAddress(IPAddress? value, byte[] checkResult)
    {
        TestWriteRead((ref MemWriter w) => w.WriteIPAddress(value), checkResult,
            (ref MemReader r) => Assert.Equal(value, r.ReadIPAddress()), (ref MemReader s) => s.SkipIPAddress());
    }
}
