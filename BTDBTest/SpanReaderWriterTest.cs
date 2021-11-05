using System;
using System.Linq;
using System.Net;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class SpanReaderWriterTest
{
    delegate void SpanReaderAction(ref SpanReader reader);
    delegate void SpanWriterAction(ref SpanWriter writer);

    static void TestWriteRead(SpanWriterAction writeAction, byte[] checkResult,
        SpanReaderAction readAction, SpanReaderAction? skipAction)
    {
        Span<byte> buf = stackalloc byte[1];
        var sw = new SpanWriter(buf);
        writeAction(ref sw);
        Assert.Equal(checkResult, sw.GetSpan().ToArray());
        SpanReader sr;
        if (checkResult.Length > 1)
        {
            sw = new SpanWriter(new Span<byte>());
            writeAction(ref sw);
            Assert.Equal(checkResult, sw.GetSpanAndReset().ToArray());
            writeAction(ref sw);
            Assert.Equal(checkResult, sw.GetSpan().ToArray());
        }
        sr = new SpanReader(checkResult);
        readAction(ref sr);
        Assert.True(sr.Eof);
        sw = new SpanWriter();
        writeAction(ref sw);
        writeAction(ref sw);
        Assert.Equal(checkResult.Concat(checkResult).ToArray(), sw.GetByteBufferAndReset().ToByteArray());
        sr = new SpanReader(checkResult.Concat(checkResult).ToArray());
        readAction(ref sr);
        readAction(ref sr);
        if (skipAction != null)
        {
            sr = new SpanReader(checkResult.Concat(checkResult).ToArray());
            skipAction(ref sr);
            readAction(ref sr);
            Assert.True(sr.Eof);
            sr = new SpanReader(checkResult.Concat(checkResult).ToArray());
            readAction(ref sr);
            skipAction(ref sr);
            Assert.True(sr.Eof);
        }
    }

    [Fact]
    public void DateTimeTest()
    {
        var d = new DateTime(1976, 2, 2);
        TestDateTime(d, new byte[] { 0x08, 0xa6, 0x52, 0xcd, 0x43, 0xff, 0xc0, 0x00 });
        d = new DateTime(d.Ticks, DateTimeKind.Utc);
        TestDateTime(d, new byte[] { 0x48, 0xa6, 0x52, 0xcd, 0x43, 0xff, 0xc0, 0x00 });
    }

    static void TestDateTime(DateTime value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteDateTime(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadDateTime()), (ref SpanReader s) => s.SkipDateTime());
    }

    [Fact]
    public void DateTimeOffsetTest()
    {
        var date = new DateTime(1976, 2, 2, 2, 2, 2, DateTimeKind.Utc);
        TestDateOffsetTime(new DateTimeOffset(date), new byte[] { 0xff, 0x8, 0xa6, 0x52, 0xde, 0x50, 0x40, 0x49, 0x0, 0x80 });
        var dateWithOffset = new DateTimeOffset(date).ToOffset(TimeSpan.FromHours(4));
        TestDateOffsetTime(dateWithOffset, new byte[] { 0xff, 0x8, 0xa6, 0x52, 0xff, 0xd7, 0x51, 0xe9, 0x0, 0xfc, 0x21, 0x87, 0x11, 0xa0, 0x0 });
    }

    static void TestDateOffsetTime(DateTimeOffset value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteDateTimeOffset(value), checkResult, (ref SpanReader r) =>
        {
            var readValue = r.ReadDateTimeOffset();
            Assert.Equal(value, readValue);
            Assert.Equal(value.Offset, readValue.Offset);
        }, (ref SpanReader s) => s.SkipDateTimeOffset());
    }

    [Fact]
    public void TimeSpanTest()
    {
        TestTimeSpan(new TimeSpan(1), new byte[] { 0x81 });
        TestTimeSpan(new TimeSpan(1, 0, 0), new byte[] { 0xfc, 0x08, 0x61, 0xc4, 0x68, 0x00 });
    }

    static void TestTimeSpan(TimeSpan value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteTimeSpan(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadTimeSpan()), (ref SpanReader s) => s.SkipTimeSpan());
    }

    [Fact]
    public void StringTest()
    {
        TestString(null, new byte[] { 0 });
        TestString("", new byte[] { 1 });
        TestString("A", new byte[] { 2, 0x41 });
        TestString("β", new byte[] { 2, 0x83, 0xB2 });
        TestString("A" + (Char)0xD800 + (Char)0xDC01 + "B" + (Char)0xD812, new byte[] { 6, 0x41, 0xC1, 0, 1, 0x42, 0xC0, 0xD8, 0x12 });
    }

    static void TestString(string value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteString(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadString()), (ref SpanReader s) => s.SkipString());
    }

    [Fact]
    public void StringOrderedTest()
    {
        TestStringOrdered(null, new byte[] { 0xd1, 0x0, 0x1 });
        TestStringOrdered("", new byte[] { 0 });
        TestStringOrdered("A", new byte[] { 0x42, 0x0 });
        TestStringOrdered("β", new byte[] { 0x83, 0xB3, 0x0 });
        TestStringOrdered("A" + (Char)0xD800 + (Char)0xDC01 + "B" + (Char)0xD812, new byte[] { 0x42, 0xC1, 0, 2, 0x43, 0xC0, 0xD8, 0x13, 0x0 });
    }

    static void TestStringOrdered(string value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteStringOrdered(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadStringOrdered()), (ref SpanReader s) => s.SkipStringOrdered());
    }

    [Fact]
    public void UInt8Test()
    {
        TestWriteRead((ref SpanWriter w) => w.WriteUInt8(42), new byte[] { 42 }, (ref SpanReader r) => Assert.Equal(42, r.ReadUInt8()), (ref SpanReader s) => s.SkipUInt8());
    }

    [Fact]
    public void Int8Test()
    {
        TestWriteRead((ref SpanWriter w) => w.WriteInt8(-42), new byte[] { 0xD6 }, (ref SpanReader r) => Assert.Equal(-42, r.ReadInt8()), (ref SpanReader s) => s.SkipInt8());
    }

    [Fact]
    public void GuidTest()
    {
        TestWriteRead((ref SpanWriter w) => w.WriteGuid(Guid.Empty), Guid.Empty.ToByteArray(), (ref SpanReader r) => Assert.Equal(Guid.Empty, r.ReadGuid()), (ref SpanReader s) => s.SkipGuid());
        var g = Guid.NewGuid();
        TestWriteRead((ref SpanWriter w) => w.WriteGuid(g), g.ToByteArray(), (ref SpanReader r) => Assert.Equal(g, r.ReadGuid()), (ref SpanReader s) => s.SkipGuid());
    }

    [Fact]
    public void BlockTest()
    {
        var b = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        TestWriteRead((ref SpanWriter w) => w.WriteBlock(b), b, (ref SpanReader r) =>
        {
            var b2 = new byte[b.Length];
            r.ReadBlock(b2);
            Assert.Equal(b, b2);
        }, (ref SpanReader s) => s.SkipBlock(b.Length));
        var bExpect = new byte[] { 2, 3, 4, 5, 6 };
        var b2Expect = new byte[] { 0, 2, 3, 4, 5, 6, 0 };
        TestWriteRead((ref SpanWriter w) => w.WriteBlock(b, 1, 5), bExpect, (ref SpanReader r) =>
        {
            var b2 = new byte[b.Length];
            r.ReadBlock(b2, 1, 5);
            Assert.Equal(b2Expect, b2);
        }, (ref SpanReader s) => s.SkipBlock(5u));
    }

    [Fact]
    public void Int64Test()
    {
        TestWriteRead((ref SpanWriter w) => w.WriteInt64(0x1234567890ABCDEFL), new byte[] { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF }, (ref SpanReader r) => Assert.Equal(0x1234567890ABCDEFL, r.ReadInt64()), (ref SpanReader s) => s.SkipInt64());
    }

    [Fact]
    public void Int32Test()
    {
        TestWriteRead((ref SpanWriter w) => w.WriteInt32(0x12345678), new byte[] { 0x12, 0x34, 0x56, 0x78 }, (ref SpanReader r) => Assert.Equal(0x12345678, r.ReadInt32()), (ref SpanReader s) => s.SkipInt32());
    }

    [Fact]
    public void VUInt32Test()
    {
        TestVUInt32(0, new byte[] { 0 });
        TestVUInt32(1, new byte[] { 1 });
        TestVUInt32(127, new byte[] { 127 });
        TestVUInt32(128, new byte[] { 128, 128 });
        TestVUInt32(0x12345678, new byte[] { 0xf0, 0x12, 0x34, 0x56, 0x78 });
    }

    static void TestVUInt32(uint value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteVUInt32(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadVUInt32()), (ref SpanReader s) => s.SkipVUInt32());
    }

    [Fact]
    public void VUInt64Test()
    {
        TestVUInt64(0, new byte[] { 0 });
        TestVUInt64(1, new byte[] { 1 });
        TestVUInt64(127, new byte[] { 127 });
        TestVUInt64(128, new byte[] { 128, 128 });
        TestVUInt64(0x1234567890ABCDEFUL, new byte[] { 255, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF });
    }

    static void TestVUInt64(ulong value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteVUInt64(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadVUInt64()), (ref SpanReader s) => s.SkipVUInt64());
    }

    [Fact]
    public void VInt32Test()
    {
        TestVInt32(0, new byte[] { 0x80 });
        TestVInt32(1, new byte[] { 0x81 });
        TestVInt32(-1, new byte[] { 0x7F });
        TestVInt32(int.MinValue, new byte[] { 0x07, 0x80, 0x00, 0x00, 0x00 });
        TestVInt32(int.MaxValue, new byte[] { 0xF8, 0x7F, 0xFF, 0xFF, 0xFF });
    }

    static void TestVInt32(int value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteVInt32(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadVInt32()), (ref SpanReader s) => s.SkipVInt64());
    }

    [Fact]
    public void VInt64Test()
    {
        TestVInt64(0, new byte[] { 0x80 });
        TestVInt64(1, new byte[] { 0x81 });
        TestVInt64(-1, new byte[] { 0x7F });
        TestVInt64(int.MinValue, new byte[] { 0x07, 0x80, 0x00, 0x00, 0x00 });
        TestVInt64(int.MaxValue, new byte[] { 0xF8, 0x7F, 0xFF, 0xFF, 0xFF });
        TestVInt64(long.MinValue, new byte[] { 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
        TestVInt64(long.MaxValue, new byte[] { 0xFF, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });
    }

    static void TestVInt64(long value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteVInt64(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadVInt64()), (ref SpanReader s) => s.SkipVInt64());
    }

    [Fact]
    public void DecimalTest()
    {
        TestDecimal(0M, new byte[] { 0 });
        TestDecimal(1M, new byte[] { 32, 1 });
        TestDecimal(-1M, new byte[] { 160, 1 });
        TestDecimal(0.0002M, new byte[] { 32 + 4, 2 });
        TestDecimal(1000000000000M, new byte[] { 32, 248, 232, 212, 165, 16, 0 });
        TestDecimal(1000000000000000000000M, new byte[] { 64, 54, 53, 201, 173, 197, 222, 160, 0, 0 });
        TestDecimal(decimal.MaxValue - 1, new byte[] { 96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE });
        TestDecimal(decimal.MaxValue, new byte[] { 96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });
        TestDecimal(decimal.MinValue + 1, new byte[] { 128 + 96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE });
        TestDecimal(decimal.MinValue, new byte[] { 128 + 96, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });
    }

    static void TestDecimal(decimal value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteDecimal(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadDecimal()), (ref SpanReader s) => s.SkipDecimal());
    }

    [Fact]
    public void SingleTest()
    {
        TestSingle(0, new byte[] { 0, 0, 0, 0 });
        TestSingle(1, new byte[] { 63, 128, 0, 0 });
        TestSingle(float.MinValue, new byte[] { 255, 127, 255, 255 });
        TestSingle(float.MaxValue, new byte[] { 127, 127, 255, 255 });
        TestSingle(float.NaN, new byte[] { 255, 192, 0, 0 });
    }

    static void TestSingle(float value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteSingle(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadSingle()), (ref SpanReader s) => s.SkipSingle());
    }

    [Fact]
    public void DoubleTest()
    {
        TestDouble(0, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
        TestDouble(1, new byte[] { 63, 240, 0, 0, 0, 0, 0, 0 });
        TestDouble(double.MinValue, new byte[] { 255, 239, 255, 255, 255, 255, 255, 255 });
        TestDouble(double.MaxValue, new byte[] { 127, 239, 255, 255, 255, 255, 255, 255 });
        TestDouble(double.NaN, new byte[] { 255, 248, 0, 0, 0, 0, 0, 0 });
    }

    static void TestDouble(double value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteDouble(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadDouble()), (ref SpanReader s) => s.SkipDouble());
    }

    [Fact]
    public void IPAddressTest()
    {
        TestIPAddress(IPAddress.Loopback, new byte[] { 0, 127, 0, 0, 1 });
        TestIPAddress(IPAddress.Broadcast, new byte[] { 0, 255, 255, 255, 255 });
        TestIPAddress(IPAddress.IPv6Loopback, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
        TestIPAddress(IPAddress.IPv6Any, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
        TestIPAddress(null, new byte[] { 3 });
        var ip = new IPAddress(IPAddress.IPv6Loopback.GetAddressBytes(), 1);
        TestIPAddress(ip, new byte[] { 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1 });
    }

    static void TestIPAddress(IPAddress? value, byte[] checkResult)
    {
        TestWriteRead((ref SpanWriter w) => w.WriteIPAddress(value), checkResult, (ref SpanReader r) => Assert.Equal(value, r.ReadIPAddress()), (ref SpanReader s) => s.SkipIPAddress());
    }
}
