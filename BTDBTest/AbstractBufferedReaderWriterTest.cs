﻿using System;
using System.Linq;
using System.Net;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest
{
    public class AbstractBufferedReaderWriterTest
    {
        class BufferedWriterStub : AbstractBufferedWriter
        {
            public BufferedWriterStub(int bufLength)
            {
                _bufLength = bufLength;
            }

            byte[] _output = new byte[0];
            readonly int _bufLength;

            public byte[] Output => _output;

            public override void FlushBuffer()
            {
                if (Pos > 0)
                {
                    var oldLength = _output.Length;
                    Array.Resize(ref _output, oldLength + Pos);
                    Array.Copy(Buf, 0, _output, oldLength, Pos);
                }
                if (Buf == null) Buf = new byte[_bufLength];
                Pos = 0;
                End = Buf.Length;
            }

            public override long GetCurrentPosition()
            {
                throw new AssertionException("Should not be called");
            }
        }

        class BufferedReaderStub : AbstractBufferedReader
        {
            readonly byte[] _input;
            readonly int _bufLength;
            int _pos;

            public BufferedReaderStub(byte[] input, int bufLength)
            {
                _input = input;
                _pos = 0;
                _bufLength = bufLength;
                Buf = new byte[_bufLength];
            }

            protected override void FillBuffer()
            {
                if (End == -1) return;
                _pos += Pos;
                Pos = 0;
                End = _bufLength;
                if (_pos + End > _input.Length) End = _input.Length - _pos;
                Array.Copy(_input, _pos, Buf, 0, End);
                if (End == 0)
                {
                    Pos = -1;
                    End = -1;
                }
            }

            public override long GetCurrentPosition()
            {
                throw new AssertionException("Should not be called");
            }
        }

        static void TestWriteRead(Action<AbstractBufferedWriter> writeAction, byte[] checkResult,
                  Action<AbstractBufferedReader> readAction, Action<AbstractBufferedReader> skipAction)
        {
            var sw = new BufferedWriterStub(1);
            writeAction(sw);
            sw.FlushBuffer();
            Assert.Equal(checkResult, sw.Output);
            BufferedReaderStub sr;
            if (checkResult.Length > 1)
            {
                sw = new BufferedWriterStub(checkResult.Length);
                writeAction(sw);
                sw.FlushBuffer();
                Assert.Equal(checkResult, sw.Output);
                sw = new BufferedWriterStub(checkResult.Length + 1);
                writeAction(sw);
                sw.FlushBuffer();
                Assert.Equal(checkResult, sw.Output);
                sr = new BufferedReaderStub(checkResult, checkResult.Length);
                readAction(sr);
                Assert.True(sr.Eof);
            }
            sr = new BufferedReaderStub(checkResult, 1);
            readAction(sr);
            Assert.True(sr.Eof);
            sw = new BufferedWriterStub(checkResult.Length * 2);
            writeAction(sw);
            writeAction(sw);
            sw.FlushBuffer();
            Assert.Equal(checkResult.Concat(checkResult).ToArray(), sw.Output);
            sr = new BufferedReaderStub(checkResult.Concat(checkResult).ToArray(), checkResult.Length * 2);
            readAction(sr);
            readAction(sr);
            if (skipAction != null)
            {
                sr = new BufferedReaderStub(checkResult.Concat(checkResult).ToArray(), checkResult.Length * 2);
                skipAction(sr);
                readAction(sr);
                Assert.True(sr.Eof);
                sr = new BufferedReaderStub(checkResult.Concat(checkResult).ToArray(), checkResult.Length * 2);
                readAction(sr);
                skipAction(sr);
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
            TestWriteRead(w => w.WriteDateTime(value), checkResult, r => Assert.Equal(value, r.ReadDateTime()), s => s.SkipDateTime());
        }

        [Fact]
        public void TimeSpanTest()
        {
            TestTimeSpan(new TimeSpan(1), new byte[] { 0x81 });
            TestTimeSpan(new TimeSpan(1, 0, 0), new byte[] { 0xfc, 0x08, 0x61, 0xc4, 0x68, 0x00 });
        }

        static void TestTimeSpan(TimeSpan value, byte[] checkResult)
        {
            TestWriteRead(w => w.WriteTimeSpan(value), checkResult, r => Assert.Equal(value, r.ReadTimeSpan()), s => s.SkipTimeSpan());
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
            TestWriteRead(w => w.WriteString(value), checkResult, r => Assert.Equal(value, r.ReadString()), s => s.SkipString());
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
            TestWriteRead(w => w.WriteStringOrdered(value), checkResult, r => Assert.Equal(value, r.ReadStringOrdered()), s => s.SkipStringOrdered());
        }

        [Fact]
        public void UInt8Test()
        {
            TestWriteRead(w => w.WriteUInt8(42), new byte[] { 42 }, r => Assert.Equal(42, r.ReadUInt8()), s => s.SkipUInt8());
        }

        [Fact]
        public void Int8Test()
        {
            TestWriteRead(w => w.WriteInt8(-42), new byte[] { 0xD6 }, r => Assert.Equal(-42, r.ReadInt8()), s => s.SkipInt8());
        }

        [Fact]
        public void GuidTest()
        {
            TestWriteRead(w => w.WriteGuid(Guid.Empty), Guid.Empty.ToByteArray(), r => Assert.Equal(Guid.Empty, r.ReadGuid()), s => s.SkipGuid());
            var g = Guid.NewGuid();
            TestWriteRead(w => w.WriteGuid(g), g.ToByteArray(), r => Assert.Equal(g, r.ReadGuid()), s => s.SkipGuid());
        }

        [Fact]
        public void BlockTest()
        {
            var b = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
            TestWriteRead(w => w.WriteBlock(b), b, r =>
                                                       {
                                                           var b2 = new byte[b.Length];
                                                           r.ReadBlock(b2);
                                                           Assert.Equal(b, b2);
                                                       }, s => s.SkipBlock(b.Length));
            var bExpect = new byte[] { 2, 3, 4, 5, 6 };
            var b2Expect = new byte[] { 0, 2, 3, 4, 5, 6, 0 };
            TestWriteRead(w => w.WriteBlock(b, 1, 5), bExpect, r =>
            {
                var b2 = new byte[b.Length];
                r.ReadBlock(b2, 1, 5);
                Assert.Equal(b2Expect, b2);
            }, s => s.SkipBlock(5u));
        }

        [Fact]
        public void Int64Test()
        {
            TestWriteRead(w => w.WriteInt64(0x1234567890ABCDEFL), new byte[] { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF }, r => Assert.Equal(0x1234567890ABCDEFL, r.ReadInt64()), s => s.SkipInt64());
        }

        [Fact]
        public void Int32Test()
        {
            TestWriteRead(w => w.WriteInt32(0x12345678), new byte[] { 0x12, 0x34, 0x56, 0x78 }, r => Assert.Equal(0x12345678, r.ReadInt32()), s => s.SkipInt32());
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
            TestWriteRead(w => w.WriteVUInt32(value), checkResult, r => Assert.Equal(value, r.ReadVUInt32()), s => s.SkipVUInt32());
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
            TestWriteRead(w => w.WriteVUInt64(value), checkResult, r => Assert.Equal(value, r.ReadVUInt64()), s => s.SkipVUInt64());
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
            TestWriteRead(w => w.WriteVInt32(value), checkResult, r => Assert.Equal(value, r.ReadVInt32()), s => s.SkipVInt64());
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
            TestWriteRead(w => w.WriteVInt64(value), checkResult, r => Assert.Equal(value, r.ReadVInt64()), s => s.SkipVInt64());
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
            TestWriteRead(w => w.WriteDecimal(value), checkResult, r => Assert.Equal(value, r.ReadDecimal()), s => s.SkipDecimal());
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
            TestWriteRead(w => w.WriteSingle(value), checkResult, r => Assert.Equal(value, r.ReadSingle()), s => s.SkipSingle());
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
            TestWriteRead(w => w.WriteDouble(value), checkResult, r => Assert.Equal(value, r.ReadDouble()), s => s.SkipDouble());
        }

        [Fact]
        public void IPAddressTest()
        {
            TestIPAddress(IPAddress.Loopback, new byte[] { 0, 127, 0, 0, 1 });
            TestIPAddress(IPAddress.Broadcast, new byte[] { 0, 255, 255, 255, 255 });
            TestIPAddress(IPAddress.IPv6Loopback, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
            TestIPAddress(IPAddress.IPv6Any, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
            var ip = IPAddress.IPv6Loopback;
            ip.ScopeId = 1;
            TestIPAddress(ip, new byte[] { 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1 });
        }

        static void TestIPAddress(IPAddress value, byte[] checkResult)
        {
            TestWriteRead(w => w.WriteIPAddress(value), checkResult, r => Assert.Equal(value, r.ReadIPAddress()), s => s.SkipIPAddress());
        }
    }
}
