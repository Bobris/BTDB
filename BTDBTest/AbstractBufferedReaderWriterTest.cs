using System;
using System.Linq;
using BTDB;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
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

            public byte[] Output
            {
                get { return _output; }
            }

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
                _pos += Pos;
                Pos = 0;
                End = _bufLength;
                if (_pos + End > _input.Length) End = _input.Length - _pos;
                Array.Copy(_input, _pos, Buf, 0, End);
                if (End == 0) End = -1;
            }
        }

        static void TestWriteRead(Action<AbstractBufferedWriter> writeAction, byte[] checkResult,
                  Action<AbstractBufferedReader> readAction)
        {
            var sw = new BufferedWriterStub(1);
            writeAction(sw);
            sw.FlushBuffer();
            Assert.AreEqual(checkResult, sw.Output);
            BufferedReaderStub sr;
            if (checkResult.Length > 1)
            {
                sw = new BufferedWriterStub(checkResult.Length);
                writeAction(sw);
                sw.FlushBuffer();
                Assert.AreEqual(checkResult, sw.Output);
                sw = new BufferedWriterStub(checkResult.Length + 1);
                writeAction(sw);
                sw.FlushBuffer();
                Assert.AreEqual(checkResult, sw.Output);
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
            Assert.AreEqual(checkResult.Concat(checkResult).ToArray(), sw.Output);
            sr = new BufferedReaderStub(checkResult.Concat(checkResult).ToArray(), checkResult.Length * 2);
            readAction(sr);
            readAction(sr);
        }

        [Test]
        public void DateTimeTest()
        {
            var d = new DateTime(1976, 2, 2);
            TestDateTime(d, new byte[] { 0x08, 0xa6, 0x52, 0xcd, 0x43, 0xff, 0xc0, 0x00 });
            d = new DateTime(d.Ticks, DateTimeKind.Utc);
            TestDateTime(d, new byte[] { 0x48, 0xa6, 0x52, 0xcd, 0x43, 0xff, 0xc0, 0x00 });
        }

        static void TestDateTime(DateTime value, byte[] checkResult)
        {
            TestWriteRead(w => w.WriteDateTime(value), checkResult, r => Assert.AreEqual(value, r.ReadDateTime()));
        }

        [Test]
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
            TestWriteRead(w => w.WriteString(value), checkResult, r => Assert.AreEqual(value, r.ReadString()));
        }

        [Test]
        public void UInt8Test()
        {
            TestWriteRead(w => w.WriteUInt8(42), new byte[] { 42 }, r => Assert.AreEqual(42, r.ReadUInt8()));
        }

        [Test]
        public void Int8Test()
        {
            TestWriteRead(w => w.WriteInt8(-42), new byte[] { 0xD6 }, r => Assert.AreEqual(-42, r.ReadInt8()));
        }

        [Test]
        public void GuidTest()
        {
            TestWriteRead(w => w.WriteGuid(Guid.Empty), Guid.Empty.ToByteArray(), r => Assert.AreEqual(Guid.Empty, r.ReadGuid()));
        }

        [Test]
        public void BlockTest()
        {
            var b = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
            TestWriteRead(w => w.WriteBlock(b), b, r =>
                                                       {
                                                           var b2 = new byte[b.Length];
                                                           r.ReadBlock(b2);
                                                           Assert.AreEqual(b, b2);
                                                       });
            var bExpect = new byte[] { 2, 3, 4, 5, 6 };
            var b2Expect = new byte[] { 0, 2, 3, 4, 5, 6, 0 };
            TestWriteRead(w => w.WriteBlock(b, 1, 5), bExpect, r =>
            {
                var b2 = new byte[b.Length];
                r.ReadBlock(b2, 1, 5);
                Assert.AreEqual(b2Expect, b2);
            });
        }

        [Test]
        public void Int64Test()
        {
            TestWriteRead(w => w.WriteInt64(0x1234567890ABCDEFL), new byte[] { 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF }, r => Assert.AreEqual(0x1234567890ABCDEFL, r.ReadInt64()));
        }

        [Test]
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
            TestWriteRead(w => w.WriteVUInt32(value), checkResult, r => Assert.AreEqual(value, r.ReadVUInt32()));
        }

        [Test]
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
            TestWriteRead(w => w.WriteVUInt64(value), checkResult, r => Assert.AreEqual(value, r.ReadVUInt64()));
        }

        [Test]
        public void VInt32Test()
        {
            TestVInt32(0, new byte[] { 0 });
            TestVInt32(1, new byte[] { 2 });
            TestVInt32(-1, new byte[] { 1 });
            TestVInt32(int.MinValue, new byte[] { 0xF0, 0xFF, 0xFF, 0xFF, 0xFF });
            TestVInt32(int.MaxValue, new byte[] { 0xF0, 0xFF, 0xFF, 0xFF, 0xFE });
        }

        static void TestVInt32(int value, byte[] checkResult)
        {
            TestWriteRead(w => w.WriteVInt32(value), checkResult, r => Assert.AreEqual(value, r.ReadVInt32()));
        }

        [Test]
        public void VInt64Test()
        {
            TestVInt64(0, new byte[] { 0 });
            TestVInt64(1, new byte[] { 2 });
            TestVInt64(-1, new byte[] { 1 });
            TestVInt64(int.MinValue, new byte[] { 0xF0, 0xFF, 0xFF, 0xFF, 0xFF });
            TestVInt64(int.MaxValue, new byte[] { 0xF0, 0xFF, 0xFF, 0xFF, 0xFE });
            TestVInt64(long.MinValue, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });
            TestVInt64(long.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE });
        }

        static void TestVInt64(long value, byte[] checkResult)
        {
            TestWriteRead(w => w.WriteVInt64(value), checkResult, r => Assert.AreEqual(value, r.ReadVInt64()));
        }
    }
}
