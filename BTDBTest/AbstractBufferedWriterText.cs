using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BTDB;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class AbstractBufferedWriterText
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

        static void TestWriteRead(Action<AbstractBufferedWriter> writeAction, byte[] checkResult,
                  Action<AbstractBufferedReader> readAction)
        {
            var s = new BufferedWriterStub(1);
            writeAction(s);
            s.FlushBuffer();
            Assert.AreEqual(checkResult, s.Output);
            if (checkResult.Length > 1)
            {
                s = new BufferedWriterStub(checkResult.Length);
                writeAction(s);
                s.FlushBuffer();
                Assert.AreEqual(checkResult, s.Output);
                s = new BufferedWriterStub(checkResult.Length + 1);
                writeAction(s);
                s.FlushBuffer();
                Assert.AreEqual(checkResult, s.Output);
            }
        }

        [Test]
        public void DateTimeTest()
        {
            var d = new DateTime(1976, 2, 2);
            TestWriteRead(w => w.WriteDateTime(d), new byte[] { 0x08, 0xa6, 0x52, 0xcd, 0x43, 0xff, 0xc0, 0x00 }, r => Assert.AreEqual(d, r.ReadDateTime()));
        }
    }
}
