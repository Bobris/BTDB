using System;
using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public class ByteBufferWriter : AbstractBufferedWriter
    {
        ByteBuffer _result;

        public ByteBufferWriter()
        {
            Buf = new byte[32];
            End = Buf.Length;
        }

        public ByteBuffer Data
        {
            get
            {
                if (_result.Length == 0)
                {
                    _result = ByteBuffer.NewAsync(Buf, 0, Pos);
                    Buf = null;
                    Pos = 0;
                    End = 1;
                }
                return _result;
            }
        }

        public override void FlushBuffer()
        {
            var newLen = Math.Max((int)Math.Min((long)End * 2, 2147483591), 128);
            if (newLen == End) throw new OutOfMemoryException();
            Array.Resize(ref Buf, newLen);
            End = Buf.Length;
        }

        public override long GetCurrentPosition()
        {
            return Pos;
        }
    }
}
