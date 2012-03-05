using System;
using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public class ByteBufferWriter : AbstractBufferedWriter
    {
        ByteBuffer _result;

        public ByteBufferWriter()
        {
            Buf = new byte[4096];
            End = Buf.Length;
        }

        public ByteBuffer Data
        {
            get
            {
                FinalFlushIfNeeded();
                return _result;
            }
        }

        public override void FlushBuffer()
        {
            JustFlushWithoutAllocNewBuffer();
            if (Buf == null) Buf = new byte[4096];
            End = Buf.Length;
            Pos = 0;
        }

        public override long GetCurrentPosition()
        {
            return _result.Length + Pos;
        }

        void FinalFlushIfNeeded()
        {
            if (Pos != 0)
            {
                JustFlushWithoutAllocNewBuffer();
                Pos = 0;
                Buf = null;
                End = 1;
            }
        }

        void JustFlushWithoutAllocNewBuffer()
        {
            if (_result.Buffer == null || _result.Length == 0)
            {
                _result = ByteBuffer.NewAsync(Buf, 0, Pos);
                Buf = null;
            }
            else
            {
                var origLength = _result.Length;
                var newLength = origLength + Pos;
                var b = _result.Buffer;
                if (newLength > b.Length)
                {
                    _result = new ByteBuffer();
                    Array.Resize(ref b, Math.Max(origLength*2, newLength));
                }
                Array.Copy(Buf, 0, b, origLength, Pos);
                _result = ByteBuffer.NewAsync(b, 0, newLength);
            }
        }
    }
}