using System;
using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public sealed class ByteArrayWriter : AbstractBufferedWriter
    {
        byte[] _result;

        public ByteArrayWriter()
        {
            Buf = new byte[4096];
            End = Buf.Length;
        }

        public byte[] Data
        {
            get
            {
                FinalFlushIfNeeded();
                return _result ?? BitArrayManipulation.EmptyByteArray;
            }
        }

        public override void FlushBuffer()
        {
            JustFlushWithoutAllocNewBuffer();
            Buf = new byte[4096];
            End = Buf.Length;
            Pos = 0;
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
            if (_result == null)
            {
                if (Pos == Buf.Length)
                {
                    _result = Buf;
                }
                else
                {
                    _result = new byte[Pos];
                    Array.Copy(Buf, _result, Pos);
                }
            }
            else
            {
                var origLength = _result.Length;
                Array.Resize(ref _result, origLength + Pos);
                Array.Copy(Buf, 0, _result, origLength, Pos);
            }
        }
    }
}