using System;

namespace BTDB.KVDBLayer.ReaderWriters
{
    public class ByteArrayWriter : AbstractBufferedWriter, IDisposable
    {
        byte[] _result;

        public ByteArrayWriter()
        {
            Buf = new byte[4096];
            End = Buf.Length;
        }

        public byte[] Data { get { return _result; } }

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

        public override void FlushBuffer()
        {
            JustFlushWithoutAllocNewBuffer();
            Buf = new byte[4096];
            End = Buf.Length;
            Pos = 0;
        }

        public virtual void Dispose()
        {
            if (Pos != 0) JustFlushWithoutAllocNewBuffer();
        }
    }
}