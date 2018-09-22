using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public class ByteBufferReader : AbstractBufferedReader, ICanMemorizePosition
    {
        int _startPos;

        public ByteBufferReader(ByteBuffer byteBuffer)
        {
            Buf = byteBuffer.Buffer;
            Pos = byteBuffer.Offset;
            _startPos = Pos;
            End = Pos + byteBuffer.Length;
        }

        public void Restart(ByteBuffer byteBuffer)
        {
            Buf = byteBuffer.Buffer;
            Pos = byteBuffer.Offset;
            _startPos = Pos;
            End = Pos + byteBuffer.Length;
        }

        protected sealed override void FillBuffer()
        {
            Pos = -1;
            End = -1;
        }

        public override long GetCurrentPosition()
        {
            return Pos - _startPos;
        }

        public IMemorizedPosition MemorizeCurrentPosition()
        {
            return new MemorizedPosition(this, Pos, End);
        }

        class MemorizedPosition : IMemorizedPosition
        {
            readonly ByteBufferReader _owner;
            readonly int _pos;
            readonly int _end;

            internal MemorizedPosition(ByteBufferReader owner, int pos, int end)
            {
                _owner = owner;
                _pos = pos;
                _end = end;
            }

            public void Restore()
            {
                _owner.Pos = _pos;
                _owner.End = _end;
            }
        }
    }
}