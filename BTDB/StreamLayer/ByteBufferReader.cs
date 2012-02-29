using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public class ByteBufferReader : AbstractBufferedReader
    {
        readonly int _startPos;

        public ByteBufferReader(ByteBuffer byteBuffer)
        {
            Buf = byteBuffer.Buffer;
            Pos = byteBuffer.Offset;
            _startPos = Pos;
            End = Pos + byteBuffer.Length;
        }

        protected override sealed void FillBuffer()
        {
            Pos = 0;
            End = -1;
        }

        public override long GetCurrentPosition()
        {
            return Pos - _startPos;
        }
    }
}