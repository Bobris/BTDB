using BTDB.Buffer;

namespace BTDB.KVDBLayer
{
    public class ByteBufferReader : AbstractBufferedReader
    {
        public ByteBufferReader(ByteBuffer byteBuffer)
        {
            Buf = byteBuffer.Buffer;
            Pos = byteBuffer.Offset;
            End = Pos + byteBuffer.Length;
        }

        protected override sealed void FillBuffer()
        {
            Pos = 0;
            End = -1;
        }
    }
}