namespace BTDB.KVDBLayer
{
    public class ByteArrayReader : AbstractBufferedReader
    {
        public ByteArrayReader(byte[] data)
        {
            Buf = data;
            Pos = 0;
            End = data.Length;
        }

        protected override sealed void FillBuffer()
        {
            Pos = 0;
            End = -1;
        }
    }
}