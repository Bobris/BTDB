using BTDB.Buffer;
using BTDB.SnappyCompression;

namespace BTDB.EventStoreLayer
{
    public class SnappyCompressionStrategy : ICompressionStrategy
    {
        public bool ShouldTryToCompress(int length)
        {
            return length > 512;
        }

        public bool Compress(ref ByteBuffer data)
        {
            return SnappyCompress.TryCompress(ref data, 80);
        }

        public void Decompress(ref ByteBuffer data)
        {
            data = SnappyDecompress.Decompress(data);
        }
    }
}