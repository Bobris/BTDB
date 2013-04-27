using BTDB.Buffer;
using BTDB.SnappyCompression;

namespace BTDB.EventStoreLayer
{
    public class NoCompressionStrategy : ICompressionStrategy
    {
        public bool ShouldTryToCompress(int length)
        {
            return false;
        }

        public bool Compress(ref ByteBuffer data)
        {
            return false;
        }

        public void Decompress(ref ByteBuffer data)
        {
            data = SnappyDecompress.Decompress(data);
        }
    }
}