using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.KV2DBLayer
{
    public class NoCompressionStrategy : ICompressionStrategy
    {
        public bool ShouldTryToCompressKey(int length)
        {
            return false;
        }

        public bool CompressKey(ref ByteBuffer data)
        {
            return false;
        }

        public bool CompressValue(ref ByteBuffer data)
        {
            return false;
        }

        public void DecompressKey(ref ByteBuffer data)
        {
            throw new BTDBException("Compression not supported");
        }

        public void DecompressValue(ref ByteBuffer data)
        {
            throw new BTDBException("Compression not supported");
        }
    }
}