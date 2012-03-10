using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.KV2DBLayer
{
    public class NoCompressionStrategy : ICompressionStrategy
    {
        public bool ShouldTryToCompressKeyToTransactionLog(int length)
        {
            return false;
        }

        public bool CompressKeyToTransactionLog(ref ByteBuffer data)
        {
            return false;
        }

        public bool CompressValueToTransactionLog(ref ByteBuffer data)
        {
            return false;
        }

        public void DecompressKeyFromTransactionLog(ref ByteBuffer data)
        {
            throw new BTDBException("Compression not supported");
        }

        public void DecompressValueFromTransactionLog(ref ByteBuffer data)
        {
            throw new BTDBException("Compression not supported");
        }
    }
}