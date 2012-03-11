using BTDB.Buffer;
using BTDB.SnappyCompression;

namespace BTDB.KV2DBLayer
{
    public class SnappyCompressionStrategy : ICompressionStrategy
    {
        public bool ShouldTryToCompressKeyToTransactionLog(int length)
        {
            return length > 1024;
        }

        public bool CompressKeyToTransactionLog(ref ByteBuffer data)
        {
            return SnappyCompress.TryCompress(ref data, 80);
        }

        public bool CompressValueToTransactionLog(ref ByteBuffer data)
        {
            if (data.Length < 32) return false;
            return SnappyCompress.TryCompress(ref data, 80);
        }

        public void DecompressKeyFromTransactionLog(ref ByteBuffer data)
        {
            data = SnappyDecompress.Decompress(data);
        }

        public void DecompressValueFromTransactionLog(ref ByteBuffer data)
        {
            data = SnappyDecompress.Decompress(data);
        }
    }
}