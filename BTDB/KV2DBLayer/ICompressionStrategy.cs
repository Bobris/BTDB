using BTDB.Buffer;

namespace BTDB.KV2DBLayer
{
    public interface ICompressionStrategy
    {
        bool ShouldTryToCompressKeyToTransactionLog(int length);
        bool CompressKeyToTransactionLog(ref ByteBuffer data);
        bool CompressValueToTransactionLog(ref ByteBuffer data);
        void DecompressKeyFromTransactionLog(ref ByteBuffer data);
        void DecompressValueFromTransactionLog(ref ByteBuffer data);
    }
}
