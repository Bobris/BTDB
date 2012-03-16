using BTDB.Buffer;

namespace BTDB.KV2DBLayer
{
    public interface ICompressionStrategy
    {
        bool ShouldTryToCompressKey(int length);
        // Return true if it was compressed
        bool CompressKey(ref ByteBuffer data);
        // Return true if it was compressed
        bool CompressValue(ref ByteBuffer data);
        void DecompressKey(ref ByteBuffer data);
        void DecompressValue(ref ByteBuffer data);
    }
}
