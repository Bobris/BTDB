using BTDB.Buffer;

namespace BTDB.EventStoreLayer
{
    public interface ICompressionStrategy
    {
        bool ShouldTryToCompress(int length);
        // Return true if it was compressed
        bool Compress(ref ByteBuffer data);
        void Decompress(ref ByteBuffer data);
    }
}