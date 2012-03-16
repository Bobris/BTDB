using BTDB.Buffer;
using BTDB.SnappyCompression;

namespace BTDB.KV2DBLayer
{
    public class SnappyCompressionStrategy : ICompressionStrategy
    {
        public bool ShouldTryToCompressKey(int length)
        {
            return length > 1024;
        }

        public bool CompressKey(ref ByteBuffer data)
        {
            return SnappyCompress.TryCompress(ref data, 80);
        }

        public bool CompressValue(ref ByteBuffer data)
        {
            if (data.Length < 32) return false;
            return SnappyCompress.TryCompress(ref data, 80);
        }

        public void DecompressKey(ref ByteBuffer data)
        {
            data = SnappyDecompress.Decompress(data);
        }

        public void DecompressValue(ref ByteBuffer data)
        {
            data = SnappyDecompress.Decompress(data);
        }
    }
}