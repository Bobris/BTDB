using System;
using BTDB.Buffer;

namespace BTDB.KVDBLayer
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

        public bool CompressKey(ref ReadOnlySpan<byte> data)
        {
            return false;
        }

        public bool CompressValue(ref ByteBuffer data)
        {
            return false;
        }

        public bool CompressValue(ref ReadOnlySpan<byte> data)
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

        public byte[] DecompressKey(ReadOnlySpan<byte> data)
        {
            throw new BTDBException("Compression not supported");
        }

        public byte[] DecompressValue(ReadOnlySpan<byte> data)
        {
            throw new BTDBException("Compression not supported");
        }
    }
}
