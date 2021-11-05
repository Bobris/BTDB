using System;
using BTDB.Buffer;
using BTDB.SnappyCompression;

namespace BTDB.KVDBLayer;

public class SnappyCompressionStrategy : ICompressionStrategy
{
    public bool CompressValue(ref ByteBuffer data)
    {
        if (data.Length < 32) return false;
        return SnappyCompress.TryCompress(ref data, 80);
    }

    public bool CompressValue(ref ReadOnlySpan<byte> data)
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

    public byte[] DecompressKey(ReadOnlySpan<byte> data)
    {
        return SnappyDecompress.Decompress(data);
    }

    public byte[] DecompressValue(ReadOnlySpan<byte> data)
    {
        return SnappyDecompress.Decompress(data);
    }
}
