using System;
using BTDB.SnappyCompression;

namespace BTDB.EventStoreLayer;

public class NoCompressionStrategy : ICompressionStrategy
{
    public bool ShouldTryToCompress(int length)
    {
        return false;
    }

    public bool Compress(ref ReadOnlySpan<byte> data)
    {
        return false;
    }

    public void Decompress(ref ReadOnlySpan<byte> data)
    {
        data = SnappyDecompress.Decompress(data);
    }
}
