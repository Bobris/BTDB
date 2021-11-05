using System;

namespace BTDB.EventStoreLayer;

public interface ICompressionStrategy
{
    bool ShouldTryToCompress(int length);
    // Return true if it was compressed
    bool Compress(ref ReadOnlySpan<byte> data);
    void Decompress(ref ReadOnlySpan<byte> data);
}
