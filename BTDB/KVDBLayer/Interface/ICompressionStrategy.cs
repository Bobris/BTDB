using System;
using BTDB.Buffer;

namespace BTDB.KVDBLayer;

public interface ICompressionStrategy
{
    // Return true if it was compressed
    bool CompressValue(ref ByteBuffer data);
    bool CompressValue(ref ReadOnlySpan<byte> data);
    void DecompressKey(ref ByteBuffer data);
    void DecompressValue(ref ByteBuffer data);
    byte[] DecompressKey(ReadOnlySpan<byte> data);
    byte[] DecompressValue(ReadOnlySpan<byte> data);
}
