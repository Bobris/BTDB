using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public interface IKviCompressionStrategy
{
    MemReader StartDecompression(KeyIndexCompression compression, in MemReader outsideReader);
    void FinishDecompression(KeyIndexCompression compression, in MemReader decompressor, ref MemReader outsideReader);
    KeyIndexCompression ChooseCompression(ulong keyCount);
    MemWriter StartCompression(KeyIndexCompression compression, in MemWriter outsideWriter);
    void FinishCompression(KeyIndexCompression compression, in MemWriter compressor, ref MemWriter outsideWriter);
}
