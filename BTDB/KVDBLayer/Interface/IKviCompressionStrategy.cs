using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public interface IKviCompressionStrategy
{
    ISpanReader StartDecompression(KeyIndexCompression compression, ISpanReader stream);
    void FinishDecompression(KeyIndexCompression compression, ISpanReader decompressor);
    (KeyIndexCompression, ISpanWriter) StartCompression(ulong keyCount, ISpanWriter stream);
    void FinishCompression(KeyIndexCompression compression, ISpanWriter compressor);
}
