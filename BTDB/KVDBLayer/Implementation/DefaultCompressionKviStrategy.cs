using System;
using System.IO;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class DefaultCompressionKviStrategy : IKviCompressionStrategy
{
    public ISpanReader StartDecompression(KeyIndexCompression compression, ISpanReader stream)
    {
        return compression switch
        {
            KeyIndexCompression.None => stream,
            KeyIndexCompression.Brotli => new BrotliDecompressSpanReader(stream),
            KeyIndexCompression.Old => throw new NotSupportedException(),
            _ => throw new InvalidDataException("Unknown Kvi compression " + compression)
        };
    }

    public void FinishDecompression(KeyIndexCompression compression, ISpanReader decompressor,
        IKeyValueDBLogger? logger)
    {
        if (compression == KeyIndexCompression.None) return;
        SpanReader reader = new(decompressor);
        logger?.LogInfo("Kvi " + compression + " decompressed to " + decompressor.GetCurrentPosition(reader) +
                        " bytes");
        reader.Sync();
        (decompressor as IDisposable)?.Dispose();
    }

    public (KeyIndexCompression, ISpanWriter) StartCompression(ulong keyCount, ISpanWriter stream)
    {
        // For small DB skip compression completely
        return keyCount < 10000
            ? (KeyIndexCompression.None, stream)
            : (KeyIndexCompression.Brotli, new BrotliCompressSpanWriter(stream));
    }

    public void FinishCompression(KeyIndexCompression compression, ISpanWriter compressor, IKeyValueDBLogger? logger)
    {
        if (compression == KeyIndexCompression.None) return;
        logger?.LogInfo("Kvi " + compression + " compressed " + compressor.GetCurrentPositionWithoutWriter() +
                        " bytes to " +
                        ((compressor as ICompressedSize)?.GetCompressedSize().ToString() ?? "unknown"));
        (compressor as IDisposable)?.Dispose();
    }
}
