using System;
using System.IO;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class DefaultCompressionKviStrategy : IKviCompressionStrategy
{
    readonly int _compression;
    readonly int _windowBits;
    readonly ulong _thresholdKeysToEnableCompression;

    public DefaultCompressionKviStrategy(int compressionStrength = -1, int windowBits = 22, ulong thresholdKeysToEnableCompression = 10000)
    {
        _compression = compressionStrength;
        _windowBits = windowBits;
        _thresholdKeysToEnableCompression = thresholdKeysToEnableCompression;
    }

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

    public void FinishDecompression(KeyIndexCompression compression, ISpanReader decompressor)
    {
        if (compression == KeyIndexCompression.None) return;
        (decompressor as IDisposable)?.Dispose();
    }

    public (KeyIndexCompression, ISpanWriter) StartCompression(ulong keyCount, ISpanWriter stream)
    {
        // For small DB skip compression completely
        return keyCount < _thresholdKeysToEnableCompression || _compression < 0
            ? (KeyIndexCompression.None, stream)
            : (KeyIndexCompression.Brotli, new BrotliCompressSpanWriter(stream, _compression, _windowBits));
    }

    public void FinishCompression(KeyIndexCompression compression, ISpanWriter compressor)
    {
        if (compression == KeyIndexCompression.None) return;
        (compressor as IDisposable)?.Dispose();
    }
}
