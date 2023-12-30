using System;
using System.IO;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class DefaultCompressionKviStrategy : IKviCompressionStrategy
{
    readonly int _compression;
    readonly int _windowBits;
    readonly ulong _thresholdKeysToEnableCompression;

    public DefaultCompressionKviStrategy(int compressionStrength = -1, int windowBits = 22,
        ulong thresholdKeysToEnableCompression = 10000)
    {
        _compression = compressionStrength;
        _windowBits = windowBits;
        _thresholdKeysToEnableCompression = thresholdKeysToEnableCompression;
    }

    public MemReader StartDecompression(KeyIndexCompression compression, in MemReader outsideReader)
    {
        return compression switch
        {
            KeyIndexCompression.None => outsideReader,
            KeyIndexCompression.Brotli => new(new BrotliDecompressMemReader(outsideReader)),
            KeyIndexCompression.Old => throw new NotSupportedException(),
            _ => throw new InvalidDataException("Unknown Kvi compression " + compression)
        };
    }

    public void FinishDecompression(KeyIndexCompression compression, in MemReader decompressor,
        ref MemReader outsideReader)
    {
        if (compression == KeyIndexCompression.None)
        {
            outsideReader = decompressor;
            return;
        }

        outsideReader = ((BrotliDecompressMemReader)decompressor.Controller).Finish(decompressor);
    }

    public KeyIndexCompression ChooseCompression(ulong keyCount)
    {
        // For small DB skip compression completely
        return keyCount < _thresholdKeysToEnableCompression || _compression < 0
            ? KeyIndexCompression.None
            : KeyIndexCompression.Brotli;
    }

    public MemWriter StartCompression(KeyIndexCompression compression, in MemWriter outsideWriter)
    {
        if (compression == KeyIndexCompression.None) return outsideWriter;
        return new(new BrotliCompressMemWriter(outsideWriter, _compression, _windowBits));
    }

    public void FinishCompression(KeyIndexCompression compression, in MemWriter compressor, ref MemWriter outsideWriter)
    {
        if (compression == KeyIndexCompression.None)
        {
            outsideWriter = compressor;
            return;
        }

        outsideWriter = ((BrotliCompressMemWriter)compressor.Controller!).Finish(compressor);
    }
}
