using System;
using BTDB.Buffer;

namespace BTDB.LSM.Format;

/// Bloom filter on full user keys using double hashing of one 64-bit XxHash.
sealed class BloomFilter
{
    readonly byte[] _bits;
    readonly int _hashCount;
    readonly ulong _bitCount;

    BloomFilter(byte[] bits, int hashCount)
    {
        _bits = bits;
        _hashCount = hashCount;
        _bitCount = (ulong)bits.Length * 8;
    }

    public int SizeInBytes => _bits.Length;

    public static void Build(LsmEncoder encoder, ReadOnlySpan<ulong> keyHashes, int bitsPerKey)
    {
        var hashCount = Math.Clamp((int)Math.Round(bitsPerKey * 0.69), 1, 30);
        var bitCount = Math.Max(64L, (long)keyHashes.Length * bitsPerKey);
        var bits = new byte[(bitCount + 7) / 8];
        var filter = new BloomFilter(bits, hashCount);
        foreach (var hash in keyHashes) filter.Add(hash);
        encoder.WriteVUInt64((ulong)hashCount);
        encoder.WriteByteArray(bits);
    }

    public static BloomFilter Read(ReadOnlySpan<byte> data)
    {
        var decoder = new LsmDecoder(data);
        var hashCount = decoder.ReadVUInt32();
        return new(decoder.ReadByteArray(), hashCount);
    }

    public static ulong Hash(ReadOnlySpan<byte> key) => XxHash64.Hash(key);

    void Add(ulong hash)
    {
        var h1 = (uint)hash;
        var h2 = (uint)(hash >> 32);
        for (var i = 0; i < _hashCount; i++)
        {
            var bit = (h1 + (ulong)i * h2) % _bitCount;
            _bits[bit >> 3] |= (byte)(1 << (int)(bit & 7));
        }
    }

    public bool MayContain(ReadOnlySpan<byte> key)
    {
        var hash = Hash(key);
        var h1 = (uint)hash;
        var h2 = (uint)(hash >> 32);
        for (var i = 0; i < _hashCount; i++)
        {
            var bit = (h1 + (ulong)i * h2) % _bitCount;
            if ((_bits[bit >> 3] & (1 << (int)(bit & 7))) == 0) return false;
        }

        return true;
    }
}
