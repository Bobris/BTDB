using System;
using System.Buffers.Binary;
using BTDB.Buffer;

namespace BTDB.KVDBLayer;

/// XxHash64 of what a key index file holds: the header fields loading uses, then each key with its value position, as
/// written and as read back. Damage anywhere in the file, compressed or not, changes what is read or fails reading.
sealed class KeyIndexChecksum
{
    XxHash64 _hash = new();

    public KeyIndexChecksum(IKeyIndex info)
    {
        Span<byte> buf = stackalloc byte[8];
        Append(buf, (ulong)info.Generation);
        Append(buf, info.TrLogFileId);
        Append(buf, info.TrLogOffset);
        Append(buf, (ulong)info.KeyValueCount);
        Append(buf, info.CommitUlong);
        Append(buf, (ulong)info.Compression);
        var ulongs = info.Ulongs ?? [];
        Append(buf, (ulong)ulongs.Length);
        foreach (var value in ulongs) Append(buf, value);
    }

    void Append(Span<byte> buf, ulong value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(buf, value);
        _hash.Update(buf);
    }

    /// A key given in two parts hashes as their concatenation.
    public void AppendKeyValue(ReadOnlySpan<byte> keyStart, ReadOnlySpan<byte> keyRest, uint valueFileId,
        uint valueOfs, int valueSize)
    {
        Span<byte> buf = stackalloc byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(buf, keyStart.Length + keyRest.Length);
        _hash.Update(buf[..4]);
        _hash.Update(keyStart);
        _hash.Update(keyRest);
        BinaryPrimitives.WriteUInt32LittleEndian(buf, valueFileId);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[4..], valueOfs);
        BinaryPrimitives.WriteInt32LittleEndian(buf[8..], valueSize);
        _hash.Update(buf);
    }

    public ulong Digest() => _hash.Digest();
}
