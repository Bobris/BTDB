using System;
using System.Buffers;
using System.Buffers.Binary;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.LSM.Format;

/// Little-endian, LEB128 based encoder used by all BTDB.LSM formats.
sealed class LsmEncoder
{
    readonly ArrayBufferWriter<byte> _buffer;

    public LsmEncoder(int initialCapacity = 256)
    {
        _buffer = new(initialCapacity);
    }

    public int Length => _buffer.WrittenCount;
    public ReadOnlySpan<byte> WrittenSpan => _buffer.WrittenSpan;
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.WrittenMemory;

    public void Clear() => _buffer.ResetWrittenCount();

    public void WriteByte(byte value)
    {
        _buffer.GetSpan(1)[0] = value;
        _buffer.Advance(1);
    }

    public void WriteVUInt64(ulong value)
    {
        var span = _buffer.GetSpan(10);
        var i = 0;
        while (value >= 0x80)
        {
            span[i++] = (byte)(value | 0x80);
            value >>= 7;
        }

        span[i++] = (byte)value;
        _buffer.Advance(i);
    }

    public void WriteVInt64(long value) => WriteVUInt64((ulong)((value << 1) ^ (value >> 63)));

    public void WriteInt32(int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.GetSpan(4), value);
        _buffer.Advance(4);
    }

    public void WriteUInt64(ulong value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(_buffer.GetSpan(8), value);
        _buffer.Advance(8);
    }

    public void WriteInt64(long value) => WriteUInt64((ulong)value);

    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        bytes.CopyTo(_buffer.GetSpan(bytes.Length));
        _buffer.Advance(bytes.Length);
    }

    public void WriteByteArray(ReadOnlySpan<byte> bytes)
    {
        WriteVUInt64((ulong)bytes.Length);
        WriteBytes(bytes);
    }

    /// Appends a checksum of everything written since start.
    public void WriteChecksum(int start)
    {
        WriteUInt64(XxHash64.Hash(_buffer.WrittenSpan[start..]));
    }
}

ref struct LsmDecoder
{
    readonly ReadOnlySpan<byte> _data;
    int _position;

    public LsmDecoder(ReadOnlySpan<byte> data, int position = 0)
    {
        _data = data;
        _position = position;
    }

    public int Position => _position;
    public bool End => _position >= _data.Length;

    public byte ReadByte()
    {
        if (_position >= _data.Length) throw Corrupted();
        return _data[_position++];
    }

    public ulong ReadVUInt64()
    {
        ulong result = 0;
        var shift = 0;
        while (true)
        {
            var b = ReadByte();
            result |= (ulong)(b & 0x7f) << shift;
            if (b < 0x80) return result;
            shift += 7;
            if (shift > 63) throw Corrupted();
        }
    }

    public long ReadVInt64()
    {
        var value = ReadVUInt64();
        return (long)(value >> 1) ^ -(long)(value & 1);
    }

    public int ReadVInt32() => checked((int)ReadVInt64());

    public int ReadVUInt32() => checked((int)ReadVUInt64());

    public int ReadInt32()
    {
        var result = BinaryPrimitives.ReadInt32LittleEndian(Take(4));
        return result;
    }

    public ulong ReadUInt64() => BinaryPrimitives.ReadUInt64LittleEndian(Take(8));

    public long ReadInt64() => (long)ReadUInt64();

    public ReadOnlySpan<byte> Take(int length)
    {
        if (length < 0 || _position + length > _data.Length) throw Corrupted();
        var result = _data.Slice(_position, length);
        _position += length;
        return result;
    }

    public byte[] ReadByteArray() => Take(ReadVUInt32()).ToArray();

    public static BTDBException Corrupted() => new("BTDB.LSM object is corrupted or truncated.");
}

static class LsmChecksum
{
    /// Verifies the 8-byte checksum trailing data and returns the payload.
    public static ReadOnlySpan<byte> Verify(ReadOnlySpan<byte> dataWithChecksum, string objectName)
    {
        if (dataWithChecksum.Length < 8) throw new BTDBException($"BTDB.LSM object {objectName} is truncated.");
        var payload = dataWithChecksum[..^8];
        if (XxHash64.Hash(payload) != BinaryPrimitives.ReadUInt64LittleEndian(dataWithChecksum[^8..]))
            throw new BTDBException($"BTDB.LSM object {objectName} has a checksum mismatch.");
        return payload;
    }
}

static class LsmKeys
{
    public static int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a.SequenceCompareTo(b);

    /// Smallest key greater than every key starting with prefix, or null when no such key exists.
    public static byte[]? PrefixSuccessor(ReadOnlySpan<byte> prefix)
    {
        var length = prefix.Length;
        while (length > 0 && prefix[length - 1] == 0xff) length--;
        if (length == 0) return null;
        var result = prefix[..length].ToArray();
        result[length - 1]++;
        return result;
    }

    /// Smallest key greater than key.
    public static byte[] Successor(ReadOnlySpan<byte> key)
    {
        var result = new byte[key.Length + 1];
        key.CopyTo(result);
        return result;
    }
}
