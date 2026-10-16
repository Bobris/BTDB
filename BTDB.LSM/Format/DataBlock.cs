using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using BTDB.LSM.Engine;
using BTDB.SnappyCompression;

namespace BTDB.LSM.Format;

/// Builds one data block (ImplementationPlan.md, section 9): prefix-compressed entries, a restart point every
/// restartInterval entries with the cumulative delta at that point, and a trailer with restart and entry counts.
sealed class DataBlockBuilder(int restartInterval)
{
    public const int TrailerSize = 12;

    readonly LsmEncoder _encoder = new(64 * 1024);
    readonly List<(int Offset, int DeltaBefore)> _restarts = [];
    byte[] _lastKey = [];
    int _delta;
    int _minPrefix;
    int _maxPrefix;

    public int EntryCount { get; private set; }
    public int EstimatedSize => _encoder.Length + _restarts.Count * 8 + TrailerSize;
    public byte[]? FirstKey { get; private set; }
    public int MinPrefix => _minPrefix;
    public int MaxPrefix => _maxPrefix;
    public int DeltaSum => _delta;

    public void Add(in LsmEntry entry)
    {
        var key = entry.Key;
        var shared = 0;
        if (EntryCount % restartInterval == 0)
        {
            _restarts.Add((_encoder.Length, _delta));
        }
        else
        {
            var max = Math.Min(key.Length, _lastKey.Length);
            while (shared < max && key[shared] == _lastKey[shared]) shared++;
        }

        FirstKey ??= key;
        _encoder.WriteVUInt64((ulong)shared);
        _encoder.WriteVUInt64((ulong)(key.Length - shared));
        _encoder.WriteByte((byte)((entry.IsSet ? 1 : 0) | (entry.ExistsBelow ? 2 : 0) | (entry.IsSeparated ? 4 : 0)));
        if (entry.IsSet) _encoder.WriteVUInt64((ulong)entry.Value.Length);
        _encoder.WriteBytes(key.AsSpan(shared));
        if (entry.IsSet) _encoder.WriteBytes(entry.Value.Span);
        _lastKey = key;
        _delta += entry.Delta;
        _minPrefix = Math.Min(_minPrefix, _delta);
        _maxPrefix = Math.Max(_maxPrefix, _delta);
        EntryCount++;
    }

    /// Appends the finished block to output: payload, compression type and checksum.
    public void Finish(LsmEncoder output, bool compress)
    {
        foreach (var (offset, deltaBefore) in _restarts)
        {
            _encoder.WriteInt32(offset);
            _encoder.WriteInt32(deltaBefore);
        }

        _encoder.WriteInt32(restartInterval);
        _encoder.WriteInt32(_restarts.Count);
        _encoder.WriteInt32(EntryCount);
        var start = output.Length;
        var payload = _encoder.WrittenSpan;
        var written = false;
        if (compress && payload.Length > 64)
        {
            var compressed = new byte[payload.Length * 7 / 8];
            var length = SnappyCompress.Compress(compressed, payload);
            if (length > 0)
            {
                output.WriteBytes(compressed.AsSpan(0, length));
                output.WriteByte(1);
                written = true;
            }
        }

        if (!written)
        {
            output.WriteBytes(payload);
            output.WriteByte(0);
        }

        output.WriteChecksum(start);
    }
}

/// Read access to one decompressed data block as stored in the cache (ImplementationPlan.md, section 9). Keys are
/// prefix-compressed with a restart point every RestartInterval entries; a lookup binary-searches the restart keys,
/// which are stored whole, then decodes at most one restart interval. Keys are rebuilt in a reusable thread-local
/// buffer, so only returned entries allocate.
readonly ref struct RawBlock
{
    readonly ReadOnlySpan<byte> _data;
    readonly int _restartsStart;

    [ThreadStatic] static byte[]? _keyBuffer;

    public RawBlock(ReadOnlySpan<byte> data)
    {
        _data = data;
        var trailer = new LsmDecoder(data, data.Length - DataBlockBuilder.TrailerSize);
        RestartInterval = trailer.ReadInt32();
        RestartCount = trailer.ReadInt32();
        Count = trailer.ReadInt32();
        _restartsStart = data.Length - DataBlockBuilder.TrailerSize - RestartCount * 8;
        if (_restartsStart < 0 || RestartInterval <= 0) throw LsmDecoder.Corrupted();
    }

    public int Count { get; }
    public int RestartInterval { get; }
    public int RestartCount { get; }

    int RestartOffset(int restart) => BinaryPrimitives.ReadInt32LittleEndian(_data[(_restartsStart + restart * 8)..]);

    int RestartDelta(int restart) => BinaryPrimitives.ReadInt32LittleEndian(_data[(_restartsStart + restart * 8 + 4)..]);

    /// Whole key stored at a restart point.
    public ReadOnlySpan<byte> RestartKey(int restart)
    {
        var decoder = new LsmDecoder(_data, RestartOffset(restart));
        decoder.ReadVUInt32();
        var unshared = decoder.ReadVUInt32();
        var flags = decoder.ReadByte();
        if ((flags & 1) != 0) decoder.ReadVUInt32();
        return decoder.Take(unshared);
    }

    /// Decodes the entry at offset, rebuilding its key from the previous key held in buffer[..keyLength].
    int DecodeAt(int offset, ref byte[] buffer, ref int keyLength, out byte flags, out int valueStart,
        out int valueLength)
    {
        var decoder = new LsmDecoder(_data, offset);
        var shared = decoder.ReadVUInt32();
        var unshared = decoder.ReadVUInt32();
        flags = decoder.ReadByte();
        valueLength = (flags & 1) != 0 ? decoder.ReadVUInt32() : 0;
        if (shared > keyLength) throw LsmDecoder.Corrupted();
        if (buffer.Length < shared + unshared) Array.Resize(ref buffer, Math.Max(shared + unshared, buffer.Length * 2));
        decoder.Take(unshared).CopyTo(buffer.AsSpan(shared));
        keyLength = shared + unshared;
        valueStart = decoder.Position;
        decoder.Take(valueLength);
        return decoder.Position;
    }

    static int Delta(byte flags) => (flags & 1) - ((flags >> 1) & 1);

    LsmEntry Materialize(ReadOnlySpan<byte> key, byte flags, int valueStart, int valueLength) =>
        new(key.ToArray(), (flags & 1) != 0, (flags & 2) != 0, _data.Slice(valueStart, valueLength).ToArray(),
            (flags & 4) != 0);

    /// Index of the first entry with key greater than (or equal to, when inclusive) key.
    public int LowerBound(ReadOnlySpan<byte> key, bool inclusive)
    {
        // Last restart whose key still precedes the answer; the answer lies in its interval or right after it.
        int lo = 0, hi = RestartCount;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = RestartKey(mid).SequenceCompareTo(key);
            if (cmp < 0 || !inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        if (lo == 0) return 0;
        var restart = lo - 1;
        var index = restart * RestartInterval;
        var end = Math.Min(Count, index + RestartInterval);
        var buffer = _keyBuffer ??= new byte[256];
        var keyLength = 0;
        var offset = RestartOffset(restart);
        for (; index < end; index++)
        {
            offset = DecodeAt(offset, ref buffer, ref keyLength, out _, out _, out _);
            var cmp = buffer.AsSpan(0, keyLength).SequenceCompareTo(key);
            if (cmp > 0 || inclusive && cmp == 0) break;
        }

        _keyBuffer = buffer;
        return index;
    }

    /// Sum of deltas of the first count entries.
    public int DeltaPrefix(int count)
    {
        if (count == 0) return 0;
        var restart = Math.Min((count - 1) / RestartInterval, RestartCount - 1);
        var sum = RestartDelta(restart);
        var decoder = new LsmDecoder(_data, RestartOffset(restart));
        for (var index = restart * RestartInterval; index < count; index++)
        {
            decoder.ReadVUInt32();
            var unshared = decoder.ReadVUInt32();
            var flags = decoder.ReadByte();
            var valueLength = (flags & 1) != 0 ? decoder.ReadVUInt32() : 0;
            decoder.Take(unshared + valueLength);
            sum += Delta(flags);
        }

        return sum;
    }

    /// The entry at index and the offset of the entry after it.
    public LsmEntry Entry(int index, out int nextOffset)
    {
        var restart = index / RestartInterval;
        var buffer = _keyBuffer ??= new byte[256];
        var keyLength = 0;
        var offset = RestartOffset(restart);
        byte flags = 0;
        int valueStart = 0, valueLength = 0;
        for (var i = restart * RestartInterval; i <= index; i++)
            offset = DecodeAt(offset, ref buffer, ref keyLength, out flags, out valueStart, out valueLength);
        _keyBuffer = buffer;
        nextOffset = offset;
        return Materialize(buffer.AsSpan(0, keyLength), flags, valueStart, valueLength);
    }

    public LsmEntry Entry(int index) => Entry(index, out _);

    /// The entry stored at offset whose predecessor has previousKey, and the offset after it.
    public LsmEntry EntryAt(int offset, byte[] previousKey, out int nextOffset)
    {
        var buffer = _keyBuffer ??= new byte[256];
        if (buffer.Length < previousKey.Length) buffer = new byte[previousKey.Length * 2];
        previousKey.CopyTo(buffer, 0);
        var keyLength = previousKey.Length;
        nextOffset = DecodeAt(offset, ref buffer, ref keyLength, out var flags, out var valueStart, out var valueLength);
        _keyBuffer = buffer;
        return Materialize(buffer.AsSpan(0, keyLength), flags, valueStart, valueLength);
    }

    /// Decompresses and verifies a stored block, returning the raw block bytes kept in the cache.
    public static byte[] Decode(ReadOnlySpan<byte> stored, string objectName)
    {
        var payloadWithType = LsmChecksum.Verify(stored, objectName);
        var type = payloadWithType[^1];
        var payload = payloadWithType[..^1];
        return type switch
        {
            0 => payload.ToArray(),
            1 => SnappyDecompress.Decompress(payload),
            _ => throw LsmDecoder.Corrupted()
        };
    }
}
