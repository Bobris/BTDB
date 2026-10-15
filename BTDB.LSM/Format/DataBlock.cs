using System;
using System.Collections.Generic;
using BTDB.LSM.Engine;
using BTDB.SnappyCompression;

namespace BTDB.LSM.Format;

/// Builds one data block (ImplementationPlan.md, section 9): prefix-compressed entries, a restart point every
/// restartInterval entries with the cumulative delta at that point, and a trailer with restart and entry counts.
sealed class DataBlockBuilder(int restartInterval)
{
    readonly LsmEncoder _encoder = new(64 * 1024);
    readonly List<(int Offset, int DeltaBefore)> _restarts = [];
    byte[] _lastKey = [];
    int _delta;
    int _minPrefix;
    int _maxPrefix;

    public int EntryCount { get; private set; }
    public int EstimatedSize => _encoder.Length + _restarts.Count * 8 + 8;
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
        _encoder.WriteByte((byte)((entry.IsSet ? 1 : 0) | (entry.ExistsBelow ? 2 : 0)));
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

/// A decoded data block kept in the block cache. It is fully decoded for simplicity; decoding only the needed restart
/// interval is a later optimization.
sealed class DataBlock
{
    readonly byte[] _data;
    readonly byte[][] _keys;
    readonly byte[] _flags;
    readonly int[] _valueStart;
    readonly int[] _valueLength;
    readonly int[] _deltaPrefix;

    DataBlock(byte[] data, byte[][] keys, byte[] flags, int[] valueStart, int[] valueLength, int[] deltaPrefix,
        long memorySize)
    {
        _data = data;
        _keys = keys;
        _flags = flags;
        _valueStart = valueStart;
        _valueLength = valueLength;
        _deltaPrefix = deltaPrefix;
        MemorySize = memorySize;
    }

    public int Count => _keys.Length;
    public long MemorySize { get; }

    public static DataBlock Decode(ReadOnlySpan<byte> stored, string objectName)
    {
        var payloadWithType = LsmChecksum.Verify(stored, objectName);
        var type = payloadWithType[^1];
        var payload = payloadWithType[..^1];
        var data = type switch
        {
            0 => payload.ToArray(),
            1 => SnappyDecompress.Decompress(payload),
            _ => throw LsmDecoder.Corrupted()
        };
        var trailer = new LsmDecoder(data, data.Length - 8);
        var restartCount = trailer.ReadInt32();
        var entryCount = trailer.ReadInt32();
        var entriesEnd = data.Length - 8 - restartCount * 8;
        if (entriesEnd < 0) throw LsmDecoder.Corrupted();
        var keys = new byte[entryCount][];
        var flags = new byte[entryCount];
        var valueStart = new int[entryCount];
        var valueLength = new int[entryCount];
        var deltaPrefix = new int[entryCount + 1];
        var decoder = new LsmDecoder(data.AsSpan(0, entriesEnd));
        byte[] previous = [];
        long keyBytes = 0;
        for (var i = 0; i < entryCount; i++)
        {
            var shared = decoder.ReadVUInt32();
            var unshared = decoder.ReadVUInt32();
            var flag = decoder.ReadByte();
            var length = (flag & 1) != 0 ? decoder.ReadVUInt32() : 0;
            if (shared > previous.Length) throw LsmDecoder.Corrupted();
            var key = new byte[shared + unshared];
            previous.AsSpan(0, shared).CopyTo(key);
            decoder.Take(unshared).CopyTo(key.AsSpan(shared));
            valueStart[i] = decoder.Position;
            valueLength[i] = length;
            decoder.Take(length);
            keys[i] = key;
            flags[i] = flag;
            keyBytes += key.Length + 32;
            deltaPrefix[i + 1] = deltaPrefix[i] + ((flag & 1) != 0 ? 1 : 0) - ((flag & 2) != 0 ? 1 : 0);
            previous = key;
        }

        return new(data, keys, flags, valueStart, valueLength, deltaPrefix,
            data.Length + keyBytes + entryCount * 13L + 128);
    }

    public byte[] Key(int index) => _keys[index];

    public LsmEntry Entry(int index) => new(_keys[index], (_flags[index] & 1) != 0, (_flags[index] & 2) != 0,
        _data.AsMemory(_valueStart[index], _valueLength[index]));

    /// Sum of deltas of the first count entries.
    public int DeltaPrefix(int count) => _deltaPrefix[count];

    /// Index of the first entry with key greater than (or equal to, when inclusive) key.
    public int LowerBound(ReadOnlySpan<byte> key, bool inclusive)
    {
        int lo = 0, hi = _keys.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = LsmKeys.Compare(_keys[mid], key);
            if (cmp < 0 || !inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }
}
