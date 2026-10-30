using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.ReadOnly;

/// Writes all keys and values visible in a transaction as data for <see cref="ReadOnlyKeyValueDB"/>.
/// Whole data must fit into 4GB and one key into 64KB.
public class ReadOnlyCreator
{
    readonly IKeyValueDBTransaction _transaction;
    uint _maxChildren = ReadOnlyFormat.DefaultMaxChildren;

    public ReadOnlyCreator(IKeyValueDBTransaction transaction)
    {
        _transaction = transaction;
    }

    /// Maximum number of keys in a leaf and children of a branch node, from 2 to 127, default is 31.
    public void SetMaxChildren(uint maxChildren)
    {
        if (maxChildren is < 2 or > ReadOnlyFormat.MaxChildren)
            throw new ArgumentOutOfRangeException(nameof(maxChildren));
        _maxChildren = maxChildren;
    }

    public void WriteToFile(string fileName)
    {
        using var fileWriter = new FileMemWriter(fileName);
        var writer = new MemWriter(fileWriter);
        Run(ref writer);
        writer.Flush();
    }

    public byte[] WriteToArray()
    {
        var writer = new MemWriter();
        Run(ref writer);
        return writer.GetSpan().ToArray();
    }

    readonly record struct NodeInfo(uint Offset, long KeyCount, byte[] FirstKey);

    public void Run(ref MemWriter writer)
    {
        // The data end with the XxHash64 of all bytes before it.
        var hashing = new HashingMemWriter(writer);
        var hashed = new MemWriter(hashing);
        var length = WriteData(ref hashed);
        if (length > uint.MaxValue - 8) throw new BTDBException("Read only database does not fit into 4GB");
        writer = hashing.Finish(hashed, out var checksum);
        writer.WriteUInt64LE(checksum);
    }

    uint WriteData(ref MemWriter writer)
    {
        var start = writer.GetCurrentPosition();
        var keyValueCount = _transaction.GetKeyValueCount();
        WriteHeader(ref writer, keyValueCount);
        var level = new List<NodeInfo>();
        var keys = new List<byte[]>((int)_maxChildren);
        var valueOffsets = new List<uint>((int)_maxChildren);
        var keysLength = 0L;
        var written = 0L;
        using (var cursor = _transaction.CreateCursor())
        {
            var valueBuffer = Span<byte>.Empty;
            if (cursor.FindFirstKey([]))
            {
                do
                {
                    var key = cursor.SlowGetKey();
                    if (key.Length > ReadOnlyFormat.MaxNodeKeyBytes)
                        throw new BTDBException($"Key with {key.Length} bytes is too long for read only database");
                    if (keys.Count == _maxChildren ||
                        keys.Count > 0 && keysLength + key.Length > ReadOnlyFormat.MaxNodeKeyBytes)
                    {
                        level.Add(WriteLeaf(ref writer, start, keys, valueOffsets));
                        keys.Clear();
                        valueOffsets.Clear();
                        keysLength = 0;
                    }

                    valueOffsets.Add(WriteValue(ref writer, start, cursor.GetValueSpan(ref valueBuffer)));
                    keys.Add(key);
                    keysLength += key.Length;
                    written++;
                } while (cursor.FindNextKey([]));
            }
        }

        if (written != keyValueCount)
            throw new BTDBException($"Transaction reports {keyValueCount} keys, but {written} were found");
        if (keys.Count > 0) level.Add(WriteLeaf(ref writer, start, keys, valueOffsets));
        while (level.Count > 1)
        {
            level = WriteBranches(ref writer, start, level);
        }

        writer.WriteUInt32LE(level.Count == 0 ? 0 : level[0].Offset);
        return CurrentOffset(ref writer, start);
    }

    void WriteHeader(ref MemWriter writer, long keyValueCount)
    {
        writer.WriteBlock(ReadOnlyFormat.Magic);
        writer.WriteInt16BE(ReadOnlyFormat.Version);
        writer.WriteVUInt64((ulong)keyValueCount);
        writer.WriteVUInt64(_transaction.GetCommitUlong());
        var ulongCount = _transaction.GetUlongCount();
        writer.WriteVUInt32(ulongCount);
        for (var i = 0u; i < ulongCount; i++)
        {
            writer.WriteVUInt64(_transaction.GetUlong(i));
        }
    }

    static uint CurrentOffset(ref MemWriter writer, long start)
    {
        var offset = writer.GetCurrentPosition() - start;
        if (offset > uint.MaxValue) throw new BTDBException("Read only database does not fit into 4GB");
        return (uint)offset;
    }

    static uint WriteValue(ref MemWriter writer, long start, in ReadOnlySpan<byte> value)
    {
        if (value.Length == 0) return 0;
        var offset = CurrentOffset(ref writer, start);
        writer.WriteVUInt32((uint)value.Length);
        writer.WriteBlock(value);
        return offset;
    }

    static NodeInfo WriteLeaf(ref MemWriter writer, long start, List<byte[]> keys, List<uint> valueOffsets)
    {
        var offset = CurrentOffset(ref writer, start);
        var keySpan = CollectionsMarshal.AsSpan(keys);
        var prefixLength = (int)TreeNodeUtils.CalcCommonPrefix(keySpan);
        writer.WriteUInt8((byte)keys.Count);
        WriteKeys(ref writer, keySpan, prefixLength);
        foreach (var valueOffset in valueOffsets)
        {
            writer.WriteUInt32LE(valueOffset);
        }

        return new(offset, keys.Count, keys[0]);
    }

    List<NodeInfo> WriteBranches(ref MemWriter writer, long start, List<NodeInfo> children)
    {
        var parents = new List<NodeInfo>((int)((children.Count + _maxChildren - 1) / _maxChildren));
        var firstKeys = new byte[_maxChildren][];
        var index = 0;
        while (index < children.Count)
        {
            // Separators are the first keys of all children except the first one
            var count = 1;
            var separatorsLength = 0L;
            while (index + count < children.Count && count < _maxChildren &&
                   separatorsLength + children[index + count].FirstKey.Length <= ReadOnlyFormat.MaxNodeKeyBytes)
            {
                separatorsLength += children[index + count].FirstKey.Length;
                count++;
            }

            for (var i = 0; i < count; i++) firstKeys[i] = children[index + i].FirstKey;
            var firstKeySpan = firstKeys.AsSpan(0, count);
            var prefixLength = count == 1 ? 0 : (int)TreeNodeUtils.CalcCommonPrefix(firstKeySpan);
            var offset = CurrentOffset(ref writer, start);
            writer.WriteUInt8((byte)(ReadOnlyFormat.BranchFlag + count));
            WriteKeys(ref writer, firstKeySpan, prefixLength, 1);
            for (var i = 0; i < count; i++)
            {
                writer.WriteUInt32LE(children[index + i].Offset);
            }

            var keyCount = 0L;
            for (var i = 0; i < count; i++)
            {
                keyCount += children[index + i].KeyCount;
                if (keyCount > uint.MaxValue) throw new BTDBException("Too many keys for read only database");
                writer.WriteUInt32LE((uint)keyCount);
            }

            parents.Add(new(offset, keyCount, firstKeys[0]));
            index += count;
        }

        return parents;
    }

    // Writes prefix length, prefix, end offsets of key suffixes and key suffixes, skipping the first skip keys
    static void WriteKeys(ref MemWriter writer, ReadOnlySpan<byte[]> keys, int prefixLength, int skip = 0)
    {
        writer.WriteVUInt32((uint)prefixLength);
        writer.WriteBlock(keys[0].AsSpan(0, prefixLength));
        var end = 0;
        for (var i = skip; i < keys.Length; i++)
        {
            end += keys[i].Length - prefixLength;
            writer.WriteUInt16LE((ushort)end);
        }

        for (var i = skip; i < keys.Length; i++)
        {
            writer.WriteBlock(keys[i].AsSpan(prefixLength));
        }
    }
}
