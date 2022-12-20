using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.ReadOnly;

public class ReadOnlyKeyValueDBTransaction : IKeyValueDBTransaction
{
    readonly ReadOnlyKeyValueDB _owner;
    bool _disposed;
    int _keyIndex = -2; // -2 = Not valid, -1 = Unknown
    long _cursorMovedCounter = 0;
    StructList<StackItem> _cursorStack;

    struct StackItem
    {
        internal uint _offset;
        internal byte _directChildren;
        internal byte _pos;
        internal bool _isLeaf;
        internal uint _recursiveChildren;
        internal uint _prefixOffset;
        internal uint _prefixLen;
        internal uint _firstKeyOffset;
        internal uint _valuePtrOffset;
    }

    public ReadOnlyKeyValueDBTransaction(ReadOnlyKeyValueDB owner)
    {
        _owner = owner;
        CreatedTime = DateTime.UtcNow;
    }

    public void Dispose()
    {
        _disposed = true;
    }

    public IKeyValueDB Owner => _owner;
    public DateTime CreatedTime { get; }
    public string? DescriptionForLeaks { get; set; }

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        throw new NotImplementedException();
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        throw new NotImplementedException();
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        throw new NotImplementedException();
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        throw new NotImplementedException();
    }

    public FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        throw new NotImplementedException();
    }

    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        throw new NotSupportedException();
    }

    public long GetKeyValueCount()
    {
        return (long)_owner._keyValueCount;
    }

    public long GetKeyIndex()
    {
        if (_keyIndex == -2) return -1;
        if (_keyIndex == -1)
        {
            _keyIndex = _cursorStack.Last._pos;
            for (var i = 0; i + 1 < _cursorStack.Count; i++)
            {
                ref var item = ref _cursorStack[i];
                for (var j = (int)item._pos; j-- > 0;)
                {
                    _keyIndex += (int)ReadRecursiveChildrenCount(item, (byte)j);
                }
            }
        }

        return _keyIndex;
    }

    public bool SetKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        if (!FindFirstKey(prefix)) return false;
        index += GetKeyIndex();
        if (!SetKeyIndex(index)) return false;
        _keyIndex = (int)index;
        if (KeyMatchesPrefix(prefix)) return true;
        InvalidateCurrentKey();
        return false;
    }

    bool KeyMatchesPrefix(ReadOnlySpan<byte> prefix)
    {
        if (_keyIndex == -2) return false;
        ref var top = ref _cursorStack.Last;
        Debug.Assert(top._isLeaf);
        var ownerBegin = _owner._begin;

        var nodePrefix = GetPrefixSpan(ownerBegin, top);
        if (nodePrefix.Length >= prefix.Length)
        {
            return nodePrefix[..prefix.Length].SequenceEqual(prefix);
        }

        if (nodePrefix.Length > 0)
        {
            if (!nodePrefix.SequenceEqual(prefix[..nodePrefix.Length]))
                return false;
            prefix = prefix[nodePrefix.Length..];
        }

        return TreeNodeUtils.IsPrefix(GetKeySuffixSpan(ownerBegin, top, top._pos), prefix);
    }

    static ReadOnlySpan<byte> GetKeySuffixSpan(nuint ownerBegin, in StackItem top, uint topPos)
    {
        var keyOffsetsPtr = ownerBegin + top._prefixOffset + top._prefixLen;
        if (topPos == 0)
        {
            return CreateSpan(ownerBegin + top._firstKeyOffset, ReadUInt16LE(keyOffsetsPtr));
        }

        var start = ReadUInt16LE(keyOffsetsPtr + 2 * topPos - 2);
        var end = ReadUInt16LE(keyOffsetsPtr + 2 * topPos);

        return CreateSpan(ownerBegin + top._firstKeyOffset + start, end - start);
    }

    static unsafe ReadOnlySpan<byte> GetPrefixSpan(nuint ownerBegin, in StackItem top)
    {
        return MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.Add(ref Unsafe.AsRef<byte>(ownerBegin.ToPointer()), top._prefixOffset), (int)top._prefixLen);
    }

    public bool SetKeyIndex(long index)
    {
        InvalidateCurrentKey();
        if (index < 0 || index >= _owner._keyValueCount) return false;
        if (_cursorStack.Count == 0) SinkTo(_owner._rootNodeOffset);
        var idx = (uint)index;
        ref var top = ref _cursorStack.Last;
        while (!top._isLeaf)
        {
            if (idx >= top._recursiveChildren)
            {
                return false;
            }

            var count = top._directChildren;
            var pos = 0u;
            while (pos < top._directChildren)
            {
                var chc = ReadRecursiveChildrenCount(top, pos);
                if (chc < idx) break;
                idx -= chc;
                pos++;
            }

            SinkToPos(pos);
            top = ref _cursorStack.Last;
        }

        top._pos = (byte)idx;
        _keyIndex = (int)index;
        return true;
    }

    public void InvalidateCurrentKey()
    {
        _cursorMovedCounter++;
        _keyIndex = -2;
        if (_cursorStack.Count > 1) _cursorStack.SetCount(1);
    }

    public bool IsValidKey()
    {
        return _keyIndex != -2;
    }

    public ReadOnlySpan<byte> GetKey()
    {
        throw new NotImplementedException();
    }

    public byte[] GetKeyToArray()
    {
        throw new NotImplementedException();
    }

    public ReadOnlySpan<byte> GetKey(scoped ref byte buffer, int bufferLength)
    {
        throw new NotImplementedException();
    }

    public ReadOnlySpan<byte> GetClonedValue(ref byte buffer, int bufferLength)
    {
        throw new NotImplementedException();
    }

    public ReadOnlySpan<byte> GetValue()
    {
        throw new NotImplementedException();
    }

    public bool IsValueCorrupted()
    {
        return false;
    }

    public void SetValue(in ReadOnlySpan<byte> value)
    {
        throw new NotSupportedException();
    }

    public void EraseCurrent()
    {
        throw new NotSupportedException();
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey)
    {
        throw new NotSupportedException();
    }

    public bool EraseCurrent(in ReadOnlySpan<byte> exactKey, ref byte buffer, int bufferLength,
        out ReadOnlySpan<byte> value)
    {
        throw new NotSupportedException();
    }

    public void EraseAll()
    {
        throw new NotSupportedException();
    }

    public void EraseRange(long firstKeyIndex, long lastKeyIndex)
    {
        throw new NotSupportedException();
    }

    public bool IsWriting()
    {
        return false;
    }

    public bool IsReadOnly()
    {
        return true;
    }

    public bool IsDisposed()
    {
        return _disposed;
    }

    public ulong GetCommitUlong()
    {
        return _owner._commitUlong;
    }

    public void SetCommitUlong(ulong value)
    {
        throw new NotSupportedException();
    }

    public uint GetUlongCount()
    {
        return (uint)_owner._ulongs.Length;
    }

    public ulong GetUlong(uint idx)
    {
        return idx >= _owner._ulongs.Length ? 0 : _owner._ulongs[idx];
    }

    public void SetUlong(uint idx, ulong value)
    {
        throw new NotSupportedException();
    }

    public void NextCommitTemporaryCloseTransactionLog()
    {
    }

    public void Commit()
    {
    }

    public long GetTransactionNumber()
    {
        return 0;
    }

    public long CursorMovedCounter => _cursorMovedCounter;

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        if (_keyIndex==-2) return new();
        ref var top = ref _cursorStack.Last;
        Debug.Assert(top._isLeaf);
        var begin = _owner._begin;
        var keyLen = top._prefixLen + (uint)GetKeySuffixSpan(begin, top, top._pos).Length;
        var valueLen = GetValueLen(begin, top, top._pos);
        return new(keyLen, valueLen);
    }

    public bool RollbackAdvised { get; set; }

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats()
    {
        throw new NotSupportedException();
    }

    void SinkTo(uint offset)
    {
        ref var item = ref _cursorStack.AddRef();
        item._offset = offset;
        var ownerBegin = _owner._begin;
        var reader = new SpanReader(CreateSpan(ownerBegin + offset, 32)); // 32 is bigger than needed
        var firstByte = reader.ReadUInt8();
        if (firstByte >= 128)
        {
            item._isLeaf = false;
            item._pos = 255;
            item._directChildren = (byte)(firstByte - 128);
            item._recursiveChildren = reader.ReadVUInt32();
            item._prefixLen = reader.ReadVUInt32();
            item._prefixOffset = offset + reader.GetCurrentPositionWithoutController();
            item._firstKeyOffset = (uint)(item._prefixOffset + item._prefixLen +
                2 * item._directChildren - 2);
            item._valuePtrOffset = item._firstKeyOffset +
                                   ReadUInt16LE(ownerBegin + (item._firstKeyOffset - 2));
        }
        else
        {
            item._isLeaf = true;
            item._pos = 255;
            item._directChildren = firstByte;
            item._recursiveChildren = firstByte;
            item._prefixLen = reader.ReadVUInt32();
            item._prefixOffset = offset + reader.GetCurrentPositionWithoutController();
            item._firstKeyOffset = (uint)(item._prefixOffset + item._prefixLen +
                                          2 * item._directChildren);
            item._valuePtrOffset = item._firstKeyOffset +
                                   ReadUInt16LE(ownerBegin + (item._firstKeyOffset - 2));
        }
    }

    void SinkToPos(uint pos)
    {
        ref var item = ref _cursorStack.Last;
        item._pos = (byte)pos;
        SinkTo(ReadUInt32LE(_owner._begin + item._valuePtrOffset + pos * 4));
    }

    static uint GetValueLen(nuint ownerBegin, in StackItem top, uint topPos)
    {
        var ofs = ReadUInt32LE(ownerBegin + top._valuePtrOffset + topPos * 4);
        if (ofs == 0) return 0;

        return (uint)PackUnpack.UnsafeUnpackVUInt(ownerBegin + ofs);
    }

    uint ReadRecursiveChildrenCount(in StackItem top, uint pos)
    {
        var ownerBegin = _owner._begin;
        var offset = ReadUInt32LE(ownerBegin + top._valuePtrOffset + pos * 4);
        var reader = new SpanReader(CreateSpan(ownerBegin + offset, 32)); // 32 is bigger than needed
        var firstByte = reader.ReadUInt8();
        if (firstByte >= 128)
        {
            return reader.ReadVUInt32();
        }
        else
        {
            return firstByte;
        }
    }

    static unsafe uint ReadUInt16LE(nuint offset)
    {
        return BitConverter.IsLittleEndian
            ? Unsafe.ReadUnaligned<ushort>(offset.ToPointer())
            : BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<ushort>(offset.ToPointer()));
    }

    static unsafe uint ReadUInt32LE(nuint offset)
    {
        return BitConverter.IsLittleEndian
            ? Unsafe.ReadUnaligned<uint>(offset.ToPointer())
            : BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(offset.ToPointer()));
    }

    static unsafe ReadOnlySpan<byte> CreateSpan(nuint start, uint len)
    {
        if (len > int.MaxValue) len = int.MaxValue;
        return MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.AsRef<byte>(start.ToPointer()), (int)len);
    }
}
