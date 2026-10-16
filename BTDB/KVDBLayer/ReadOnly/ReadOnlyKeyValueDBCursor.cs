using System;
using System.Collections.Generic;
using System.IO;
using BTDB.Buffer;
using BTDB.Collections;

namespace BTDB.KVDBLayer.ReadOnly;

sealed class ReadOnlyKeyValueDBCursor : IKeyValueDBCursor
{
    internal struct Node
    {
        internal bool IsLeaf;
        internal int ChildCount;
        uint _prefixOffset;
        uint _prefixLength;
        uint _keyEndsOffset;
        uint _suffixesOffset;
        uint _pointersOffset;
        internal int Pos;

        internal int KeyCount => IsLeaf ? ChildCount : ChildCount - 1;
        internal uint PrefixLength => _prefixLength;

        internal static Node Read(ReadOnlyKeyValueDB db, uint offset)
        {
            var begin = db.Begin;
            var end = (ulong)db.TotalLength - 4;
            if (offset >= end) throw Corrupted();
            var first = ReadOnlyFormat.Span(begin + offset, 1)[0];
            var node = new Node
            {
                IsLeaf = first < ReadOnlyFormat.BranchFlag, ChildCount = first & ~ReadOnlyFormat.BranchFlag
            };
            if (node.ChildCount == 0 || offset + 6ul > end) throw Corrupted();
            node._prefixLength = ReadOnlyFormat.ReadVUInt32(begin + offset + 1, out var lengthOfLength);
            node._prefixOffset = offset + 1 + lengthOfLength;
            var keyEndsOffset = (ulong)node._prefixOffset + node._prefixLength;
            var suffixesOffset = keyEndsOffset + 2ul * (uint)node.KeyCount;
            if (suffixesOffset > end) throw Corrupted();
            node._keyEndsOffset = (uint)keyEndsOffset;
            node._suffixesOffset = (uint)suffixesOffset;
            var suffixesLength = node.KeyCount == 0 ? 0 : node.SuffixEnd(begin, node.KeyCount - 1);
            var pointersOffset = suffixesOffset + suffixesLength;
            if (pointersOffset + (node.IsLeaf ? 4ul : 8ul) * (uint)node.ChildCount > end) throw Corrupted();
            node._pointersOffset = (uint)pointersOffset;
            return node;
        }

        uint SuffixEnd(nuint begin, int index) => ReadOnlyFormat.ReadUInt16LE(begin + _keyEndsOffset + 2u * (uint)index);

        internal ReadOnlySpan<byte> Prefix(nuint begin) => ReadOnlyFormat.Span(begin + _prefixOffset, _prefixLength);

        internal ReadOnlySpan<byte> Suffix(nuint begin, int index)
        {
            var start = index == 0 ? 0 : SuffixEnd(begin, index - 1);
            var end = SuffixEnd(begin, index);
            if (end < start) throw Corrupted();
            return ReadOnlyFormat.Span(begin + _suffixesOffset + start, end - start);
        }

        internal uint Pointer(nuint begin, int index) =>
            ReadOnlyFormat.ReadUInt32LE(begin + _pointersOffset + 4u * (uint)index);

        internal uint CountUpTo(nuint begin, int index) =>
            ReadOnlyFormat.ReadUInt32LE(begin + _pointersOffset + 4u * (uint)(ChildCount + index));

        internal uint CountBefore(nuint begin, int index) => index == 0 ? 0 : CountUpTo(begin, index - 1);
    }

    readonly ReadOnlyKeyValueDBTransaction _transaction;
    readonly ReadOnlyKeyValueDB _db;
    // Path from the root to the leaf with current key, empty when the cursor is not valid
    StructList<Node> _stack;
    long _keyIndex = -1;
    bool _disposed;

    public ReadOnlyKeyValueDBCursor(ReadOnlyKeyValueDBTransaction transaction)
    {
        _transaction = transaction;
        _db = transaction.Db;
    }

    static InvalidDataException Corrupted() => new("Corrupted read only database");

    void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _transaction.ThrowIfDisposed();
    }

    public void Dispose()
    {
        _disposed = true;
        _stack.Clear();
    }

    public IKeyValueDBTransaction Transaction => _transaction;

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        ThrowIfDisposed();
        var index = FindBoundary(prefix, false);
        if (PositionAt(index) && KeyHasPrefix(prefix)) return true;
        Invalidate();
        return false;
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        ThrowIfDisposed();
        var index = FindBoundary(prefix, true);
        if (index > 0 && PositionBefore(index) && KeyHasPrefix(prefix)) return true;
        Invalidate();
        return false;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        ThrowIfDisposed();
        if (!IsValid()) return FindLastKey(prefix);
        if (MoveTo(_keyIndex - 1) && KeyHasPrefix(prefix)) return true;
        Invalidate();
        return false;
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        ThrowIfDisposed();
        if (!IsValid()) return FindFirstKey(prefix);
        if (MoveTo(_keyIndex + 1) && KeyHasPrefix(prefix)) return true;
        Invalidate();
        return false;
    }

    public FindResult Find(scoped in ReadOnlySpan<byte> key, uint prefixLen)
    {
        ThrowIfDisposed();
        var index = FindBoundary(key, false);
        FindResult result;
        if (PositionAt(index) && CompareCurrentKey(key, int.MaxValue) == 0)
            result = FindResult.Exact;
        else if (index > 0 && PositionBefore(index))
            result = FindResult.Previous;
        else if (SeekIndex(0))
            result = FindResult.Next;
        else
            return FindResult.NotFound;

        if (prefixLen == 0) return result;
        var keyPrefix = key[..(int)prefixLen];
        switch (result)
        {
            case FindResult.Previous when !KeyHasPrefix(keyPrefix):
                if (MoveTo(_keyIndex + 1) && KeyHasPrefix(keyPrefix)) return FindResult.Next;
                Invalidate();
                return FindResult.NotFound;
            case FindResult.Next when !KeyHasPrefix(keyPrefix):
                // FindResult.Previous is preferred that's why it has to be NotFound when next does not match prefix
                Invalidate();
                return FindResult.NotFound;
            default:
                return result;
        }
    }

    public long GetKeyIndex() => IsValid() ? _keyIndex : -1;

    public bool FindKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        ThrowIfDisposed();
        if (!FindFirstKey(prefix)) return false;
        if (MoveTo(_keyIndex + index) && KeyHasPrefix(prefix)) return true;
        Invalidate();
        return false;
    }

    public bool FindKeyIndex(long index)
    {
        ThrowIfDisposed();
        return SeekIndex(index);
    }

    public bool KeyHasPrefix(in ReadOnlySpan<byte> prefix) =>
        IsValid() && CompareCurrentKey(prefix, prefix.Length) == 0;

    public void Invalidate()
    {
        _stack.Clear();
        _keyIndex = -1;
    }

    public bool IsValid() => _stack.Count > 0;

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        if (!IsValid()) return new();
        return new((uint)CurrentKeyLength(), (uint)CurrentValue().Length);
    }

    public ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy = false)
    {
        if (!IsValid()) return new();
        var length = CurrentKeyLength();
        if (buffer.Length < length) buffer = GC.AllocateUninitializedArray<byte>(length);
        CopyCurrentKey(buffer.Span);
        return buffer[..length];
    }

    public ReadOnlySpan<byte> GetKeySpan(scoped ref Span<byte> buffer, bool copy = false)
    {
        if (!IsValid()) return new();
        ref var leaf = ref _stack.Last;
        // Keys without node prefix are stored whole and the memory never changes
        if (leaf.PrefixLength == 0 && !copy) return leaf.Suffix(_db.Begin, leaf.Pos);
        var length = CurrentKeyLength();
        if (buffer.Length < length) buffer = GC.AllocateUninitializedArray<byte>(length);
        CopyCurrentKey(buffer);
        return buffer[..length];
    }

    public ReadOnlySpan<byte> GetKeySpan(Span<byte> buffer, bool copy = false) => GetKeySpan(ref buffer, copy);

    public bool IsValueCorrupted() => false;

    public ReadOnlyMemory<byte> GetValueMemory(ref Memory<byte> buffer, bool copy = false)
    {
        if (!IsValid()) return new();
        var value = CurrentValue();
        if (value.Length == 0) return ReadOnlyMemory<byte>.Empty;
        if (!copy) return new UnmanagedMemoryManager<byte>(value).Memory;
        if (buffer.Length < value.Length) buffer = GC.AllocateUninitializedArray<byte>(value.Length);
        value.CopyTo(buffer.Span);
        return buffer[..value.Length];
    }

    public ReadOnlySpan<byte> GetValueSpan(scoped ref Span<byte> buffer, bool copy = false)
    {
        ThrowIfDisposed();
        if (!IsValid()) throw new InvalidOperationException("Current key is not valid");
        var value = CurrentValue();
        if (!copy) return value;
        if (buffer.Length < value.Length) buffer = GC.AllocateUninitializedArray<byte>(value.Length);
        value.CopyTo(buffer);
        return buffer[..value.Length];
    }

    public void SetValue(in ReadOnlySpan<byte> value) => throw ReadOnlyKeyValueDB.ReadOnlyException();

    public void EraseCurrent() => throw ReadOnlyKeyValueDB.ReadOnlyException();

    public long EraseUpTo(IKeyValueDBCursor to) => throw ReadOnlyKeyValueDB.ReadOnlyException();

    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value) =>
        throw ReadOnlyKeyValueDB.ReadOnlyException();

    public UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen) =>
        throw ReadOnlyKeyValueDB.ReadOnlyException();

    public void FastIterate(ref Span<byte> buffer, CursorIterateCallback callback)
    {
        ThrowIfDisposed();
        if (!IsValid() && !FindFirstKey([])) return;
        var begin = _db.Begin;
        while (true)
        {
            ref var leaf = ref _stack.Last;
            var prefix = leaf.Prefix(begin);
            for (; leaf.Pos < leaf.ChildCount; leaf.Pos++, _keyIndex++)
            {
                var suffix = leaf.Suffix(begin, leaf.Pos);
                var length = prefix.Length + suffix.Length;
                if (buffer.Length < length)
                    buffer = GC.AllocateUninitializedArray<byte>(Math.Max(length, buffer.Length * 2));
                prefix.CopyTo(buffer);
                suffix.CopyTo(buffer[prefix.Length..]);
                if (callback(_keyIndex, buffer[..length])) return;
            }

            // _keyIndex is now the index of the first key of the next leaf
            if (!SeekIndex(_keyIndex)) return;
        }
    }

    // Number of keys before the boundary, which is the first key not smaller than target, or with includePrefixed
    // the first key which is bigger than target and does not start with it. The stack ends in the leaf of the
    // boundary, possibly positioned after its last key.
    long FindBoundary(in ReadOnlySpan<byte> target, bool includePrefixed)
    {
        Invalidate();
        if (_db.RootOffset == 0) return 0;
        var begin = _db.Begin;
        var maxLength = includePrefixed ? target.Length : int.MaxValue;
        var before = 0L;
        var node = Node.Read(_db, _db.RootOffset);
        while (true)
        {
            var prefix = node.Prefix(begin);
            var l = 0;
            var r = node.KeyCount;
            while (l < r)
            {
                var m = (l + r) / 2;
                var comparison = Compare(prefix, node.Suffix(begin, m), target, maxLength);
                if (comparison < 0 || includePrefixed && comparison == 0) l = m + 1;
                else r = m;
            }

            node.Pos = l;
            _stack.Add(node);
            if (node.IsLeaf) return before + l;
            before += node.CountBefore(begin, l);
            node = Node.Read(_db, node.Pointer(begin, l));
        }
    }

    // Positions on the key with index found by FindBoundary
    bool PositionAt(long index)
    {
        if (_stack.Count > 0 && _stack.Last.Pos < _stack.Last.ChildCount)
        {
            _keyIndex = index;
            return true;
        }

        return SeekIndex(index);
    }

    // Positions on the key before the one with index found by FindBoundary
    bool PositionBefore(long index)
    {
        if (_stack.Count > 0 && _stack.Last.Pos > 0)
        {
            _stack.Last.Pos--;
            _keyIndex = index - 1;
            return true;
        }

        return SeekIndex(index - 1);
    }

    bool MoveTo(long index)
    {
        if (_stack.Count > 0)
        {
            ref var leaf = ref _stack.Last;
            var pos = leaf.Pos + (index - _keyIndex);
            if (pos >= 0 && pos < leaf.ChildCount)
            {
                leaf.Pos = (int)pos;
                _keyIndex = index;
                return true;
            }
        }

        return SeekIndex(index);
    }

    bool SeekIndex(long index)
    {
        Invalidate();
        if (index < 0 || index >= _db.KeyValueCount) return false;
        var begin = _db.Begin;
        var remaining = (uint)index;
        var node = Node.Read(_db, _db.RootOffset);
        while (!node.IsLeaf)
        {
            var l = 0;
            var r = node.ChildCount - 1;
            while (l < r)
            {
                var m = (l + r) / 2;
                if (node.CountUpTo(begin, m) > remaining) r = m;
                else l = m + 1;
            }

            remaining -= node.CountBefore(begin, l);
            node.Pos = l;
            _stack.Add(node);
            node = Node.Read(_db, node.Pointer(begin, l));
        }

        if (remaining >= node.ChildCount) throw Corrupted();
        node.Pos = (int)remaining;
        _stack.Add(node);
        _keyIndex = index;
        return true;
    }

    int CurrentKeyLength()
    {
        ref var leaf = ref _stack.Last;
        return (int)leaf.PrefixLength + leaf.Suffix(_db.Begin, leaf.Pos).Length;
    }

    void CopyCurrentKey(Span<byte> target)
    {
        ref var leaf = ref _stack.Last;
        var prefix = leaf.Prefix(_db.Begin);
        prefix.CopyTo(target);
        leaf.Suffix(_db.Begin, leaf.Pos).CopyTo(target[prefix.Length..]);
    }

    int CompareCurrentKey(in ReadOnlySpan<byte> target, int maxLength)
    {
        ref var leaf = ref _stack.Last;
        return Compare(leaf.Prefix(_db.Begin), leaf.Suffix(_db.Begin, leaf.Pos), target, maxLength);
    }

    ReadOnlySpan<byte> CurrentValue()
    {
        ref var leaf = ref _stack.Last;
        var offset = leaf.Pointer(_db.Begin, leaf.Pos);
        if (offset == 0) return [];
        var end = (ulong)_db.TotalLength - 4;
        if (offset + 1ul > end) throw Corrupted();
        var length = ReadOnlyFormat.ReadVUInt32(_db.Begin + offset, out var lengthOfLength);
        if ((ulong)offset + lengthOfLength + length > end) throw Corrupted();
        return ReadOnlyFormat.Span(_db.Begin + offset + lengthOfLength, length);
    }

    // Compares key made of prefix and suffix, cut to maxLength bytes, with target
    static int Compare(in ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> suffix, in ReadOnlySpan<byte> target,
        int maxLength)
    {
        if (prefix.Length >= maxLength) return prefix[..maxLength].SequenceCompareTo(target);
        var common = Math.Min(prefix.Length, target.Length);
        var comparison = prefix[..common].SequenceCompareTo(target[..common]);
        if (comparison != 0) return comparison;
        // Target is shorter than node prefix and equal to its start, so the key is bigger
        if (common < prefix.Length) return 1;
        var rest = maxLength - prefix.Length;
        if (suffix.Length > rest) suffix = suffix[..rest];
        return suffix.SequenceCompareTo(target[prefix.Length..]);
    }
}
