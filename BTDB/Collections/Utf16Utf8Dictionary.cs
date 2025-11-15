using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace BTDB.Collections;

public class Utf16Utf8Dictionary<TValue>
{
    // 0-based index into _entries, -1 means empty
    readonly BucketEntry[] _buckets;
    readonly Entry[] _entries;
    readonly byte[] _bytes;
    readonly char[] _chars;

    struct BucketEntry
    {
        public int I8;
        public int I16;
    }

    struct Entry
    {
        public int Next8;
        public int Hash8;
        public int Next16;
        public int Hash16;
        public uint Length8;
        public uint Offset8;
        public uint Length16;
        public uint Offset16;
        public TValue Value;
    }

    public Utf16Utf8Dictionary(int count, IEnumerable<string> keys)
    {
        var bucketCount = HashHelpers.PowerOf2(count);
        _buckets = new BucketEntry[bucketCount];
        for (int i = 0; i < bucketCount; i++)
        {
            _buckets[i] = new BucketEntry() { I8 = -1, I16 = -1 };
        }

        _entries = new Entry[count];
        _bytes = [];
        var bytesUsed = 0;
        _chars = [];
        var charsUsed = 0;
        var entryIndex = 0;
        foreach (var key in keys)
        {
            var key8 = new byte[Encoding.UTF8.GetByteCount(key)];
            Encoding.UTF8.GetBytes(key, key8);
            var hash8 = CalcHash8(key8);
            var hash16 = CalcHash16(key);
            var bucketIndex = hash8 & (bucketCount - 1);
            ref var entry = ref _entries[entryIndex];
            entry.Hash8 = hash8;
            entry.Length8 = (uint)key8.Length;
            entry.Offset8 = (uint)bytesUsed;
            if (_bytes.Length < bytesUsed + key8.Length)
            {
                Array.Resize(ref _bytes,
                    Math.Min(Array.MaxLength, Math.Max(_bytes.Length * 2, bytesUsed + key8.Length)));
            }

            key8.CopyTo(_bytes, bytesUsed);
            bytesUsed += key8.Length;
            entry.Next8 = _buckets[bucketIndex].I8;
            _buckets[bucketIndex].I8 = entryIndex;
            bucketIndex = hash16 & (bucketCount - 1);
            entry.Hash16 = hash16;
            entry.Length16 = (uint)key.Length;
            entry.Offset16 = (uint)charsUsed;
            if (_chars.Length < charsUsed + key.Length)
            {
                Array.Resize(ref _chars,
                    Math.Min(Array.MaxLength, Math.Max(_chars.Length * 2, charsUsed + key.Length)));
            }

            key.CopyTo(_chars.AsSpan(charsUsed));
            charsUsed += key.Length;
            entry.Next16 = _buckets[bucketIndex].I16;
            _buckets[bucketIndex].I16 = entryIndex;
#if DEBUG
            for (var i = entry.Next16; i >= 0; i = _entries[i].Next16)
            {
                if (Equal16(key, hash16, _entries[i]))
                {
                    throw new ArgumentException("Duplicate key " + key);
                }
            }
#endif
            entryIndex++;
        }

        if (entryIndex != count)
            throw new ArgumentException("Count mismatch");

        Array.Resize(ref _bytes, bytesUsed);
        Array.Resize(ref _chars, charsUsed);
    }

    static int CalcHash8(in ReadOnlySpan<byte> key)
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(key);
        return hashCode.ToHashCode();
    }

    static int CalcHash16(in ReadOnlySpan<char> key)
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(MemoryMarshal.AsBytes(key));
        return hashCode.ToHashCode();
    }

    bool Equal8(in ReadOnlySpan<byte> key, int hash, in Entry entry)
    {
        if (hash != entry.Hash8)
            return false;
        if (key.Length != entry.Length8)
            return false;
        return _bytes.AsSpan((int)entry.Offset8, (int)entry.Length8).SequenceEqual(key);
    }

    bool Equal16(in ReadOnlySpan<char> key, int hash, in Entry entry)
    {
        if (hash != entry.Hash16)
            return false;
        if (key.Length != entry.Length16)
            return false;
        return _chars.AsSpan((int)entry.Offset16, (int)entry.Length16).SequenceEqual(key);
    }

    public int Count => _entries.Length;

    public bool ContainsKey(ReadOnlySpan<char> key)
    {
        var hash = CalcHash16(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I16; i >= 0; i = entries[i].Next16)
        {
            if (Equal16(key, hash, entries[i]))
                return true;
        }

        return false;
    }

    public bool ContainsKey(ReadOnlySpan<byte> key)
    {
        var hash = CalcHash8(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I8; i >= 0; i = entries[i].Next8)
        {
            if (Equal8(key, hash, entries[i]))
                return true;
        }

        return false;
    }

    public ReadOnlySpan<byte> KeyUtf8(uint index)
    {
        return _bytes.AsSpan((int)_entries[(int)index].Offset8, (int)_entries[(int)index].Length8);
    }

    public ReadOnlySpan<char> KeyUtf16(uint index)
    {
        return _chars.AsSpan((int)_entries[(int)index].Offset16, (int)_entries[(int)index].Length16);
    }

    public ref TValue ValueRef(uint index)
    {
        return ref _entries[(int)index].Value;
    }

    public int GetIndex(ReadOnlySpan<char> key)
    {
        var hash = CalcHash16(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I16; i >= 0; i = entries[i].Next16)
        {
            if (Equal16(key, hash, entries[i]))
                return i;
        }

        return -1;
    }

    public int GetIndex(ReadOnlySpan<byte> key)
    {
        var hash = CalcHash8(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I8; i >= 0; i = entries[i].Next8)
        {
            if (Equal8(key, hash, entries[i]))
                return i;
        }

        return -1;
    }

    public bool TryGetValue(ReadOnlySpan<char> key, out TValue value)
    {
        var hash = CalcHash16(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I16; i >= 0; i = entries[i].Next16)
        {
            if (Equal16(key, hash, entries[i]))
            {
                value = entries[i].Value;
                return true;
            }
        }

        value = default!;
        return false;
    }

    public bool TryGetValue(ReadOnlySpan<byte> key, out TValue value)
    {
        var hash = CalcHash8(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I8; i >= 0; i = entries[i].Next8)
        {
            if (Equal8(key, hash, entries[i]))
            {
                value = entries[i].Value;
                return true;
            }
        }

        value = default!;
        return false;
    }

    public ref TValue GetValueRef(ReadOnlySpan<char> key, out bool found)
    {
        var hash = CalcHash16(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I16; i >= 0; i = entries[i].Next16)
        {
            if (Equal16(key, hash, entries[i]))
            {
                found = true;
                return ref entries[i].Value;
            }
        }

        found = false;
        return ref Unsafe.NullRef<TValue>();
    }

    public ref TValue GetValueRef(ReadOnlySpan<byte> key, out bool found)
    {
        var hash = CalcHash8(key);
        var entries = _entries;
        for (var i = _buckets[hash & (_buckets.Length - 1)].I8; i >= 0; i = entries[i].Next8)
        {
            if (Equal8(key, hash, entries[i]))
            {
                found = true;
                return ref entries[i].Value;
            }
        }

        found = false;
        return ref Unsafe.NullRef<TValue>();
    }

    public IndexEnumerator Index => new IndexEnumerator(this);

    public struct IndexEnumerator : IEnumerable<uint>
    {
        Utf16Utf8Dictionary<TValue> _owner;

        public IndexEnumerator(Utf16Utf8Dictionary<TValue> owner)
        {
            _owner = owner;
        }

        public FastEnumerator GetEnumerator() => new FastEnumerator(_owner);

        IEnumerator IEnumerable.GetEnumerator() => new FastEnumerator(_owner);

        IEnumerator<uint> IEnumerable<uint>.GetEnumerator() => new FastEnumerator(_owner);

        public struct FastEnumerator : IEnumerator<uint>
        {
            readonly uint _count;
            uint _index;
            uint _current;

            internal FastEnumerator(Utf16Utf8Dictionary<TValue> dictionary)
            {
                _index = 0;
                _count = (uint)dictionary._entries.Length;
            }

            public bool MoveNext()
            {
                if (_index < _count)
                {
                    _current = _index++;
                    return true;
                }

                return false;
            }

            public uint Current => _current;

            object IEnumerator.Current => _current;

            public void Reset()
            {
                _index = 0;
            }

            public void Dispose()
            {
            }
        }
    }
}
