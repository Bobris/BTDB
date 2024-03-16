using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace BTDB.Collections;

[DebuggerTypeProxy(typeof(SpanByteLruCacheDebugView<>))]
[DebuggerDisplay("Count = {Count}, Capacity = {Capacity}")]
public class SpanByteLruCache<TValue> : IReadOnlyCollection<KeyValuePair<byte[], TValue>>
{
    int _count;
    readonly int _maxCapacity;

    // 0-based index into _entries of head of free chain: -1 means empty
    int _freeList = -1;

    int UsageHead = -1;
    int UsageTail = -1;

    // 1-based index into _entries; 0 means empty
    int[] _buckets;
    Entry[] _entries;

    int _usedBytes = 0;
    int _freeBytes = 0;

    byte[] _bytes = Array.Empty<byte>();

    [DebuggerDisplay("({Key}, {Value})->{Next} P:{UsagePrev} N:{UsageNext}")]
    struct Entry
    {
        public int Hash;

        // 0-based index of next entry in chain: -1 means end of chain
        // also encodes whether this entry _itself_ is part of the free list by changing sign and subtracting 3,
        // so -2 means end of free list, -3 means index 0 but on free list, -4 means index 1 but on free list, etc.
        public int Next;
        public int UsagePrev;
        public int UsageNext;
        public int Offset;
        public int Length;
        public TValue Value;
    }

    /// <summary>
    /// Last Recently Used Cache with maximum capacity and gradually increasing capacity to specified maximum.
    /// It uses GetHashCode of TKey to distribute keys into buckets.
    /// </summary>
    /// <param name="capacity">Must be at least 1, and it will be modified to next power of 2</param>
    /// <param name="startingCapacity">How many buckets it will allocate at start</param>
    public SpanByteLruCache(int capacity = 64, int startingCapacity = 16)
    {
        if (capacity < 1)
            HashHelpers.ThrowCapacityArgumentOutOfRangeException();
        if (capacity < 2)
            capacity = 2;
        _maxCapacity = HashHelpers.PowerOf2(capacity);
        if (startingCapacity > capacity) startingCapacity = capacity;
        if (startingCapacity < 2) startingCapacity = 2;
        capacity = HashHelpers.PowerOf2(startingCapacity);
        _buckets = new int[capacity];
        _entries = new Entry[capacity];
    }

    public int Count => _count;

    public int Capacity => _entries.Length;

    public int MaxCapacity => _maxCapacity;

    static int CalcHash(in ReadOnlySpan<byte> key)
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(key);
        return hashCode.ToHashCode();
    }

    bool Equal(in ReadOnlySpan<byte> key, int hash, in Entry entry)
    {
        if (hash != entry.Hash)
            return false;
        if (key.Length != entry.Length)
            return false;
        return _bytes.AsSpan(entry.Offset, entry.Length).SequenceEqual(key);
    }

    public bool TryGetValue(in ReadOnlySpan<byte> key, out TValue value)
    {
        var hash = CalcHash(key);
        var entries = _entries;
        var collisionCount = 0;
        for (var i = _buckets[hash & (_buckets.Length - 1)] - 1;
             (uint)i < (uint)entries.Length;
             i = entries[i].Next)
        {
            if (Equal(key, hash, entries[i]))
            {
                value = entries[i].Value;
                if (UsageHead != i)
                {
                    if (UsageTail == i)
                    {
                        UsageTail = entries[i].UsagePrev;
                        entries[UsageTail].UsageNext = -1;
                    }
                    else
                    {
                        entries[entries[i].UsageNext].UsagePrev = entries[i].UsagePrev;
                        entries[entries[i].UsagePrev].UsageNext = entries[i].UsageNext;
                    }

                    entries[i].UsagePrev = -1;
                    entries[i].UsageNext = UsageHead;
                    entries[UsageHead].UsagePrev = i;
                    UsageHead = i;
                }

                return true;
            }

            if (collisionCount == entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }

            collisionCount++;
        }

        value = default;
        return false;
    }

    public bool Remove(in ReadOnlySpan<byte> key)
    {
        var entries = _entries;
        var hash = CalcHash(key);
        var bucketIndex = hash & (_buckets.Length - 1);
        var entryIndex = _buckets[bucketIndex] - 1;

        var lastIndex = -1;
        var collisionCount = 0;
        while (entryIndex != -1)
        {
            ref var candidate = ref entries[entryIndex];
            if (Equal(key, hash, candidate))
            {
                _freeBytes += candidate.Length;
                if (UsageHead == entryIndex)
                {
                    UsageHead = candidate.UsageNext;
                    if (UsageHead != -1)
                        entries[UsageHead].UsagePrev = -1;
                }
                else
                {
                    entries[candidate.UsagePrev].UsageNext = candidate.UsageNext;
                    if (candidate.UsageNext != -1)
                        entries[candidate.UsageNext].UsagePrev = candidate.UsagePrev;
                }

                if (UsageTail == entryIndex)
                    UsageTail = candidate.UsagePrev;
                if (lastIndex != -1)
                {
                    // Fixup preceding element in chain to point to next (if any)
                    entries[lastIndex].Next = candidate.Next;
                }
                else
                {
                    // Fixup bucket to new head (if any)
                    _buckets[bucketIndex] = candidate.Next + 1;
                }

                entries[entryIndex] = default;

                entries[entryIndex].Next = -3 - _freeList; // New head of free list
                _freeList = entryIndex;

                _count--;
                return true;
            }

            lastIndex = entryIndex;
            entryIndex = candidate.Next;

            if (collisionCount == entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }

            collisionCount++;
        }

        return false;
    }

    public ref TValue this[in ReadOnlySpan<byte> key] => ref GetOrAddValueRef(key, out _);

    // Not safe for concurrent _reads_ (at least, if either of them add)
    public ref TValue GetOrAddValueRef(in ReadOnlySpan<byte> key, out bool added)
    {
        var entries = _entries;
        var collisionCount = 0;
        var hash = CalcHash(key);
        var bucketIndex = hash & (_buckets.Length - 1);
        for (var i = _buckets[bucketIndex] - 1;
             (uint)i < (uint)entries.Length;
             i = entries[i].Next)
        {
            if (Equal(key, hash, entries[i]))
            {
                if (UsageHead != i)
                {
                    if (UsageTail == i)
                    {
                        UsageTail = entries[i].UsagePrev;
                        entries[UsageTail].UsageNext = -1;
                    }
                    else
                    {
                        entries[entries[i].UsageNext].UsagePrev = entries[i].UsagePrev;
                        entries[entries[i].UsagePrev].UsageNext = entries[i].UsageNext;
                    }

                    entries[i].UsagePrev = -1;
                    entries[i].UsageNext = UsageHead;
                    entries[UsageHead].UsagePrev = i;
                    UsageHead = i;
                }

                added = false;
                return ref entries[i].Value;
            }

            if (collisionCount == entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }

            collisionCount++;
        }

        added = true;
        return ref AddKey(key, bucketIndex, hash);
    }

    public void Clear()
    {
        Array.Clear(_buckets);
        Array.Clear(_entries, 0, _count);
        _count = 0;
        _freeList = -1;
        UsageHead = -1;
        UsageTail = -1;
        _usedBytes = 0;
        _freeBytes = 0;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ref TValue AddKey(in ReadOnlySpan<byte> key, int bucketIndex, int hash)
    {
        var entries = _entries;
        int entryIndex;
        if (_freeList != -1)
        {
            entryIndex = _freeList;
            _freeList = -3 - entries[_freeList].Next;
        }
        else
        {
            if (_count < entries.Length)
            {
                entryIndex = _count;
            }
            else if (_count < _maxCapacity)
            {
                entryIndex = _count;
                IncreaseCapacity(_count * 2);
                entries = _entries;
                bucketIndex = hash & (_buckets.Length - 1);
            }
            else
            {
                entryIndex = UsageTail;
                var oldhash = entries[entryIndex].Hash;
                var oldbucketIndex = oldhash & (_buckets.Length - 1);
                var oldentryIndex = _buckets[oldbucketIndex] - 1;

                var lastIndex = -1;
                var collisionCount = 0;
                while (oldentryIndex != -1)
                {
                    ref var candidate = ref entries[oldentryIndex];
                    if (oldentryIndex == entryIndex)
                    {
                        // there are always at least two entries, so this is always valid
                        UsageTail = candidate.UsagePrev;
                        entries[UsageTail].UsageNext = -1;
                        if (lastIndex != -1)
                        {
                            // Fixup preceding element in chain to point to next (if any)
                            entries[lastIndex].Next = candidate.Next;
                        }
                        else
                        {
                            // Fixup bucket to new head (if any)
                            _buckets[oldbucketIndex] = candidate.Next + 1;
                        }

                        entries[entryIndex] = default;
                        _count--;
                        break;
                    }

                    lastIndex = oldentryIndex;
                    oldentryIndex = candidate.Next;

                    if (collisionCount == entries.Length)
                    {
                        // The chain of entries forms a loop; which means a concurrent update has happened.
                        // Break out of the loop and throw, rather than looping forever.
                        HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
                    }

                    collisionCount++;
                }
            }
        }

        ref var entry = ref entries[entryIndex];
        entry.Hash = hash;
        entry.Length = key.Length;
        entry.Offset = _usedBytes;
        if (_bytes.Length < _usedBytes + key.Length)
        {
            if (_freeBytes >= _usedBytes >> 1 && _usedBytes - _freeBytes + key.Length <= _bytes.Length)
            {
                // compact
                entry.Next = -2;
                _usedBytes = 0;
                _freeBytes = 0;
                var newBytes = new byte[_bytes.Length];
                for (var i = 0; i < _count; i++)
                {
                    ref var e = ref entries[i];
                    if (e.Next < -1)
                        continue;

                    Array.Copy(_bytes, e.Offset, newBytes, _usedBytes, e.Length);
                    e.Offset = _usedBytes;
                    _usedBytes += e.Length;
                }

                _bytes = newBytes;
            }
            else
            {
                Array.Resize(ref _bytes,
                    Math.Min(Array.MaxLength, Math.Max(_bytes.Length * 2, _usedBytes + key.Length)));
            }
        }

        key.CopyTo(_bytes.AsSpan(_usedBytes));
        _usedBytes += key.Length;

        entry.Next = _buckets[bucketIndex] - 1;
        entry.UsagePrev = -1;
        entry.UsageNext = UsageHead;
        if (UsageHead != -1)
            entries[UsageHead].UsagePrev = entryIndex;
        UsageHead = entryIndex;
        if (UsageTail == -1)
            UsageTail = entryIndex;
        _buckets[bucketIndex] = entryIndex + 1;
        _count++;
        return ref entry.Value;
    }

    public void IncreaseCapacity(int newCapacity)
    {
        if (newCapacity < _buckets.Length) return;
        newCapacity = HashHelpers.PowerOf2(newCapacity);
        var count = _count;

        var entries = new Entry[newCapacity];
        Array.Copy(_entries, 0, entries, 0, count);

        var newBuckets = new int[entries.Length];
        while (count-- > 0)
        {
            var bucketIndex = entries[count].Hash & (newBuckets.Length - 1);
            entries[count].Next = newBuckets[bucketIndex] - 1;
            newBuckets[bucketIndex] = count + 1;
        }

        _buckets = newBuckets;
        _entries = entries;
    }

    public Enumerator GetEnumerator() => new Enumerator(this); // avoid boxing

    IEnumerator<KeyValuePair<byte[], TValue>> IEnumerable<KeyValuePair<byte[], TValue>>.GetEnumerator() =>
        new Enumerator(this);

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

    public struct Enumerator : IEnumerator<KeyValuePair<byte[], TValue>>
    {
        readonly SpanByteLruCache<TValue> _dictionary;
        int _index;
        int _count;
        KeyValuePair<byte[], TValue> _current;

        internal Enumerator(SpanByteLruCache<TValue> dictionary)
        {
            _dictionary = dictionary;
            _index = 0;
            _count = _dictionary._count;
            _current = default;
        }

        public bool MoveNext()
        {
            if (_count == 0)
            {
                _current = default;
                return false;
            }

            _count--;

            while (_dictionary._entries[_index].Next < -1)
                _index++;

            ref var entry = ref _dictionary._entries[_index++];
            _current = new(
                _dictionary._bytes.AsSpan(entry.Offset, entry.Length).ToArray(),
                entry.Value);
            return true;
        }

        public KeyValuePair<byte[], TValue> Current => _current;

        object IEnumerator.Current => _current;

        void IEnumerator.Reset()
        {
            _index = 0;
            _count = _dictionary._count;
        }

        public void Dispose()
        {
        }
    }
}

sealed class SpanByteLruCacheDebugView<V>(SpanByteLruCache<V> lruCache)
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public KeyValuePair<string, V>[] Items =>
        lruCache.Select(v => new KeyValuePair<string, V>(Encoding.UTF8.GetString(v.Key), v.Value)).ToArray();
}
