using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;

namespace BTDB.Collections;

[DebuggerTypeProxy(typeof(LruCacheDebugView<,>))]
[DebuggerDisplay("Count = {Count}, Capacity = {Capacity}")]
public class LruCache<TKey, TValue> : IReadOnlyCollection<KeyValuePair<TKey, TValue>> where TKey : IEquatable<TKey>
{
    int _count;

    // 0-based index into _entries of head of free chain: -1 means empty
    int _freeList = -1;

    // 1-based index into _entries; 0 means empty
    int[] _buckets;
    int UsageHead = -1;
    int UsageTail = -1;
    Entry[] _entries;

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
        public TKey Key;
        public TValue Value;
    }

    public LruCache(int capacity)
    {
        if (capacity < 1)
            HashHelpers.ThrowCapacityArgumentOutOfRangeException();
        if (capacity < 2)
            capacity = 2;
        capacity = HashHelpers.PowerOf2(capacity);
        _buckets = new int[capacity];
        _entries = new Entry[capacity];
    }

    public int Count => _count;

    public int Capacity => _entries.Length;

    public bool TryGetValue(TKey key, out TValue value)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var collisionCount = 0;
        var hash = key.GetHashCode();
        for (var i = _buckets[hash & (_buckets.Length - 1)] - 1;
             (uint)i < (uint)entries.Length;
             i = entries[i].Next)
        {
            if (key.Equals(entries[i].Key))
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

    public bool Remove(TKey key)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var hash = key.GetHashCode();
        var bucketIndex = hash & (_buckets.Length - 1);
        var entryIndex = _buckets[bucketIndex] - 1;

        var lastIndex = -1;
        var collisionCount = 0;
        while (entryIndex != -1)
        {
            ref var candidate = ref entries[entryIndex];
            if (candidate.Key.Equals(key))
            {
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

    // Not safe for concurrent _reads_ (at least, if either of them add)
    public ref TValue GetOrAddValueRef(TKey key)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var collisionCount = 0;
        var hash = key.GetHashCode();
        var bucketIndex = hash & (_buckets.Length - 1);
        for (var i = _buckets[bucketIndex] - 1;
             (uint)i < (uint)entries.Length;
             i = entries[i].Next)
        {
            if (key.Equals(entries[i].Key))
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

        return ref AddKey(key, bucketIndex, hash);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ref TValue AddKey(TKey key, int bucketIndex, int hash)
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
            Debug.Assert(_count == entries.Length);
            entryIndex = UsageTail;
            // there are always at least two entries, so this is always valid
            UsageTail = entries[UsageTail].UsagePrev;
            entries[UsageTail].UsageNext = -1;
        }

        entries[entryIndex].Hash = hash;
        entries[entryIndex].Key = key;
        entries[entryIndex].Next = _buckets[bucketIndex] - 1;
        entries[entryIndex].UsagePrev = -1;
        entries[entryIndex].UsageNext = UsageHead;
        if (UsageHead != -1)
            entries[UsageHead].UsagePrev = entryIndex;
        UsageHead = entryIndex;
        _buckets[bucketIndex] = entryIndex + 1;
        _count++;
        return ref entries[entryIndex].Value;
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

    public void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
    {
        ArgumentNullException.ThrowIfNull(array);
        // Let the runtime validate the index

        var entries = _entries;
        var i = 0;
        var count = _count;
        while (count > 0)
        {
            var entry = entries[i];
            if (entry.Next > -2) // part of free list?
            {
                count--;
                array[index++] = new(entry.Key, entry.Value);
            }

            i++;
        }
    }

    public Enumerator GetEnumerator() => new Enumerator(this); // avoid boxing

    IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() =>
        new Enumerator(this);

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

    public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
    {
        readonly LruCache<TKey, TValue> _dictionary;
        int _index;
        int _count;
        KeyValuePair<TKey, TValue> _current;

        internal Enumerator(LruCache<TKey, TValue> dictionary)
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

            _current = new(
                _dictionary._entries[_index].Key,
                _dictionary._entries[_index++].Value);
            return true;
        }

        public KeyValuePair<TKey, TValue> Current => _current;

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

sealed class LruCacheDebugView<K, V>(LruCache<K, V> lruCache)
    where K : IEquatable<K>
{
    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public KeyValuePair<K, V>[] Items => lruCache.ToArray();
}
