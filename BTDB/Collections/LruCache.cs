using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;

// ReSharper disable ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract

namespace BTDB.Collections;

[DebuggerTypeProxy(typeof(LruCacheDebugView<,>))]
[DebuggerDisplay("Count = {Count}, Capacity = {Capacity}")]
public class LruCache<TKey, TValue> : IReadOnlyCollection<KeyValuePair<TKey, TValue>> where TKey : IEquatable<TKey>
{
    int _count;
    readonly int _maxCapacity;

    // 0-based index into _entries of head of a free chain: -1 means empty
    int _freeList = -1;

    int _usageHead = -1;
    int _usageTail = -1;

    // 1-based index into _entries; 0 means empty
    int[] _buckets;
    Entry[] _entries;

    [DebuggerDisplay("({Key}, {Value})->{Next} P:{UsagePrev} N:{UsageNext}")]
    struct Entry
    {
        public int Hash;

        // 0-based index of next entry in the chain: -1 means end of the chain
        // also encodes whether this entry _itself_ is part of the free list by changing sign and subtracting 3,
        // so -2 means end of a free list, -3 means index 0 but on free list, -4 means index 1 but on free list, etc.
        public int Next;
        public int UsagePrev;
        public int UsageNext;
        public TKey Key;
        public TValue Value;
    }

    /// <summary>
    /// Last Recently Used Cache with maximum capacity and gradually increasing capacity to specified maximum.
    /// It uses GetHashCode of TKey to distribute keys into buckets.
    /// </summary>
    /// <param name="capacity">Must be at least 1, and it will be modified to next power of 2</param>
    /// <param name="startingCapacity">How many buckets it will allocate at start</param>
    public LruCache(int capacity = 64, int startingCapacity = 16)
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
                if (_usageHead != i)
                {
                    if (_usageTail == i)
                    {
                        _usageTail = entries[i].UsagePrev;
                        entries[_usageTail].UsageNext = -1;
                    }
                    else
                    {
                        entries[entries[i].UsageNext].UsagePrev = entries[i].UsagePrev;
                        entries[entries[i].UsagePrev].UsageNext = entries[i].UsageNext;
                    }

                    entries[i].UsagePrev = -1;
                    entries[i].UsageNext = _usageHead;
                    entries[_usageHead].UsagePrev = i;
                    _usageHead = i;
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

        value = default!;
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
                if (_usageHead == entryIndex)
                {
                    _usageHead = candidate.UsageNext;
                    if (_usageHead != -1)
                        entries[_usageHead].UsagePrev = -1;
                }
                else
                {
                    entries[candidate.UsagePrev].UsageNext = candidate.UsageNext;
                    if (candidate.UsageNext != -1)
                        entries[candidate.UsageNext].UsagePrev = candidate.UsagePrev;
                }

                if (_usageTail == entryIndex)
                    _usageTail = candidate.UsagePrev;
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

    public void Clear()
    {
        Array.Clear(_buckets);
        Array.Clear(_entries, 0, _count);
        _count = 0;
        _freeList = -1;
        _usageHead = -1;
        _usageTail = -1;
    }

    public ref TValue this[TKey key] => ref GetOrAddValueRef(key, out _);

    // Not safe for concurrent _reads_ (at least, if either of them add)
    public ref TValue GetOrAddValueRef(TKey key, out bool added)
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
                if (_usageHead != i)
                {
                    if (_usageTail == i)
                    {
                        _usageTail = entries[i].UsagePrev;
                        entries[_usageTail].UsageNext = -1;
                    }
                    else
                    {
                        entries[entries[i].UsageNext].UsagePrev = entries[i].UsagePrev;
                        entries[entries[i].UsagePrev].UsageNext = entries[i].UsageNext;
                    }

                    entries[i].UsagePrev = -1;
                    entries[i].UsageNext = _usageHead;
                    entries[_usageHead].UsagePrev = i;
                    _usageHead = i;
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
                entryIndex = _usageTail;
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
                        _usageTail = candidate.UsagePrev;
                        entries[_usageTail].UsageNext = -1;
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

        entries[entryIndex].Hash = hash;
        entries[entryIndex].Key = key;
        entries[entryIndex].Next = _buckets[bucketIndex] - 1;
        entries[entryIndex].UsagePrev = -1;
        entries[entryIndex].UsageNext = _usageHead;
        if (_usageHead != -1)
            entries[_usageHead].UsagePrev = entryIndex;
        _usageHead = entryIndex;
        if (_usageTail == -1)
            _usageTail = entryIndex;
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
