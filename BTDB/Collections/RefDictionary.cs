// Idea taken from DictionarySlim in .NetCore with following license:
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;

namespace BTDB.Collections;

/// <summary>
/// RefDictionaryTKey, TValue> is similar to Dictionary<TKey, TValue> but optimized in three ways:
/// 1) It allows access to the value by ref replacing the common TryGetValue and Add pattern.
/// 2) It does not store the hash code (assumes it is cheap to equate values).
/// 3) It does not accept an equality comparer (assumes Object.GetHashCode() and Object.Equals() or overridden implementation are cheap and sufficient).
/// 4) Have GetOrFakeValue method
/// 5) Index Enumerator allowing to get value by ref ValueRef and KeyRef methods
/// </summary>
[DebuggerTypeProxy(typeof(RefDictionaryDebugView<,>))]
[DebuggerDisplay("Count = {Count}")]
public class RefDictionary<TKey, TValue> : IReadOnlyCollection<KeyValuePair<TKey, TValue>> where TKey : IEquatable<TKey>
{
    // We want to initialize without allocating arrays. We also want to avoid null checks.
    // Array.Empty would give divide by zero in modulo operation. So we use static one element arrays.
    // The first add will cause a resize replacing these with real arrays of power of 2 elements.
    // Arrays are wrapped in a class to avoid being duplicated for each <TKey, TValue>
    static readonly Entry[] InitialEntries = new Entry[1];

    int _count;
    // 0-based index into _entries of head of free chain: -1 means empty
    int _freeList = -1;
    // 1-based index into _entries; 0 means empty
    int[] _buckets;
    Entry[] _entries;

    TValue _fake;

    [DebuggerDisplay("({key}, {value})->{next}")]
    struct Entry
    {
        public TKey key;
        public TValue value;
        // 0-based index of next entry in chain: -1 means end of chain
        // also encodes whether this entry _itself_ is part of the free list by changing sign and subtracting 3,
        // so -2 means end of free list, -3 means index 0 but on free list, -4 means index 1 but on free list, etc.
        public int next;
    }

    public RefDictionary()
    {
        _buckets = HashHelpers.RefDictionarySizeOneIntArray;
        _entries = InitialEntries;
    }

    public RefDictionary(int capacity)
    {
        if (capacity < 0)
            HashHelpers.ThrowCapacityArgumentOutOfRangeException();
        if (capacity < 2)
            capacity = 2;
        capacity = HashHelpers.PowerOf2(capacity);
        _buckets = new int[capacity];
        _entries = new Entry[capacity];
    }

    public int Count => _count;

    public int Capacity => _entries.Length;

    public bool ContainsKey(TKey key)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var collisionCount = 0;
        for (var i = _buckets[key.GetHashCode() & (_buckets.Length - 1)] - 1;
                (uint)i < (uint)entries.Length; i = entries[i].next)
        {
            if (key.Equals(entries[i].key))
                return true;
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

    public bool TryGetValue(TKey key, out TValue value)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var collisionCount = 0;
        for (var i = _buckets[key.GetHashCode() & (_buckets.Length - 1)] - 1;
                (uint)i < (uint)entries.Length; i = entries[i].next)
        {
            if (key.Equals(entries[i].key))
            {
                value = entries[i].value;
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
        var bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
        var entryIndex = _buckets[bucketIndex] - 1;

        var lastIndex = -1;
        var collisionCount = 0;
        while (entryIndex != -1)
        {
            ref var candidate = ref entries[entryIndex];
            if (candidate.key.Equals(key))
            {
                if (lastIndex != -1)
                {   // Fixup preceding element in chain to point to next (if any)
                    entries[lastIndex].next = candidate.next;
                }
                else
                {   // Fixup bucket to new head (if any)
                    _buckets[bucketIndex] = candidate.next + 1;
                }

                entries[entryIndex] = default;

                entries[entryIndex].next = -3 - _freeList; // New head of free list
                _freeList = entryIndex;

                _count--;
                return true;
            }
            lastIndex = entryIndex;
            entryIndex = candidate.next;

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

    [return: MaybeNull]
    public ref TValue GetOrFakeValueRef(TKey key)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var collisionCount = 0;
        var bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
        for (var i = _buckets[bucketIndex] - 1;
                (uint)i < (uint)entries.Length; i = entries[i].next)
        {
            if (key.Equals(entries[i].key))
                return ref entries[i].value;
            if (collisionCount == entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }
            collisionCount++;
        }

        return ref _fake;
    }

    public ref TValue GetOrFakeValueRef(TKey key, out bool found)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var collisionCount = 0;
        var bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
        for (var i = _buckets[bucketIndex] - 1;
                (uint)i < (uint)entries.Length; i = entries[i].next)
        {
            if (key.Equals(entries[i].key))
            {
                found = true;
                return ref entries[i].value;
            }
            if (collisionCount == entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }
            collisionCount++;
        }

        found = false;
        return ref _fake;
    }

    // Not safe for concurrent _reads_ (at least, if either of them add)
    public ref TValue GetOrAddValueRef(TKey key)
    {
        if (key == null) HashHelpers.ThrowKeyArgumentNullException();
        var entries = _entries;
        var collisionCount = 0;
        var bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
        for (var i = _buckets[bucketIndex] - 1;
                (uint)i < (uint)entries.Length; i = entries[i].next)
        {
            if (key.Equals(entries[i].key))
                return ref entries[i].value;
            if (collisionCount == entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }
            collisionCount++;
        }

        return ref AddKey(key, bucketIndex);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ref TValue AddKey(TKey key, int bucketIndex)
    {
        var entries = _entries;
        int entryIndex;
        if (_freeList != -1)
        {
            entryIndex = _freeList;
            _freeList = -3 - entries[_freeList].next;
        }
        else
        {
            if (_count == entries.Length || entries.Length == 1)
            {
                entries = Resize();
                bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
                // entry indexes were not changed by Resize
            }
            entryIndex = _count;
        }

        entries[entryIndex].key = key;
        entries[entryIndex].next = _buckets[bucketIndex] - 1;
        _buckets[bucketIndex] = entryIndex + 1;
        _count++;
        return ref entries[entryIndex].value;
    }

    Entry[] Resize()
    {
        var count = _count;
        var newSize = _entries.Length * 2;
        if ((uint)newSize > (uint)int.MaxValue) // uint cast handles overflow
            throw new InvalidOperationException("Capacity overflow");

        var entries = new Entry[newSize];
        Array.Copy(_entries, 0, entries, 0, count);

        var newBuckets = new int[entries.Length];
        while (count-- > 0)
        {
            var bucketIndex = entries[count].key.GetHashCode() & (newBuckets.Length - 1);
            entries[count].next = newBuckets[bucketIndex] - 1;
            newBuckets[bucketIndex] = count + 1;
        }

        _buckets = newBuckets;
        _entries = entries;

        return entries;
    }

    public void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
    {
        if (array == null)
            throw new ArgumentNullException("array");
        // Let the runtime validate the index

        var entries = _entries;
        var i = 0;
        var count = _count;
        while (count > 0)
        {
            var entry = entries[i];
            if (entry.next > -2) // part of free list?
            {
                count--;
                array[index++] = new KeyValuePair<TKey, TValue>(
                    entry.key,
                    entry.value);
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
        readonly RefDictionary<TKey, TValue> _dictionary;
        int _index;
        int _count;
        KeyValuePair<TKey, TValue> _current;

        internal Enumerator(RefDictionary<TKey, TValue> dictionary)
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

            while (_dictionary._entries[_index].next < -1)
                _index++;

            _current = new KeyValuePair<TKey, TValue>(
                _dictionary._entries[_index].key,
                _dictionary._entries[_index++].value);
            return true;
        }

        public KeyValuePair<TKey, TValue> Current => _current;

        object IEnumerator.Current => _current;

        void IEnumerator.Reset()
        {
            _index = 0;
            _count = _dictionary._count;
        }

        public void Dispose() { }
    }


    public ref readonly TKey KeyRef(uint index)
    {
        return ref _entries[(int)index].key;
    }


    // Key should not be mutated in way it will change its hash
    public ref TKey DangerousKeyRef(uint index)
    {
        return ref _entries[(int)index].key;
    }

    public ref TValue ValueRef(uint index)
    {
        return ref _entries[(int)index].value;
    }

    public IndexEnumerator Index { get => new IndexEnumerator(this); }

    public struct IndexEnumerator : IEnumerable<uint>
    {
        RefDictionary<TKey, TValue> _owner;

        public IndexEnumerator(RefDictionary<TKey, TValue> owner)
        {
            _owner = owner;
        }

        public FastEnumerator GetEnumerator() => new FastEnumerator(_owner);

        IEnumerator IEnumerable.GetEnumerator() => new FastEnumerator(_owner);

        IEnumerator<uint> IEnumerable<uint>.GetEnumerator() => new FastEnumerator(_owner);

        public struct FastEnumerator : IEnumerator<uint>
        {
            readonly RefDictionary<TKey, TValue> _dictionary;
            int _index;
            int _count;
            uint _current;

            internal FastEnumerator(RefDictionary<TKey, TValue> dictionary)
            {
                _dictionary = dictionary;
                _index = 0;
                _count = _dictionary._count;
                _current = 0;
            }

            public bool MoveNext()
            {
                if (_count == 0)
                {
                    return false;
                }

                _count--;

                while (_dictionary._entries[_index].next < -1)
                    _index++;

                _current = (uint)_index++;
                return true;
            }

            public uint Current => _current;

            object IEnumerator.Current => _current;

            void IEnumerator.Reset()
            {
                _index = 0;
                _count = _dictionary._count;
            }

            public void Dispose() { }
        }

    }
}

sealed class RefDictionaryDebugView<K, V> where K : IEquatable<K>
{
    readonly RefDictionary<K, V> _dictionary;

    public RefDictionaryDebugView(RefDictionary<K, V> dictionary)
    {
        _dictionary = dictionary ?? throw new ArgumentNullException(nameof(dictionary));
    }

    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public KeyValuePair<K, V>[] Items
    {
        get
        {
            return _dictionary.ToArray();
        }
    }
}
