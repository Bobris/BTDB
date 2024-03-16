// Idea taken from DictionarySlim in .NetCore with following license:
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace BTDB.Collections;

/// <summary>
/// SpanByteNoRemoveDictionary<TValue> is Dictionary with Key of ReadOnlySpan<byte>, allow only find and add. For simplicity it is not possible to remove keys
/// </summary>
[DebuggerTypeProxy(typeof(SpanByteNoRemoveDictionaryDebugView<>))]
[DebuggerDisplay("Count = {Count}")]
public class SpanByteNoRemoveDictionary<TValue> : IReadOnlyCollection<KeyValuePair<byte[], TValue>>
{
    // We want to initialize without allocating arrays. We also want to avoid null checks.
    // Array.Empty would give divide by zero in modulo operation. So we use static one element arrays.
    // The first add will cause a resize replacing these with real arrays of power of 2 elements.
    // Arrays are wrapped in a class to avoid being duplicated for each <TKey, TValue>
    static readonly Entry[] InitialEntries = new Entry[1];

    int _count;

    // 0-based index into _entries of head of free chain: -1 means empty
    int _freeList = -1;

    int _bytesUsed = 0;

    // 1-based index into _entries; 0 means empty
    int[] _buckets;
    Entry[] _entries;
    byte[] _bytes = [];

    [DebuggerDisplay("({length}:{offset}:{hash}, {value})->{next}")]
    struct Entry
    {
        // 0-based index of next entry in chain: -1 means end of chain
        // also encodes whether this entry _itself_ is part of the free list by changing sign and subtracting 3,
        // so -2 means end of free list, -3 means index 0 but on free list, -4 means index 1 but on free list, etc.
        public int next;
        public int hash;
        public uint length;
        public uint offset;
        public TValue value;
    }

    public SpanByteNoRemoveDictionary()
    {
        _buckets = HashHelpers.RefDictionarySizeOneIntArray;
        _entries = InitialEntries;
    }

    public SpanByteNoRemoveDictionary(int capacity)
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

    public bool ContainsKey(in ReadOnlySpan<byte> key)
    {
        var hash = CalcHash(key);
        var entries = _entries;
        var collisionCount = 0;
        for (var i = _buckets[hash & (_buckets.Length - 1)] - 1;
             (uint)i < (uint)entries.Length;
             i = entries[i].next)
        {
            if (Equal(key, hash, entries[i]))
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

    bool Equal(in ReadOnlySpan<byte> key, int hash, in Entry entry)
    {
        if (hash != entry.hash)
            return false;
        if (key.Length != entry.length)
            return false;
        return _bytes.AsSpan((int)entry.offset, (int)entry.length).SequenceEqual(key);
    }

    static int CalcHash(in ReadOnlySpan<byte> key)
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(key);
        return hashCode.ToHashCode();
    }

    public bool TryGetValue(in ReadOnlySpan<byte> key, out TValue value)
    {
        var hash = CalcHash(key);
        var entries = _entries;
        var collisionCount = 0;
        for (var i = _buckets[hash & (_buckets.Length - 1)] - 1;
             (uint)i < (uint)entries.Length;
             i = entries[i].next)
        {
            if (Equal(key, hash, entries[i]))
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

    // Not safe for concurrent _reads_ (at least, if either of them add)
    public ref TValue GetOrAddValueRef(in ReadOnlySpan<byte> key)
    {
        var hash = CalcHash(key);
        var entries = _entries;
        var collisionCount = 0;
        var bucketIndex = hash & (_buckets.Length - 1);
        for (var i = _buckets[bucketIndex] - 1;
             (uint)i < (uint)entries.Length;
             i = entries[i].next)
        {
            if (Equal(key, hash, entries[i]))
                return ref entries[i].value;
            if (collisionCount == entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }

            collisionCount++;
        }

        return ref AddKey(key, hash, bucketIndex);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    ref TValue AddKey(in ReadOnlySpan<byte> key, int hash, int bucketIndex)
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
                bucketIndex = hash & (_buckets.Length - 1);
                // entry indexes were not changed by Resize
            }

            entryIndex = _count;
        }

        ref var entry = ref entries[entryIndex];
        entry.hash = hash;
        entry.length = (uint)key.Length;
        entry.offset = (uint)_bytesUsed;
        if (_bytes.Length < _bytesUsed + key.Length)
        {
            Array.Resize(ref _bytes, Math.Min(Array.MaxLength, Math.Max(_bytes.Length * 2, _bytesUsed + key.Length)));
        }

        key.CopyTo(_bytes.AsSpan(_bytesUsed));
        _bytesUsed += key.Length;
        entry.next = _buckets[bucketIndex] - 1;
        _buckets[bucketIndex] = entryIndex + 1;
        _count++;
        return ref entries[entryIndex].value;
    }

    Entry[] Resize()
    {
        var count = _count;
        var newSize = _entries.Length * 2;
        if ((uint)newSize > int.MaxValue) // uint cast handles overflow
            throw new InvalidOperationException("Capacity overflow");

        var entries = new Entry[newSize];
        Array.Copy(_entries, 0, entries, 0, count);

        var newBuckets = new int[entries.Length];
        while (count-- > 0)
        {
            var bucketIndex = entries[count].hash & (newBuckets.Length - 1);
            entries[count].next = newBuckets[bucketIndex] - 1;
            newBuckets[bucketIndex] = count + 1;
        }

        _buckets = newBuckets;
        _entries = entries;

        return entries;
    }

    public void CopyTo(KeyValuePair<byte[], TValue>[] array, int index)
    {
        ArgumentNullException.ThrowIfNull(array);
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
                array[index++] = new(
                    entry.length == 0 ? [] : _bytes.AsSpan((int)entry.offset, (int)entry.length).ToArray(),
                    entry.value);
            }

            i++;
        }
    }

    public Enumerator GetEnumerator() => new Enumerator(this); // avoid boxing

    IEnumerator<KeyValuePair<byte[], TValue>> IEnumerable<KeyValuePair<byte[], TValue>>.GetEnumerator() =>
        new Enumerator(this);

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

    public struct Enumerator : IEnumerator<KeyValuePair<byte[], TValue>>
    {
        readonly SpanByteNoRemoveDictionary<TValue> _dictionary;
        int _index;
        int _count;
        KeyValuePair<byte[], TValue> _current;

        internal Enumerator(SpanByteNoRemoveDictionary<TValue> dictionary)
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

            _current = new KeyValuePair<byte[], TValue>(
                _dictionary._bytes.AsSpan((int)_dictionary._entries[_index].offset,
                    (int)_dictionary._entries[_index].length).ToArray(),
                _dictionary._entries[_index++].value);
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

    public TValue this[ReadOnlySpan<byte> key]
    {
        get
        {
            if (TryGetValue(key, out var value)) return value;
            throw new ArgumentException("Key not found");
        }
        set => GetOrAddValueRef(key) = value;
    }
}

sealed class SpanByteNoRemoveDictionaryDebugView<V>
{
    readonly SpanByteNoRemoveDictionary<V> _dictionary;

    public SpanByteNoRemoveDictionaryDebugView(SpanByteNoRemoveDictionary<V> dictionary)
    {
        _dictionary = dictionary ?? throw new ArgumentNullException(nameof(dictionary));
    }

    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public KeyValuePair<string, V>[] Items
    {
        get
        {
            return _dictionary.Select(p => new KeyValuePair<string, V>(Encoding.UTF8.GetString(p.Key), p.Value))
                .ToArray();
        }
    }
}
