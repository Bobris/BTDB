using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BTDB.Collections;

public struct StructList<T> : IEnumerable<T>
{
    T[]? _a;
    uint _count;

    public StructList(in StructList<T> from) : this()
    {
        if (from.Count > 0)
        {
            _count = from.Count;
            _a = new T[_count];
            Array.Copy(from._a!, _a, _count);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public StructList(T[]? backingArray, uint count)
    {
        _a = backingArray;
        _count = count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public void Add(in T value)
    {
        if (_a == null || _count == _a.Length)
        {
            Expand();
        }

        _a![_count++] = value;
    }

    /// <summary>
    /// Adds value to a collection only if it is not already contained in collection
    /// </summary>
    /// <param name="value"></param>
    public bool AddUnique(in T value)
    {
        for (var i = 0u; i < _count; i++)
        {
            if (value!.Equals(_a![i]))
            {
                return false;
            }
        }

        Add(value);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public ref T AddRef()
    {
        if (_a == null || _count == _a.Length)
        {
            Expand();
        }

        return ref _a![_count++];
    }

    public ref T Insert(uint index) => ref Insert((int)index);
    public ref T Insert(Index index) => ref Insert(index.GetOffset((int)_count));

    public ref T Insert(int index)
    {
        if ((uint)index > _count) ThrowIndexOutOfRange(index);
        if (_a == null || _count == _a.Length)
        {
            Expand();
        }

        _count++;
        if (index + 1 < _count)
        {
            AsSpan(index, (int)_count - index - 1)
                .CopyTo(AsSpan(index + 1, (int)(_count - index - 1)));
        }

        _a![index] = default!;
        return ref _a[index];
    }

    public void RemoveAt(Index index)
    {
        var idx = index.GetOffset((int)_count);
        if ((uint)idx >= _count) ThrowIndexOutOfRange(idx);
        AsSpan(idx + 1).CopyTo(AsSpan(idx));
        _count--;
        _a![_count] = default!;
    }

    public void RemoveItem(T item)
    {
        var index = IndexOf(item);
        if (index < 0) throw new ArgumentOutOfRangeException(nameof(item), item, "Item not found in list");
        RemoveAt(index);
    }

    public void Reserve(uint count)
    {
        if (count > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(count));
        if (count <= _count) return;
        Array.Resize(ref _a, (int)count);
    }

    public void Truncate()
    {
        if (_count == 0)
        {
            _a = null;
        }
        else
        {
            Array.Resize(ref _a, (int)_count);
        }
    }

    void Expand()
    {
        Array.Resize(ref _a, (int)Math.Min(int.MaxValue, Math.Max(2u, _count * 2)));
    }

    void Expand(uint count)
    {
        Array.Resize(ref _a, (int)Math.Min(int.MaxValue, Math.Max(count, _count * 2)));
    }

    public ref T this[int index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        get
        {
            if ((uint)index >= _count)
                ThrowIndexOutOfRange(index);
            return ref _a![index];
        }
    }

    public ref T this[uint index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        get
        {
            if (index >= _count)
                ThrowIndexOutOfRange(index);
            return ref _a![index];
        }
    }

    public ref T this[Index index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        get => ref this[index.GetOffset((int)_count)];
    }

    public Span<T> this[Range range]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        get => AsSpan()[range];
    }

    public ref T Last
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        get
        {
            if (_count == 0) ThrowEmptyList();
            return ref _a![_count - 1];
        }
    }

    void ThrowIndexOutOfRange(uint index)
    {
        throw new ArgumentOutOfRangeException(nameof(index), index,
            "List has " + _count + " items. Accessing " + index);
    }

    void ThrowIndexOutOfRange(int index)
    {
        throw new ArgumentOutOfRangeException(nameof(index), index,
            "List has " + _count + " items. Accessing " + index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear()
    {
        _count = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ClearAndTruncate()
    {
        _count = 0;
        _a = null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public void Pop()
    {
        if (_count == 0)
        {
            ThrowEmptyList();
        }

        _count--;
    }

    static void ThrowEmptyList()
    {
        throw new InvalidOperationException("Cannot pop empty List");
    }

    public readonly uint Count
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [DebuggerStepThrough]
        get => _count;
    }

    public T[]? UnsafeBackingArray => _a;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public Span<T> AsSpan()
    {
        return _a.AsSpan(0, (int)_count);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public static implicit operator ReadOnlySpan<T>(in StructList<T> value) => value.AsReadOnlySpan();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public readonly ReadOnlySpan<T> AsReadOnlySpan()
    {
        return _a.AsSpan(0, (int)_count);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public static implicit operator ReadOnlyMemory<T>(in StructList<T> value) => value.AsReadOnlyMemory();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public readonly ReadOnlyMemory<T> AsReadOnlyMemory()
    {
        return _a.AsMemory(0, (int)_count);
    }

    [DebuggerStepThrough]
    public Span<T> AsSpan(int start)
    {
        return AsSpan().Slice(start);
    }

    [DebuggerStepThrough]
    public readonly ReadOnlySpan<T> AsReadOnlySpan(int start)
    {
        return AsReadOnlySpan().Slice(start);
    }

    [DebuggerStepThrough]
    public Span<T> AsSpan(int start, int length)
    {
        return AsSpan().Slice(start, length);
    }

    [DebuggerStepThrough]
    public readonly ReadOnlySpan<T> AsReadOnlySpan(int start, int length)
    {
        return AsReadOnlySpan().Slice(start, length);
    }

    public struct Enumerator : IEnumerator<T>
    {
        int _position;
        int _count;
        T[] _array;

        public Enumerator(int count, T[] array)
        {
            _position = -1;
            _count = count;
            _array = array;
        }

        public bool MoveNext()
        {
            Debug.Assert(_position < _count);
            _position++;
            return _position < _count;
        }

        public void Reset()
        {
            _position = -1;
        }

        public readonly T Current => _array[_position];

        object IEnumerator.Current => Current!;

        public void Dispose()
        {
        }
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator((int)_count, _a!);
    }

    IEnumerator<T> IEnumerable<T>.GetEnumerator()
    {
        return new Enumerator((int)_count, _a!);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void TransferFrom(ref StructList<T> reference)
    {
        _a = reference._a;
        _count = reference._count;
        reference._a = null;
        reference._count = 0;
    }

    public void RepeatAdd(in T value, uint count)
    {
        if (count == 0) return;
        var totalCount = _count + count;
        if (_a == null || totalCount > _a.Length)
            Expand(totalCount);
        _a.AsSpan((int)_count, (int)count).Fill(value);
        _count = totalCount;
    }

    public void SetCount(uint count)
    {
        if (count <= _count) _count = count;
        else RepeatAdd(default, count - _count);
    }

    public void AddRange(in ReadOnlySpan<T> range)
    {
        if (range.IsEmpty)
            return;
        var count = _count + (uint)range.Length;
        if (_a == null || count > _a.Length)
            Expand(count);
        range.CopyTo(_a.AsSpan((int)_count));
        _count = count;
    }

    public readonly bool All(Predicate<T> predicate)
    {
        for (uint i = 0; i < _count; i++)
        {
            if (!predicate(_a![i]))
                return false;
        }

        return true;
    }

    public readonly int IndexOf(in T value)
    {
        var comparer = EqualityComparer<T>.Default;
        for (uint i = 0; i < _count; i++)
        {
            if (comparer.Equals(_a![i], value))
                return (int)i;
        }

        return -1;
    }

    public readonly T[] ToArray()
    {
        var res = new T[_count];
        AsReadOnlySpan().CopyTo(res);
        return res;
    }

    public void ReplaceItem(T originalItem, T newItem)
    {
        var index = IndexOf(originalItem);
        _a![index] = newItem;
    }

    public void ReplaceItem(T originalItem, in ReadOnlySpan<T> newItems)
    {
        var index = IndexOf(originalItem);
        if (index < 0)
            throw new ArgumentOutOfRangeException(nameof(originalItem), originalItem, "Item not found in List");
        var itemsToInsert = newItems.Length;
        if (itemsToInsert == 0)
        {
            RemoveAt(index);
            return;
        }

        if (itemsToInsert == 1)
        {
            _a![index] = newItems[0];
            return;
        }

        var totalCount = (uint)(_count + itemsToInsert - 1);
        if (totalCount > _a!.Length)
            Expand(totalCount);

        _count = totalCount;

        AsSpan(index + 1, (int)_count - (int)itemsToInsert - index).CopyTo(AsSpan((int)(index + itemsToInsert)));
        newItems.CopyTo(AsSpan(index));
    }

    public void InsertRange(Index index, in ReadOnlySpan<T> values)
    {
        var idx = index.GetOffset((int)_count);
        if ((uint)idx > _count) ThrowIndexOutOfRange(idx);
        if (values.IsEmpty) return;
        var totalCount = _count + (uint)values.Length;
        Expand(totalCount);
        _count = totalCount;
        AsSpan(idx, (int)_count - values.Length - idx).CopyTo(AsSpan(idx + values.Length));
        values.CopyTo(AsSpan(idx));
    }

    public void Sort(IComparer<T> comparer)
    {
        if (_count > 1)
            Array.Sort(_a!, 0, (int)_count, comparer);
    }
}
