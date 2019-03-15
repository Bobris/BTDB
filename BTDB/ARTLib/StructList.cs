using System;
using System.Runtime.CompilerServices;

namespace BTDB.ARTLib
{
    public struct StructList<T>
    {
        T[] _a;
        uint _count;

        public StructList(StructList<T> from) : this()
        {
            if (from.Count > 0)
            {
                _count = from.Count;
                _a = new T[_count];
                Array.Copy(from._a, _a, _count);
            }
        }

        public void Add(in T value)
        {
            if (_a == null || _count == _a.Length)
            {
                Expand();
            }
            _a[_count++] = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref T Add()
        {
            if (_a == null || _count == _a.Length)
            {
                Expand();
            }
            return ref _a[_count++];
        }

        void Expand()
        {
            Array.Resize(ref _a, (int)Math.Min(int.MaxValue, Math.Max(2u, _count * 2)));
        }

        public ref T this[uint index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (index >= _count)
                    ThrowIndexOutOfRange(index);
                return ref _a[index];
            }
        }

        void ThrowIndexOutOfRange(uint index)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, "List has " + _count + " items. Accessing " + index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            _count = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        public uint Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan()
        {
            return _a.AsSpan(0, (int)_count);
        }

        public Span<T> AsSpan(int start)
        {
            return AsSpan().Slice(start);
        }

        public Span<T> AsSpan(int start, int length)
        {
            return AsSpan().Slice(start, length);
        }
    }
}
