using System;

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
                Array.Resize(ref _a, (int)Math.Min(int.MaxValue, Math.Max(2u, _count * 2)));
            }
            _a[_count++] = value;
        }

        public ref T Add()
        {
            if (_a == null || _count == _a.Length)
            {
                Array.Resize(ref _a, (int)Math.Min(int.MaxValue, Math.Max(2u, _count * 2)));
            }
            return ref _a[_count++];
        }

        public ref T this[uint index]
        {
            get
            {
                if (index >= _count)
                    throw new ArgumentOutOfRangeException(nameof(index), index, "List has " + _count + " items. Accessing " + index);
                return ref _a[index];
            }
        }

        public void Clear()
        {
            _count = 0;
        }

        public void Pop()
        {
            if (_count == 0)
                throw new InvalidOperationException("Cannot pop empty List");
            _count--;
        }

        public uint Count { get => _count; }

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
