using System;
using System.Collections;
using System.Collections.Generic;

namespace BTDB.EventStoreLayer
{
    public class ReadOnlyListArrayWrapper<T> : IReadOnlyList<T>
    {
        readonly T[] _array;

        public ReadOnlyListArrayWrapper(T[] array)
        {
            _array = array;
        }

        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return _array[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count { get; set; }

        public T this[int index]
        {
            get
            {
                if (index >= Count) throw new ArgumentOutOfRangeException("index");
                return _array[index];
            }
        }
    }
}