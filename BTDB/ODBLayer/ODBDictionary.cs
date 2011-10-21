using System;
using System.Collections;
using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public class ODBDictionary<K,V> : IDictionary<K,V>
    {
        int _count;

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<K, V> item)
        {
            Add(item.Key,item.Value);
        }

        public void Clear()
        {
            _count = 0;
        }

        public bool Contains(KeyValuePair<K, V> item)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(KeyValuePair<K, V>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<K, V> item)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { return _count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool ContainsKey(K key)
        {
            throw new NotImplementedException();
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }

        public bool Remove(K key)
        {
            throw new NotImplementedException();
        }

        public bool TryGetValue(K key, out V value)
        {
            throw new NotImplementedException();
        }

        public V this[K key]
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public ICollection<K> Keys
        {
            get { throw new NotImplementedException(); }
        }

        public ICollection<V> Values
        {
            get { throw new NotImplementedException(); }
        }
    }
}