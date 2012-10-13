using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public interface IOrderedDictionary<TKey, TValue> : IDictionary<TKey,TValue>
    {
        IEnumerable<KeyValuePair<TKey, TValue>> GetReverseEnumerator();
        IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start);
        IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start);
    }
}