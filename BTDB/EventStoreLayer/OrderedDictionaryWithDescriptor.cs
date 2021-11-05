using BTDB.ODBLayer;
using System;
using System.Collections.Generic;

namespace BTDB.EventStoreLayer;

public class OrderedDictionaryWithDescriptor<TK, TV> : DictionaryWithDescriptor<TK, TV>, IOrderedDictionary<TK, TV>
{
    public OrderedDictionaryWithDescriptor(int capacity, ITypeDescriptor descriptor)
        : base(capacity, descriptor)
    {
    }

    public IOrderedDictionaryEnumerator<TK, TV> GetAdvancedEnumerator(AdvancedEnumeratorParam<TK> param)
        => throw new NotSupportedException();

    public IEnumerable<KeyValuePair<TK, TV>> GetDecreasingEnumerator(TK start)
        => throw new NotSupportedException();

    public IEnumerable<KeyValuePair<TK, TV>> GetIncreasingEnumerator(TK start)
        => throw new NotSupportedException();

    public IEnumerable<KeyValuePair<TK, TV>> GetReverseEnumerator()
        => throw new NotSupportedException();

    public long RemoveRange(TK start, bool includeStart, TK end, bool includeEnd)
        => throw new NotSupportedException();
}
