using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface IOrderedSet<T> : ISet<T>
{
    IEnumerable<T> GetReverseEnumerator();
    IEnumerable<T> GetIncreasingEnumerator(T start);
    IEnumerable<T> GetDecreasingEnumerator(T start);

    long RemoveRange(AdvancedEnumeratorParam<T> param);
    IEnumerable<T> GetAdvancedEnumerator(AdvancedEnumeratorParam<T> param);
}

public class OrderedSet<T> : HashSet<T>, IOrderedSet<T>
{
    public IEnumerable<T> GetReverseEnumerator()
    {
        throw new System.NotSupportedException();
    }

    public IEnumerable<T> GetIncreasingEnumerator(T start)
    {
        throw new System.NotSupportedException();
    }

    public IEnumerable<T> GetDecreasingEnumerator(T start)
    {
        throw new System.NotSupportedException();
    }

    public long RemoveRange(AdvancedEnumeratorParam<T> param)
    {
        throw new System.NotSupportedException();
    }

    public IEnumerable<T> GetAdvancedEnumerator(AdvancedEnumeratorParam<T> param)
    {
        throw new System.NotSupportedException();
    }
}
