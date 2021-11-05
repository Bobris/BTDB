using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace BTDB.ODBLayer;

public sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T>
{
    public static readonly ReferenceEqualityComparer<T> Instance;

    static ReferenceEqualityComparer()
    {
        Instance = new ReferenceEqualityComparer<T>();
    }

    ReferenceEqualityComparer()
    {
    }

    public bool Equals(T x, T y)
    {
        return ReferenceEquals(x, y);
    }

    public int GetHashCode(T obj)
    {
        return RuntimeHelpers.GetHashCode(obj);
    }
}
