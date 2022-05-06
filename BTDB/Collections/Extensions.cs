using System.Collections.Generic;

namespace BTDB.Collections;

public static class Extensions
{
    public static int IndexOf<T>(this IReadOnlyList<T> readOnlyList, in T value)
    {
        var count = readOnlyList.Count;
        var equalityComparer = EqualityComparer<T>.Default;
        for (var i = 0; i < count; i++)
        {
            var current = readOnlyList[i];
            if (equalityComparer.Equals(current, value)) return i;
        }
        return -1;
    }
}
