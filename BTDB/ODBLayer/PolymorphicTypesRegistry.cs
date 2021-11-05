using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer;

public class PolymorphicTypesRegistry : IPolymorphicTypesRegistry
{
    readonly object _lock = new object();
    readonly HashSet<Type> _types = new HashSet<Type>();

    public void RegisterPolymorphicType(Type type)
    {
        lock (_lock)
        {
            _types.Add(type);
        }
    }

    public IEnumerable<Type> GetPolymorphicTypes(Type baseType)
    {
        lock (_lock)
        {
            foreach (var t in _types)
            {
                if (!baseType.IsAssignableFrom(t)) continue;
                yield return t;
            }
        }
    }
}
