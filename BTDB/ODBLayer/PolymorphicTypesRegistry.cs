using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public class PolymorphicTypesRegistry : IPolymorphicTypesRegistry
    {
        readonly object _lock = new object();
        readonly IDictionary<Type, List<Type>> _subTypes = new Dictionary<Type, List<Type>>();

        public void RegisterPolymorphicType(Type type, Type baseType)
        {
            if (!baseType.IsAssignableFrom(type))
                throw new ArgumentException();
            lock (_lock)
            {
                if (_subTypes.TryGetValue(baseType, out var sub))
                    sub.Add(type);
                else
                    _subTypes[baseType] = new List<Type> {type};
            }
        }

        public bool IsPolymorphicType(Type baseType, out IEnumerable<Type> subTypes)
        {
            lock (_lock)
            {
                if (!_subTypes.TryGetValue(baseType, out var sub))
                {
                    subTypes = null;
                    return false;
                }

                subTypes = sub;
                return true;
            }
        }
    }
}