using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public interface IPolymorphicTypesRegistry
    {
        void RegisterPolymorphicType(Type type, Type baseType);
        bool IsPolymorphicType(Type baseType, out IEnumerable<Type> subTypes);
    }
}