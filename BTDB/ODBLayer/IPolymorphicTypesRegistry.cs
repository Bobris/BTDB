using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface IPolymorphicTypesRegistry
{
    void RegisterPolymorphicType(Type type);
    IEnumerable<Type> GetPolymorphicTypes(Type baseType);
}
