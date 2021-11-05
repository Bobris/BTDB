using System;
using System.Collections.Generic;

namespace BTDB.EventStoreLayer;

public interface ITypeDescriptorMultipleNativeTypes : ITypeDescriptor
{
    IEnumerable<Type> GetNativeTypes();
}
