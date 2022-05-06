using System;

namespace BTDB.EventStoreLayer;

public interface ITypeDescriptorFactory
{
    ITypeDescriptor? Create(Type type);
}
