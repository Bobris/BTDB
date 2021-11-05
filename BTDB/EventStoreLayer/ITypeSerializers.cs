using System;
using BTDB.FieldHandler;

namespace BTDB.EventStoreLayer;

public interface ITypeDescriptorCallbacks
{
    ITypeDescriptor DescriptorOf(object obj);
    ITypeDescriptor DescriptorOf(Type type);
    bool IsSafeToLoad(Type type);
    ITypeConvertorGenerator ConvertorGenerator { get; }
    ITypeNameMapper TypeNameMapper { get; }
    Type LoadAsType(ITypeDescriptor descriptor);
    Type LoadAsType(ITypeDescriptor descriptor, Type targetType);
}

interface ITypeSerializers : ITypeSerializerMappingFactory, ITypeDescriptorCallbacks
{
    void SetTypeNameMapper(ITypeNameMapper typeNameMapper);
    void ForgotAllTypesAndSerializers();
}
