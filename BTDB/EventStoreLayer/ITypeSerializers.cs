using System;
using BTDB.FieldHandler;

namespace BTDB.EventStoreLayer
{
    public interface ITypeDescriptorCallbacks
    {
        ITypeDescriptor DescriptorOf(object obj);
        ITypeConvertorGenerator ConvertorGenerator { get; }
        ITypeNameMapper TypeNameMapper { get; }
        Type LoadAsType(ITypeDescriptor descriptor);
    }

    interface ITypeSerializers : ITypeSerializerMappingFactory, ITypeDescriptorCallbacks
    {
        void SetTypeNameMapper(ITypeNameMapper typeNameMapper);
        void ForgotAllTypesAndSerializers();
    }
}