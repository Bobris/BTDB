using BTDB.FieldHandler;

namespace BTDB.EventStoreLayer
{
    interface ITypeSerializers : ITypeSerializerMappingFactory
    {
        void SetTypeNameMapper(ITypeNameMapper typeNameMapper);
        void ForgotAllTypesAndSerializers();
        ITypeDescriptor DescriptorOf(object obj);
        ITypeConvertorGenerator ConvertorGenerator { get; }
    }
}