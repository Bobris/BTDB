namespace BTDB.EventStoreLayer
{
    internal interface ITypeSerializers : ITypeSerializerMappingFactory
    {
        void SetTypeNameMapper(ITypeNameMapper typeNameMapper);
        void ForgotAllTypesAndSerializers();
        ITypeDescriptor DescriptorOf(object obj);
    }
}