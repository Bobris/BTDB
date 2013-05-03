namespace BTDB.EventStoreLayer
{
    internal interface ITypeSerializers
    {
        ITypeSerializersMapping CreateMapping();
        void SetTypeNameMapper(ITypeNameMapper typeNameMapper);
        void ForgotAllTypesAndSerializers();
        ITypeDescriptor DescriptorOf(object obj);
    }
}