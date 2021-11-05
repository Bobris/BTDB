namespace BTDB.EventStoreLayer;

public interface IEventStoreManager
{
    ICompressionStrategy CompressionStrategy { get; set; }
    void SetNewTypeNameMapper(ITypeNameMapper mapper);
    void ForgotAllTypesAndSerializers();
    IReadEventStore OpenReadOnlyStore(IEventFileStorage file);
    IWriteEventStore AppendToStore(IEventFileStorage file);
    ITypeDescriptor DescriptorOf(object obj);
}
