using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface IDescriptorSerializerLiteContext
{
    void StoreNewDescriptors(object obj);
}

public interface IDescriptorSerializerContext
{
    bool SomeTypeStored { get; }
    IDescriptorSerializerContext StoreNewDescriptors(object obj);
    void CommitNewDescriptors();
    void StoreObject(ref SpanWriter writer, object obj);
    void FinishNewDescriptors(ref SpanWriter writer);
}
