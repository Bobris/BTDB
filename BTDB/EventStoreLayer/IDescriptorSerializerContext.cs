using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public interface IDescriptorSerializerLiteContext
    {
        void StoreNewDescriptors(ref SpanWriter writer, object obj);
    }

    public interface IDescriptorSerializerContext
    {
        bool SomeTypeStored { get; }
        IDescriptorSerializerContext StoreNewDescriptors(ref SpanWriter writer, object obj);
        void CommitNewDescriptors();
        void StoreObject(ref SpanWriter writer, object obj);
        void FinishNewDescriptors(ref SpanWriter writer);
    }
}
