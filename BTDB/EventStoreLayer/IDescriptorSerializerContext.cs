using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public interface IDescriptorSerializerLiteContext
    {
        void StoreNewDescriptors(object obj);
    }

    public interface IDescriptorSerializerContext
    {
        bool SomeTypeStored { get; }
        IDescriptorSerializerContext StoreNewDescriptors(AbstractBufferedWriter writer, object obj);
        void CommitNewDescriptors();
        void StoreObject(AbstractBufferedWriter writer, object obj);
        void FinishNewDescriptors(AbstractBufferedWriter writer);
    }
}