using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal interface ITypeSerializersMapping
    {
        void LoadTypeDescriptors(AbstractBufferedReader reader);
        object LoadObject(AbstractBufferedReader reader);
        IDescriptorSerializerContext StoreNewDescriptors(AbstractBufferedWriter writer, object obj);
        void CommitNewDescriptors(IDescriptorSerializerContext context);
        void StoreObject(AbstractBufferedWriter writer, object obj);
    }
}