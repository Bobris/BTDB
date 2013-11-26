using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public interface ITypeSerializersMapping : IDescriptorSerializerContext
    {
        void LoadTypeDescriptors(AbstractBufferedReader reader);
        object LoadObject(AbstractBufferedReader reader);
        void Reset();
    }
}