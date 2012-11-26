using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal interface ITypeSerializersMapping : IDescriptorSerializerContext
    {
        void LoadTypeDescriptors(AbstractBufferedReader reader);
        object LoadObject(AbstractBufferedReader reader);
    }
}