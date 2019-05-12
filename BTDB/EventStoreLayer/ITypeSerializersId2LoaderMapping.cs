using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public interface ITypeSerializersId2LoaderMapping
    {
        object Load(uint typeId, AbstractBufferedReader reader, ITypeBinaryDeserializerContext context);
    }
}