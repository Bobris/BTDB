using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeSerializersId2LoaderMapping
{
    object Load(uint typeId, ref SpanReader reader, ITypeBinaryDeserializerContext context);
    ISymmetricCipher GetSymmetricCipher();
}
