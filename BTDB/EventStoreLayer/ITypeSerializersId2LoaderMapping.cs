using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeSerializersId2LoaderMapping
{
    object Load(uint typeId, ref MemReader reader, ITypeBinaryDeserializerContext context);
    ISymmetricCipher GetSymmetricCipher();
}
