using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeBinarySerializerContext
{
    void StoreObject(ref MemWriter writer, object obj);
    void StoreEncryptedString(ref MemWriter writer, EncryptedString value);
}
