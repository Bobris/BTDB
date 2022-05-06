using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeBinarySerializerContext
{
    void StoreObject(ref SpanWriter writer, object obj);
    void StoreEncryptedString(ref SpanWriter writer, EncryptedString value);
}
