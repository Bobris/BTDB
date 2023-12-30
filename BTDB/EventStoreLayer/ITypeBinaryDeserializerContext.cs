using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeBinaryDeserializerContext
{
    object LoadObject(ref MemReader reader);
    void AddBackRef(object obj);
    void SkipObject(ref MemReader reader);
    EncryptedString LoadEncryptedString(ref MemReader reader);
    void SkipEncryptedString(ref MemReader reader);
}
