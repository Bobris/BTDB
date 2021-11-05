using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public interface ITypeBinaryDeserializerContext
{
    object LoadObject(ref SpanReader reader);
    void AddBackRef(object obj);
    void SkipObject(ref SpanReader reader);
    EncryptedString LoadEncryptedString(ref SpanReader reader);
    void SkipEncryptedString(ref SpanReader reader);
}
