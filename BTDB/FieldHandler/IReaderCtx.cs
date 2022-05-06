using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public interface IReaderCtx
{
    // Returns true if actual content needs to be deserialized
    bool ReadObject(ref SpanReader reader, out object @object);
    // Register last deserialized object
    void RegisterObject(object @object);
    void ReadObjectDone(ref SpanReader reader);
    object ReadNativeObject(ref SpanReader reader);
    // Returns true if actual content needs to be deserialized
    bool SkipObject(ref SpanReader reader);
    void SkipNativeObject(ref SpanReader reader);

    void FreeContentInNativeObject(ref SpanReader reader);
    void RegisterDict(ulong dictId);
    void RegisterOid(ulong oid);

    EncryptedString ReadEncryptedString(ref SpanReader reader);
    void SkipEncryptedString(ref SpanReader reader);
    EncryptedString ReadOrderedEncryptedString(ref SpanReader reader);
    void SkipOrderedEncryptedString(ref SpanReader reader);
}
