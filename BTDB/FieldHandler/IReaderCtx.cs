using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public interface IReaderCtx
{
    // Returns true if actual content needs to be deserialized
    bool ReadObject(ref MemReader reader, out object? @object);

    // Register last deserialized object
    void RegisterObject(object @object);
    void ReadObjectDone(ref MemReader reader);

    object ReadNativeObject(ref MemReader reader);

    // Returns true if actual content needs to be deserialized
    bool SkipObject(ref MemReader reader);
    void SkipNativeObject(ref MemReader reader);

    void FreeContentInNativeObject(ref MemReader reader);
    void RegisterDict(ulong dictId);
    void RegisterOid(ulong oid);

    EncryptedString ReadEncryptedString(ref MemReader reader);
    void SkipEncryptedString(ref MemReader reader);
    EncryptedString ReadOrderedEncryptedString(ref MemReader reader);
    void SkipOrderedEncryptedString(ref MemReader reader);
}
