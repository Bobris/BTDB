using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public interface IWriterCtx
{
    // Return true if actual content needs to be serialized
    bool WriteObject(ref MemWriter writer, object @object);
    void WriteNativeObject(ref MemWriter writer, object @object);
    void WriteNativeObjectPreventInline(ref MemWriter writer, object @object);
    void WriteEncryptedString(ref MemWriter writer, EncryptedString value);
    void WriteOrderedEncryptedString(ref MemWriter writer, EncryptedString value);
}
