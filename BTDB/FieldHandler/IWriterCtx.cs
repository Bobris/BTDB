using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public interface IWriterCtx
{
    // Return true if actual content needs to be serialized
    bool WriteObject(ref SpanWriter writer, object @object);
    void WriteNativeObject(ref SpanWriter writer, object @object);
    void WriteNativeObjectPreventInline(ref SpanWriter writer, object @object);
    void WriteEncryptedString(ref SpanWriter writer, EncryptedString value);
    void WriteOrderedEncryptedString(ref SpanWriter writer, EncryptedString value);
}
