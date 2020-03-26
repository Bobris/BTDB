using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public interface IWriterCtx
    {
        // Return true if actual content needs to be serialized
        bool WriteObject(object @object);
        void WriteNativeObject(object @object);
        void WriteNativeObjectPreventInline(object @object);
        AbstractBufferedWriter Writer();
        void WriteEncryptedString(EncryptedString value);
        void WriteOrderedEncryptedString(EncryptedString value);
    }
}
