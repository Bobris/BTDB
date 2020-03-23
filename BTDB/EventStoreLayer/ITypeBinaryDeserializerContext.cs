using BTDB.Encrypted;

namespace BTDB.EventStoreLayer
{
    public interface ITypeBinaryDeserializerContext
    {
        object LoadObject();
        void AddBackRef(object obj);
        void SkipObject();
        EncryptedString LoadEncryptedString();
        void SkipEncryptedString();
    }
}
