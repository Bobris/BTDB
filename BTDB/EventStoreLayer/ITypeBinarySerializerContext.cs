using BTDB.Encrypted;

namespace BTDB.EventStoreLayer
{
    public interface ITypeBinarySerializerContext
    {
        void StoreObject(object obj);
        void StoreEncryptedString(EncryptedString value);
    }
}
