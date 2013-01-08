namespace BTDB.EventStoreLayer
{
    public interface ITypeBinarySerializerContext
    {
        void StoreObject(object obj);
    }
}