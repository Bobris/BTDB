namespace BTDB.ODBLayer
{
    public interface IMidLevelDBTransactionInternal
    {
        ulong CreateNewObjectId();
        void RegisterNewObject(ulong id, object obj);
        AbstractBufferedWriter PrepareToWriteObject(ulong id);
        void ObjectModified(ulong id, object obj);
    }
}