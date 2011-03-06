namespace BTDB.ODBLayer
{
    public interface IMidLevelDBTransactionInternal
    {
        ulong CreateNewObjectId();
        void RegisterDirtyObject(ulong id, object obj);
        AbstractBufferedWriter PrepareToWriteObject(ulong id);

    }
}