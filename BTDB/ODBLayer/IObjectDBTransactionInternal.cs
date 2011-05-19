namespace BTDB.ODBLayer
{
    public interface IObjectDBTransactionInternal
    {
        void RegisterNewObject(ulong id, object obj);
        AbstractBufferedWriter PrepareToWriteObject(ulong id);
        void ObjectModified(ulong id, object obj);
        object Get(ulong oid);
        ulong GetOid(object obj);
        void CheckPropertyOperationValidity(object obj);
        void InternalDelete(object obj);
    }
}