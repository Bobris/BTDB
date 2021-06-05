using BTDB.KVDBLayer;

namespace ODbDump.TrlDump
{
    public interface ITrlVisitor
    {
        void StartFile(uint index, ulong size);

        void StartOperation(KVCommandType type);
        //generic detail
        void OperationDetail(string detail);
        //detail for CreateOrUpdate
        void UpsertObject(ulong oid, uint tableId, int keyLength, int valueLength);
        void UpsertODBDictionary(ulong oid, int keyLength, int valueLength);
        void UpsertRelationValue(ulong relationIdx, int keyLength, int valueLength);
        void UpsertRelationSecondaryKey(ulong relationIdx, int skIndex, int keyLength, int valueLength);

        void EndOperation();
    }
}
