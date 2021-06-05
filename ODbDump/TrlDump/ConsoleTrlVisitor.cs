using System;
using BTDB.KVDBLayer;

namespace ODbDump.TrlDump
{
    public class ConsoleTrlVisitor : ITrlVisitor
    {
        public void StartFile(uint index, ulong size)
        {
            Console.WriteLine($"StartFile idx: {index} size: {size}");
        }

        public void StartOperation(KVCommandType type)
        {
            Console.Write(type.ToString());
        }

        public void OperationDetail(string detail)
        {
            Console.Write(" " + detail);
        }

        public void UpsertObject(ulong oid, uint tableId, int keyLength, int valueLength)
        {
            OperationDetail(
                $"{(valueLength > 500000 ? "bigobject" : "object")} oid: {oid} tableId: {tableId} keyLength: {keyLength} valueLength: {valueLength}");
        }

        public void UpsertODBDictionary(ulong oid, int keyLength, int valueLength)
        {
            OperationDetail($"IDictionary value oid: {oid} keyLength: {keyLength} valueLength: {valueLength}");
        }

        public void UpsertRelationValue(ulong relationIdx, int keyLength, int valueLength)
        {
            OperationDetail($"Relation id {relationIdx} keyLength: {keyLength} valueLength: {valueLength}");
        }

        public void UpsertRelationSecondaryKey(ulong relationIdx, int skIndex, int keyLength, int valueLength)
        {
            OperationDetail(
                $"Secondary key relIdx {relationIdx} skIdx {skIndex} keyLength: {keyLength} valueLength: {valueLength}");
        }

        public void EndOperation()
        {
            Console.WriteLine("");
        }
    }
}
