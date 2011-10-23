using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public interface IInternalObjectDBTransaction : IObjectDBTransaction
    {
        IObjectDB Owner { get; }
        IKeyValueDBTransaction KeyValueDBTransaction { get; }
        KeyValueDBTransactionProtector TransactionProtector { get; }
        ulong AllocateDictionaryId();
    }
}