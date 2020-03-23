using BTDB.FieldHandler;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public interface IInternalObjectDBTransaction : IObjectDBTransaction
    {
        IObjectDB Owner { get; }
        IKeyValueDBTransaction KeyValueDBTransaction { get; }
        KeyValueDBTransactionProtector TransactionProtector { get; }
        IRelationModificationCounter GetRelationModificationCounter(uint relationId);
        ulong AllocateDictionaryId();
        object ReadInlineObject(IReaderCtx readerCtx);
        void WriteInlineObject(object @object, IWriterCtx writerCtx);
        ulong StoreIfNotInlined(object @object, bool autoRegister, bool forceInline);
        void FreeContentInNativeObject(IReaderCtx readerCtx);
    }
}
