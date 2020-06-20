using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public interface IInternalObjectDBTransaction : IObjectDBTransaction
    {
        KeyValueDBTransactionProtector TransactionProtector { get; }
        IRelationModificationCounter GetRelationModificationCounter(uint relationId);
        ulong AllocateDictionaryId();
        object ReadInlineObject(ref SpanReader reader, IReaderCtx readerCtx);
        void WriteInlineObject(ref SpanWriter writer, object @object, IWriterCtx writerCtx);
        ulong StoreIfNotInlined(object @object, bool autoRegister, bool forceInline);
        void FreeContentInNativeObject(ref SpanReader reader, IReaderCtx readerCtx);
    }
}
