using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public interface IInternalObjectDBTransaction : IObjectDBTransaction
    {
        IObjectDB Owner { get; }
        IKeyValueDBTransaction KeyValueDBTransaction { get; }
        KeyValueDBTransactionProtector TransactionProtector { get; }
        ulong AllocateDictionaryId();
        object ReadInlineObject(IReaderCtx readerCtx);
        IWriterCtx GetWriterCtx(AbstractBufferedWriter writer);
        IWriterCtx ExtractWriterCtx();
        void InjectWriterCtx(IWriterCtx writer);
        IReaderCtx GetReaderCtx(AbstractBufferedReader reader);
        IReaderCtx ExtractReaderCtx();
        void InjectReaderCtx(IReaderCtx reader);
        void WriteInlineObject(object @object, IWriterCtx writerCtx);
        ulong StoreIfNotInlined(object @object, bool autoRegister, bool preferInline);
        bool FreeContentInNativeObject(IReaderCtx readerCtx);
    }
}