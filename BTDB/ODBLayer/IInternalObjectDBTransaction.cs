using System;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public interface IInternalObjectDBTransaction : IObjectDBTransaction
{
    /// Runs before rollback releases the writer; discarded on successful commit. Actions must not throw.
    void RegisterRollbackAction(Action action);
    ulong AllocateDictionaryId();
    object ReadInlineObject(ref MemReader reader, IReaderCtx readerCtx, bool skipping);
    void WriteInlineObject(ref MemWriter writer, object @object, IWriterCtx writerCtx);
    ulong StoreIfNotInlined(object @object, bool autoRegister, bool forceInline);
    void FreeContentInNativeObject(ref MemReader reader, IReaderCtx readerCtx);
    bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);
    void ThrowIfDisposed();
}
