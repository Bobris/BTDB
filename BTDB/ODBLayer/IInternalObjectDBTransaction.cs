using System;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public interface IInternalObjectDBTransaction : IObjectDBTransaction
{
    ulong AllocateDictionaryId();
    object ReadInlineObject(ref SpanReader reader, IReaderCtx readerCtx, bool skipping);
    void WriteInlineObject(ref SpanWriter writer, object @object, IWriterCtx writerCtx);
    ulong StoreIfNotInlined(object @object, bool autoRegister, bool forceInline);
    void FreeContentInNativeObject(ref SpanReader reader, IReaderCtx readerCtx);
    void SetSerializationCallbacks(IInternalSerializationCallbacks? callbacks);
    bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);
}
