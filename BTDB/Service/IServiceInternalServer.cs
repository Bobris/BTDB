using System;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public interface IServiceInternalServer
    {
        void StartResultMarshaling(ref SpanWriter writer, uint resultId);
        void FinishResultMarshaling(ref SpanWriter writer);
        void ExceptionMarshaling(uint resultId, Exception ex);
        void VoidResultMarshaling(uint resultId);
        object LoadObjectOnServer(ref SpanReader reader, IReaderCtx readerCtx);
        void WriteObjectForClient(object @object, ref SpanWriter writer, IWriterCtx writerCtx);
        ISymmetricCipher GetSymmetricCipher();
    }
}
