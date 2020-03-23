using System;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public interface IServiceInternalServer
    {
        AbstractBufferedWriter StartResultMarshaling(uint resultId);
        void FinishResultMarshaling(AbstractBufferedWriter writer);
        void ExceptionMarshaling(uint resultId, Exception ex);
        void VoidResultMarshaling(uint resultId);
        object LoadObjectOnServer(IReaderCtx readerCtx);
        void WriteObjectForClient(object @object, IWriterCtx writerCtx);
        ISymmetricCipher GetSymmetricCipher();
    }
}
