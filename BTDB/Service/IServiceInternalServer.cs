using System;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public interface IServiceInternalServer
    {
        AbstractBufferedWriter StartResultMarshaling(uint resultId);
        void FinishResultMarshaling(AbstractBufferedWriter writer);
        void ExceptionMarshaling(uint resultId, Exception ex);
        void VoidResultMarshaling(uint resultId);
    }
}