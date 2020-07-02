using System.Threading.Tasks;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public interface IServiceInternalClient
    {
        void StartTwoWayMarshaling(ref SpanWriter writer, ClientBindInf binding, out Task resultReturned);
        void FinishTwoWayMarshaling(ref SpanWriter writer);
        void StartOneWayMarshaling(ref SpanWriter writer, ClientBindInf binding);
        void FinishOneWayMarshaling(ref SpanWriter writer);
        void WriteObjectForServer(object @object, ref SpanWriter writer, IWriterCtx writerCtx);
        object LoadObjectOnClient(ref SpanReader reader, IReaderCtx readerCtx);
        ISymmetricCipher GetSymmetricCipher();
    }
}
