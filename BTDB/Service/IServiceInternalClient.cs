using System.Threading.Tasks;
using BTDB.Encrypted;
using BTDB.FieldHandler;

namespace BTDB.Service
{
    public interface IServiceInternalClient
    {
        AbstractBufferedWriter StartTwoWayMarshaling(ClientBindInf binding, out Task resultReturned);
        void FinishTwoWayMarshaling(AbstractBufferedWriter writer);
        AbstractBufferedWriter StartOneWayMarshaling(ClientBindInf binding);
        void FinishOneWayMarshaling(AbstractBufferedWriter writer);
        void WriteObjectForServer(object @object, IWriterCtx writerCtx);
        object LoadObjectOnClient(IReaderCtx readerCtx);
        ISymmetricCipher GetSymmetricCipher();
    }
}
