using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.ServiceLayer
{
    public interface IServiceInternalClient
    {
        AbstractBufferedWriter StartTwoWayMarshaling(ClientBindInf binding, out Task resultReturned);
        void FinishTwoWayMarshaling(AbstractBufferedWriter writer);
        AbstractBufferedWriter StartOneWayMarshaling(ClientBindInf binding);
        void FinishOneWayMarshaling(AbstractBufferedWriter writer);
    }
}