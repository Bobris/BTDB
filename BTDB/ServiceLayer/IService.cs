using System.Linq;
using System.Text;
using System.Threading;

namespace BTDB.ServiceLayer
{
    public interface IService : IServiceClient, IServiceServer
    {
        IChannel Channel { get; }
    }
}
