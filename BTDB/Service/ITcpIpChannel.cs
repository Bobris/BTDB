using System.Net;

namespace BTDB.Service
{
    public interface ITcpIpChannel
    {
        IPEndPoint LocalEndPoint { get; }
        IPEndPoint RemoteEndPoint { get; }
    }
}