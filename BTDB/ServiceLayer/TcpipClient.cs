using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer.Helpers;

namespace BTDB.ServiceLayer
{
    public class TcpipClient : IChannel
    {
        readonly TcpipServer.Client _client;

        public TcpipClient(IPEndPoint endPoint, Action<IChannel> statusChanged)
        {
            var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _client = new TcpipServer.Client(socket);
            _client.StatusChanged = statusChanged;
            _client.Connect(endPoint);
        }

        public void Dispose()
        {
            _client.Dispose();
        }

        public Action<IChannel> StatusChanged
        {
            set { _client.StatusChanged = value; }
        }

        public void Send(ByteBuffer data)
        {
            _client.Send(data);
        }

        public IObservable<ByteBuffer> OnReceive
        {
            get { return _client.OnReceive; }
        }

        public ChannelStatus Status
        {
            get { return _client.Status; }
        }
    }
}