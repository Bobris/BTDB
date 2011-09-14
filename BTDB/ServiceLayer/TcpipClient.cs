using System;
using System.Net;
using System.Net.Sockets;
using BTDB.Buffer;

namespace BTDB.ServiceLayer
{
    public class TcpipClient : IChannel
    {
        readonly TcpipServer.Client _client;

        public TcpipClient(IPEndPoint endPoint)
        {
            var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _client = new TcpipServer.Client(socket);
            _client.Connect(endPoint);
        }

        public void Dispose()
        {
            _client.Dispose();
        }

        public void Send(ByteBuffer data)
        {
            _client.Send(data);
        }

        public IObservable<ByteBuffer> OnReceive
        {
            get { return _client.OnReceive; }
        }

    }
}