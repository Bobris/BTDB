using System;
using System.Net;
using System.Net.Sockets;
using BTDB.Buffer;

namespace BTDB.Service
{
    public class TcpipClient : IChannel, ITcpIpChannel
    {
        readonly TcpipServer.Client _client;
        readonly IPEndPoint _endPoint;

        public TcpipClient(IPEndPoint endPoint)
        {
            _endPoint = endPoint;
            var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _client = new TcpipServer.Client(socket);
        }

        public void ConnectAsync()
        {
            _client.Connect(_endPoint);
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

        public IObservable<bool> OnConnect
        {
            get { return _client.OnConnect; }
        }

        public IPEndPoint LocalEndPoint
        {
            get { return _client.LocalEndPoint; }
        }

        public IPEndPoint RemoteEndPoint
        {
            get { return _client.RemoteEndPoint; }
        }
    }
}