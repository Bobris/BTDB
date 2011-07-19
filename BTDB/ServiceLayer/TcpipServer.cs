using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using BTDB.KVDBLayer.Helpers;

namespace BTDB.ServiceLayer
{
    public class TcpipServer : IServer
    {
        readonly TcpListener _listener;
        Action<IChannel> _newClient;

        public TcpipServer(IPEndPoint endPoint)
        {
            _listener = new TcpListener(endPoint);
        }

        public Action<IChannel> NewClient
        {
            set { _newClient = value; }
        }

        public void StartListening()
        {
            _listener.Start(10);
            Task.Factory.StartNew(AcceptNewClients);
        }

        public void StopListening()
        {
            _listener.Stop();
        }

        void AcceptNewClients()
        {
            while (true)
            {
                Socket socket;
                try
                {
                    socket = _listener.AcceptSocket();
                }
                catch (SocketException)
                {
                    return;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                var channel = new Client(socket);
                _newClient(channel);
                channel.SignalConnected();
            }
        }

        internal class Client : IChannel
        {
            readonly Socket _socket;
            ChannelStatus _status;
            Action<IChannel> _statusChanged;
            TaskCompletionSource<ArraySegment<byte>> _receiveSource;

            public Client(Socket socket)
            {
                _socket = socket;
                _status = ChannelStatus.Connecting;
                _statusChanged = _ => { };
            }

            public void Dispose()
            {
                if (_status != ChannelStatus.Disconnected)
                {
                    _status = ChannelStatus.Disconnecting;
                    _statusChanged(this);
                    _socket.Shutdown(SocketShutdown.Both);
                    _status = ChannelStatus.Disconnected;
                    _statusChanged(this);
                }
                _socket.Dispose();
            }

            public Action<IChannel> StatusChanged
            {
                set { _statusChanged = value; }
            }

            public void Send(ArraySegment<byte> data)
            {
                var vuLen = PackUnpack.LengthVUInt((uint)data.Count);
                var vuBuf = new byte[vuLen];
                int o = 0;
                PackUnpack.PackVUInt(vuBuf, ref o, (uint)data.Count);
                _socket.Send(new[] { new ArraySegment<byte>(vuBuf), data });
            }

            public Task<ArraySegment<byte>> Receive()
            {
                return Task<ArraySegment<byte>>.Factory.StartNew(() =>
                    {
                        var buf = new byte[9];
                        if (_socket.Receive(buf, 0, 1, SocketFlags.None) != 1) throw new InvalidDataException();
                        var packLen = PackUnpack.LengthVUInt(buf, 0);
                        if (packLen > 1 && _socket.Receive(buf, 1, packLen - 1, SocketFlags.None) != packLen - 1) throw new InvalidDataException();
                        int o = 0;
                        var len = PackUnpack.UnpackVUInt(buf, ref o);
                        if (len > int.MaxValue) throw new InvalidDataException();
                        var result = new byte[len];
                        if (len != 0 && _socket.Receive(result) != (int)len) throw new InvalidDataException();
                        return new ArraySegment<byte>(result);
                    });
            }

            public ChannelStatus Status
            {
                get { return _status; }
            }

            internal void SignalConnected()
            {
                _status = ChannelStatus.Connected;
                _statusChanged(this);
            }

            public void Connect(IPEndPoint connectPoint)
            {
                Task.Factory.StartNew(() =>
                                          {
                                              try
                                              {
                                                  _socket.Connect(connectPoint);
                                                  SignalConnected();
                                              }
                                              catch (Exception)
                                              {
                                                  _status = ChannelStatus.Disconnected;
                                                  _statusChanged(this);
                                              }
                                          });
            }
        }
    }
}
