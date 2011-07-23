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

            public Client(Socket socket)
            {
                _socket = socket;
                _socket.Blocking = true;
                _socket.ReceiveTimeout = -1;
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
                    SignalDisconnected();
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
                SocketError socketError;
                _socket.Send(new[] { new ArraySegment<byte>(vuBuf), data }, SocketFlags.None, out socketError);
                if (socketError != SocketError.Success)
                {
                    if (!IsConnected())
                    {
                        SignalDisconnected();
                        return;
                    }
                    throw new SocketException((int)socketError);
                }
            }

            public Task<ArraySegment<byte>> Receive()
            {
                return Task<ArraySegment<byte>>.Factory.StartNew(() =>
                    {
                        var buf = new byte[9];
                        SocketError errorCode;
                        Receive(buf, 0, 1);
                        var packLen = PackUnpack.LengthVUInt(buf, 0);
                        if (packLen > 1) Receive(buf, 1, packLen - 1);
                        int o = 0;
                        var len = PackUnpack.UnpackVUInt(buf, ref o);
                        if (len > int.MaxValue) throw new InvalidDataException();
                        var result = new byte[len];
                        if (len != 0) Receive(result, 0, (int)len);
                        return new ArraySegment<byte>(result);
                    });
            }

            void Receive(byte[] buf, int ofs, int len)
            {
                while (len > 0)
                {
                    SocketError errorCode;
                    var received = _socket.Receive(buf, ofs, len, SocketFlags.None, out errorCode);
                    if (errorCode != SocketError.Success)
                    {
                        throw new InvalidDataException();
                    }
                    ofs += received;
                    len -= received;
                    if (received == 0)
                    {
                        if (!IsConnected())
                        {
                            SignalDisconnected();
                            throw new OperationCanceledException();
                        }
                    }
                }
            }

            bool IsConnected()
            {
                try
                {
                    return !(_socket.Poll(1, SelectMode.SelectRead) && _socket.Available == 0);
                }
                catch (SocketException) { return false; }
            }

            void SignalDisconnected()
            {
                _status = ChannelStatus.Disconnected;
                _statusChanged(this);
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
                                                  SignalDisconnected();
                                              }
                                          });
            }
        }
    }
}
