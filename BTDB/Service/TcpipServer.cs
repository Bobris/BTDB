using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Reactive;

namespace BTDB.Service
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
            Task.Factory.StartNew(AcceptNewClients, TaskCreationOptions.LongRunning);
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
                channel.StartReceiving();
            }
        }

        internal class Client : IChannel, ITcpIpChannel
        {
            readonly Socket _socket;
            readonly FastSubject<ByteBuffer> _receiver = new FastSubject<ByteBuffer>();
            readonly FastBehaviourSubject<bool> _connector = new FastBehaviourSubject<bool>();
            readonly object _sendlock = new object();
            bool _disconnected;

            public Client(Socket socket)
            {
                _socket = socket;
                _socket.Blocking = true;
                _socket.ReceiveTimeout = -1;
            }

            public void Dispose()
            {
                if (!_disconnected)
                {
                    _socket.Shutdown(SocketShutdown.Both);
                    SignalDisconnected();
                }
                _socket.Dispose();
            }

            public void Send(ByteBuffer data)
            {
                if (_disconnected) throw new SocketException((int)SocketError.NotConnected);
                var vuLen = PackUnpack.LengthVUInt((uint)data.Length);
                var vuBuf = new byte[vuLen];
                int o = 0;
                PackUnpack.PackVUInt(vuBuf, ref o, (uint)data.Length);
                SocketError socketError;
                lock (_sendlock)
                {
                    _socket.Send(new[] { new ArraySegment<byte>(vuBuf), data.ToArraySegment() }, SocketFlags.None,
                                 out socketError);
                }
                if (socketError == SocketError.Success) return;
                if (!IsConnected())
                {
                    SignalDisconnected();
                }
                throw new SocketException((int)socketError);
            }

            public IObservable<ByteBuffer> OnReceive
            {
                get { return _receiver; }
            }

            public IObservable<bool> OnConnect
            {
                get { return _connector; }
            }

            void SignalDisconnected()
            {
                _connector.OnCompleted();
                _receiver.OnCompleted();
                _disconnected = true;
            }

            bool Receive(byte[] buf, int ofs, int len)
            {
                while (len > 0)
                {
                    SocketError errorCode;
                    var received = _socket.Receive(buf, ofs, len, SocketFlags.None, out errorCode);
                    if (errorCode != SocketError.Success)
                    {
                        SignalDisconnected();
                        return false;
                    }
                    ofs += received;
                    len -= received;
                    if (received == 0)
                    {
                        if (!IsConnected())
                        {
                            SignalDisconnected();
                            return false;
                        }
                    }
                }
                return true;
            }

            bool IsConnected()
            {
                try
                {
                    return !(_socket.Poll(1, SelectMode.SelectRead) && _socket.Available == 0);
                }
                catch (SocketException) { return false; }
            }

            public void Connect(IPEndPoint connectPoint)
            {
                Task.Factory.StartNew(() =>
                                          {
                                              try
                                              {
                                                  _socket.Connect(connectPoint);
                                              }
                                              catch (Exception exception)
                                              {
                                                  _connector.OnError(exception);
                                                  _receiver.OnCompleted();
                                                  return;
                                              }
                                              ReceiveBody();
                                          });
            }

            void ReceiveBody()
            {
                try
                {
                    _connector.OnNext(true);
                    var buf = new byte[9];
                    while (!_disconnected)
                    {
                        if (!Receive(buf, 0, 1)) return;
                        var packLen = PackUnpack.LengthVUInt(buf, 0);
                        if (packLen > 1) if (!Receive(buf, 1, packLen - 1)) return;
                        int o = 0;
                        var len = PackUnpack.UnpackVUInt(buf, ref o);
                        if (len > int.MaxValue) throw new InvalidDataException();
                        var result = new byte[len];
                        if (len != 0) if (!Receive(result, 0, (int)len)) return;
                        _receiver.OnNext(ByteBuffer.NewAsync(result));
                    }
                }
                catch (Exception)
                {
                    SignalDisconnected();
                }
                SignalDisconnected();
            }

            internal void StartReceiving()
            {
                Task.Factory.StartNew(ReceiveBody, TaskCreationOptions.LongRunning);
            }

            public IPEndPoint LocalEndPoint
            {
                get { return _socket.LocalEndPoint as IPEndPoint; }
            }

            public IPEndPoint RemoteEndPoint
            {
                get { return _socket.RemoteEndPoint as IPEndPoint; }
            }
        }
    }
}
