using System;
using System.Net;
using System.Threading;
using BTDB.Service;
using Xunit;

namespace BTDBTest
{
    public class TcpipClientServerChannelTest
    {
        const int Port = 14514;
        readonly IPEndPoint _ipEndPoint = new IPEndPoint(IPAddress.Loopback, Port);

        [Fact]
        public void StartAndStopListeningWorks()
        {
            var server = new TcpipServer(_ipEndPoint);
            server.StartListening();
            server.StopListening();
        }

        [Fact]
        public void ConnectNothereFails()
        {
            var e = new AutoResetEvent(false);
            var client = new TcpipClient(_ipEndPoint);
            Exception connectException = null;
            client.OnConnect.Subscribe(
                u => Assert.True(false, "Should not connect"),
                ex => connectException = ex,
                () => Assert.True(false, "Connect should end with exception"));
            client.OnReceive.Subscribe(b => { }, () => e.Set());
            client.ConnectAsync();
            Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.NotNull(connectException);
        }

        [Fact]
        public void ConnectClientToServerClientDisconnects()
        {
            var server = new TcpipServer(_ipEndPoint);
            var e = new AutoResetEvent(false);
            var e1 = new AutoResetEvent(false);
            var e2 = new AutoResetEvent(false);
            bool servercompleted = false;
            bool servercompleted2 = false;
            bool clientcompleted = false;
            bool clientcompleted2 = false;
            bool onServerConnected = false;
            bool onClientConnected = false;
            IPEndPoint clientEndPoint = null;
            IPEndPoint clientEndPoint2 = null;
            server.NewClient = ch =>
                {
                    Assert.Equal(_ipEndPoint, (ch as ITcpIpChannel).LocalEndPoint);
                    clientEndPoint = (ch as ITcpIpChannel).RemoteEndPoint;
                    ch.OnConnect.Subscribe(u => onServerConnected = true,
                                           ex => Assert.True(false, ex.ToString()),
                                           () => servercompleted2 = true);
                    ch.OnReceive.Subscribe(bb => Assert.True(false, "receive without send"), ex => Assert.True(false, ex.ToString()), () =>
                        {
                            servercompleted = true; e2.Set();
                        });
                    e.Set();
                };
            server.StartListening();
            try
            {
                var client = new TcpipClient(_ipEndPoint);
                client.OnConnect.Subscribe(u =>
                    {
                        onClientConnected = true;
                        clientEndPoint2 = client.LocalEndPoint;
                        Assert.Equal(_ipEndPoint, client.RemoteEndPoint);
                        e1.Set();
                    },
                    ex => Assert.True(false, ex.ToString()),
                    () => clientcompleted2 = true);
                client.OnReceive.Subscribe(bb => Assert.True(false, "receive without send"),
                                           ex => Assert.True(false, ex.ToString()),
                                           () => clientcompleted = true);
                client.ConnectAsync();
                Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
                Assert.True(e1.WaitOne(TimeSpan.FromSeconds(10)));
                client.Dispose();
                Assert.True(clientcompleted);
                Assert.True(clientcompleted2);
                Assert.True(e2.WaitOne(TimeSpan.FromSeconds(10)));
                Assert.True(servercompleted);
                Assert.True(servercompleted2);
                Assert.True(onClientConnected);
                Assert.True(onServerConnected);
                Assert.Equal(clientEndPoint, clientEndPoint2);
            }
            finally 
            {
                server.StopListening();
            }
        }

        [Fact]
        public void ConnectClientToServerServerDisconnects()
        {
            var server = new TcpipServer(_ipEndPoint);
            var e = new AutoResetEvent(false);
            var e2 = new AutoResetEvent(false);
            bool servercompleted = false;
            bool servercompleted2 = false;
            bool clientcompleted = false;
            bool clientcompleted2 = false;
            bool onServerConnected = false;
            bool onClientConnected = false;
            IPEndPoint clientEndPoint = null;
            IPEndPoint clientEndPoint2 = null;
            IChannel serverChannel = null;
            server.NewClient = ch =>
            {
                Assert.Equal(_ipEndPoint, (ch as ITcpIpChannel).LocalEndPoint);
                clientEndPoint = (ch as ITcpIpChannel).RemoteEndPoint;
                serverChannel = ch;
                ch.OnConnect.Subscribe(u => { onServerConnected = true; e.Set(); },
                                       ex => Assert.True(false, ex.ToString()),
                                       () => servercompleted2 = true);
                ch.OnReceive.Subscribe(bb => Assert.True(false, "receive without send"), ex => Assert.True(false, ex.ToString()), () =>
                {
                    servercompleted = true;
                });
            };
            server.StartListening();
            try
            {
                var client = new TcpipClient(_ipEndPoint);
                client.OnConnect.Subscribe(u =>
                    {
                        onClientConnected = true;
                        clientEndPoint2 = client.LocalEndPoint;
                        Assert.Equal(_ipEndPoint, client.RemoteEndPoint);
                    },
                    ex => Assert.True(false, ex.ToString()),
                    () => clientcompleted2 = true);
                client.OnReceive.Subscribe(bb => Assert.True(false, "receive without send"),
                                           ex => Assert.True(false, ex.ToString()),
                                           () => { clientcompleted = true; e2.Set(); });
                client.ConnectAsync();
                Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
                serverChannel.Dispose();
                Assert.True(e2.WaitOne(TimeSpan.FromSeconds(10)));
                Assert.True(servercompleted);
                Assert.True(servercompleted2);
                Assert.True(clientcompleted);
                Assert.True(clientcompleted2);
                Assert.True(onClientConnected);
                Assert.True(onServerConnected);
                Assert.Equal(clientEndPoint, clientEndPoint2);
            }
            finally
            {
                server.StopListening();
            }
        }
    }
}
