using System;
using System.Net;
using System.Threading;
using BTDB.Service;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class TcpipClientServerChannelTest
    {
        const int Port = 14514;
        readonly IPEndPoint _ipEndPoint = new IPEndPoint(IPAddress.Loopback, Port);

        [Test]
        public void StartAndStopListeningWorks()
        {
            var server = new TcpipServer(_ipEndPoint);
            server.StartListening();
            server.StopListening();
        }

        [Test]
        public void ConnectNothereFails()
        {
            var e = new AutoResetEvent(false);
            var client = new TcpipClient(_ipEndPoint);
            Exception connectException = null;
            client.OnConnect.Subscribe(
                u => Assert.Fail("Should not connect"),
                ex => connectException = ex,
                () => Assert.Fail("Connect should end with exception"));
            client.OnReceive.Subscribe(b => { }, () => e.Set());
            client.ConnectAsync();
            Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.NotNull(connectException);
        }

        [Test]
        public void ConnectClientToServerClientDisconnects()
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
            server.NewClient = ch =>
                {
                    Assert.AreEqual(_ipEndPoint, (ch as ITcpIpChannel).LocalEndPoint);
                    clientEndPoint = (ch as ITcpIpChannel).RemoteEndPoint;
                    ch.OnConnect.Subscribe(u => onServerConnected = true,
                                           ex => Assert.Fail(ex.ToString()),
                                           () => servercompleted2 = true);
                    ch.OnReceive.Subscribe(bb => Assert.Fail("receive without send"), ex => Assert.Fail(ex.ToString()), () =>
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
                        Assert.AreEqual(_ipEndPoint, client.RemoteEndPoint);
                    },
                    ex => Assert.Fail(ex.ToString()),
                    () => clientcompleted2 = true);
                client.OnReceive.Subscribe(bb => Assert.Fail("receive without send"),
                                           ex => Assert.Fail(ex.ToString()),
                                           () => clientcompleted = true);
                client.ConnectAsync();
                Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
                client.Dispose();
                Assert.True(clientcompleted);
                Assert.True(clientcompleted2);
                Assert.True(e2.WaitOne(TimeSpan.FromSeconds(10)));
                Assert.True(servercompleted);
                Assert.True(servercompleted2);
                Assert.True(onClientConnected);
                Assert.True(onServerConnected);
                Assert.AreEqual(clientEndPoint, clientEndPoint2);
            }
            finally 
            {
                server.StopListening();
            }
        }

        [Test]
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
                Assert.AreEqual(_ipEndPoint, (ch as ITcpIpChannel).LocalEndPoint);
                clientEndPoint = (ch as ITcpIpChannel).RemoteEndPoint;
                serverChannel = ch;
                ch.OnConnect.Subscribe(u => { onServerConnected = true; e.Set(); },
                                       ex => Assert.Fail(ex.ToString()),
                                       () => servercompleted2 = true);
                ch.OnReceive.Subscribe(bb => Assert.Fail("receive without send"), ex => Assert.Fail(ex.ToString()), () =>
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
                        Assert.AreEqual(_ipEndPoint, client.RemoteEndPoint);
                    },
                    ex => Assert.Fail(ex.ToString()),
                    () => clientcompleted2 = true);
                client.OnReceive.Subscribe(bb => Assert.Fail("receive without send"),
                                           ex => Assert.Fail(ex.ToString()),
                                           () => { clientcompleted = true; e2.Set(); });
                client.ConnectAsync();
                Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
                serverChannel.Dispose();
                Assert.True(servercompleted);
                Assert.True(servercompleted2);
                Assert.True(e2.WaitOne(TimeSpan.FromSeconds(10)));
                Assert.True(clientcompleted);
                Assert.True(clientcompleted2);
                Assert.True(onClientConnected);
                Assert.True(onServerConnected);
                Assert.AreEqual(clientEndPoint, clientEndPoint2);
            }
            finally
            {
                server.StopListening();
            }
        }
    }
}
