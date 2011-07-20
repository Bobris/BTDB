using System;
using System.Net;
using System.Threading;
using BTDB.ServiceLayer;
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
            ChannelStatus status = ChannelStatus.Connecting;
            var client = new TcpipClient(_ipEndPoint, ch =>
                {
                    status = ch.Status;
                    e.Set();
                });
            Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.AreEqual(ChannelStatus.Disconnected, status);
        }

        [Test]
        public void ConnectClientToServer()
        {
            var server = new TcpipServer(_ipEndPoint);
            var e = new AutoResetEvent(false);
            var e2 = new AutoResetEvent(false);
            var status2 = ChannelStatus.Connecting;
            IChannel clientOnServer = null;
            server.NewClient = ch =>
                {
                    clientOnServer = ch;
                    ch.StatusChanged = ch2 =>
                        {
                            status2 = ch2.Status;
                            e2.Set();
                        };
                };
            server.StartListening();

            var status = ChannelStatus.Connecting;
            var client = new TcpipClient(_ipEndPoint, ch =>
                {
                    status = ch.Status;
                    e.Set();
                });
            Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.True(e2.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.AreEqual(ChannelStatus.Connected, status);
            Assert.AreEqual(ChannelStatus.Connected, status2);
            client.Dispose();
            clientOnServer.Receive().ContinueWith(t => Assert.True(t.IsFaulted)).Wait(TimeSpan.FromSeconds(10));
            Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.True(e2.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.AreEqual(ChannelStatus.Disconnected, status);
            Assert.AreEqual(ChannelStatus.Disconnected, status2);
            server.StopListening();
        }
    }
}