using System;
using System.Net;
using System.Threading;
using BTDB.Reactive;
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
            client.OnReceive.FastSubscribe(b=> { },()=>e.Set());
            Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
        }

        [Test]
        public void ConnectClientToServer()
        {
            var server = new TcpipServer(_ipEndPoint);
            var e = new AutoResetEvent(false);
            var e2 = new AutoResetEvent(false);
            bool servercompleted = false;
            bool clientcompleted = false;
            server.NewClient = ch =>
                {
                    ch.OnReceive.FastSubscribe(bb => Assert.Fail("receive without send"), ex => Assert.Fail(ex.ToString()), () =>
                        {
                            servercompleted = true; e2.Set();
                        });
                    e.Set();
                };
            server.StartListening();
            var client = new TcpipClient(_ipEndPoint);
            client.OnReceive.FastSubscribe(bb => Assert.Fail("receive without send"), ex => Assert.Fail(ex.ToString()),
                                           () => clientcompleted = true);
            Assert.True(e.WaitOne(TimeSpan.FromSeconds(10)));
            client.Dispose();
            Assert.True(clientcompleted);
            Assert.True(e2.WaitOne(TimeSpan.FromSeconds(10)));
            Assert.True(servercompleted);
            server.StopListening();
        }
    }
}