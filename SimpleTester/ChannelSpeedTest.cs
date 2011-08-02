using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer.Helpers;
using BTDB.ServiceLayer;

namespace SimpleTester
{
    public class TimeStatCalculator
    {
        int _count;
        double _average;
        double _q;
        double _min;
        double _max;

        public TimeStatCalculator()
        {
            _min = double.MaxValue;
            _max = double.MinValue;
        }

        public void Record(long ticks)
        {
            _count++;
            double x = ticks;
            double prevAverage = _average;
            double newAverage = prevAverage + (x - prevAverage) / _count;
            _average = newAverage;
            _q += (x - prevAverage) * (x - newAverage);
            if (x < _min) _min = x;
            if (x > _max) _max = x;
        }

        double Stdev()
        {
            if (_count < 2) return 0;
            return Math.Sqrt(_q / (_count - 1));
        }

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "Min {0:F1}us Avg {1:F1}us Max {2:F1}us Stdev {3:F1}us Count {4}",
                                 ToUs(_min), ToUs(_average), ToUs(_max), ToUs(Stdev()), _count);
        }

        static double ToUs(double ticks)
        {
            return ticks * 1e6 / Stopwatch.Frequency;
        }
    }

    class ChannelSpeedTest
    {
        interface ITestRun
        {
            void Setup(string[] parameters);
            void Run();
            void Shutdown();
        }

        public class ReadLine : ITestRun
        {
            public void Setup(string[] parameters)
            {
            }

            public void Run()
            {
                Console.WriteLine("Press Enter to shutdown");
                Console.ReadLine();
            }

            public void Shutdown()
            {
            }
        }

        public class EchoServer : ITestRun
        {
            IServer _server;

            public void Setup(string[] parameters)
            {
                _server = new TcpipServer(new IPEndPoint(IPAddress.Any, 12345)) { NewClient = OnNewClient };
                _server.StartListening();
            }

            static void OnNewClient(IChannel channel)
            {
                channel.OnReceive.Subscribe(channel.Send);
            }

            public void Run()
            {
            }

            public void Shutdown()
            {
                _server.StopListening();
            }
        }

        public class EchoClient : ITestRun
        {
            int _messageLen;
            int _messageCount;
            IChannel _client;
            readonly Stopwatch _stopwatch = new Stopwatch();
            readonly TimeStatCalculator _stats = new TimeStatCalculator();
            int _receiveCounter;

            public void Setup(string[] parameters)
            {
                _messageLen = 1024;
                _messageCount = 1000;
                if (parameters.Length > 0) _messageLen = int.Parse(parameters[0]);
                if (parameters.Length > 1) _messageCount = int.Parse(parameters[1]);
            }

            public void Run()
            {
                _client = new TcpipClient(new IPEndPoint(IPAddress.Loopback, 12345), ch => { });
                while (_client.Status != ChannelStatus.Connected)
                {
                    if (_client.Status == ChannelStatus.Disconnected)
                    {
                        Console.WriteLine("EchoClient cannot connect to server");
                    }
                    Thread.Yield();
                }
                _stopwatch.Start();
                Receive();
                var message = new byte[_messageLen];
                for (int i = 0; i < _messageCount; i++)
                {
                    PackUnpack.PackInt64LE(message, 0, Stopwatch.GetTimestamp());
                    _client.Send(ByteBuffer.NewSync(message));
                }
                var timeSpan = _stopwatch.Elapsed;
                Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Echo send in {0:G6} s which is {1:G6} messages of size {2} per second", timeSpan.TotalSeconds, _messageCount / timeSpan.TotalSeconds, _messageLen));
                while (_receiveCounter != _messageCount && _client.Status != ChannelStatus.Disconnected) Thread.Sleep(100);
                Thread.Sleep(100);
            }

            void Receive()
            {
                _client.OnReceive.Subscribe(message=>
                    {
                        if (message.Length != _messageLen)
                        {
                            throw new InvalidOperationException(string.Format("Recived message of len {0} instead {1}", message.Length, _messageLen));
                        }
                        var ticks = Stopwatch.GetTimestamp();
                        ticks -= PackUnpack.UnpackInt64LE(message.Buffer, message.Offset);
                        _stats.Record(ticks);
                        _receiveCounter++;
                        if (_receiveCounter == _messageCount)
                        {
                            var timeSpan = _stopwatch.Elapsed;
                            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Echo recv in {0:G6} s which is {1:G6} messages of size {2} per second", timeSpan.TotalSeconds, _messageCount / timeSpan.TotalSeconds, _messageLen));
                            Console.WriteLine(_stats.ToString());
                        }
                    });
            }

            public void Shutdown()
            {
                _client.Dispose();
            }
        }

        public class SendClient : ITestRun
        {
            int _messageLen;
            int _messageCount;
            IChannel _client;
            readonly Stopwatch _stopwatch = new Stopwatch();

            public void Setup(string[] parameters)
            {
                _messageLen = 1024;
                _messageCount = 1000;
                if (parameters.Length > 0) _messageLen = int.Parse(parameters[0]);
                if (parameters.Length > 1) _messageCount = int.Parse(parameters[1]);
            }

            public void Run()
            {
                _client = new TcpipClient(new IPEndPoint(IPAddress.Loopback, 12345), ch => { });
                while (_client.Status != ChannelStatus.Connected)
                {
                    if (_client.Status == ChannelStatus.Disconnected)
                    {
                        Console.WriteLine("EchoClient cannot connect to server");
                    }
                    Thread.Yield();
                }
                _stopwatch.Start();
                var message = new byte[_messageLen];
                for (int i = 0; i < _messageCount; i++)
                {
                    PackUnpack.PackInt64LE(message, 0, Stopwatch.GetTimestamp());
                    _client.Send(ByteBuffer.NewSync(message));
                }
                var timeSpan = _stopwatch.Elapsed;
                Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Send in {0:G6} s which is {1:G6} messages of size {2} per second", timeSpan.TotalSeconds, _messageCount / timeSpan.TotalSeconds, _messageLen));
            }

            public void Shutdown()
            {
                _client.Dispose();
            }
        }

        public class RecvServer : ITestRun
        {
            IServer _server;
            int _messageLen;
            int _messageCount;
            IChannel _client;
            readonly Stopwatch _stopwatch = new Stopwatch();
            readonly TimeStatCalculator _stats = new TimeStatCalculator();
            int _receiveCounter;

            public void Setup(string[] parameters)
            {
                _messageLen = 1024;
                _messageCount = 1000;
                if (parameters.Length > 0) _messageLen = int.Parse(parameters[0]);
                if (parameters.Length > 1) _messageCount = int.Parse(parameters[1]);
                _server = new TcpipServer(new IPEndPoint(IPAddress.Any, 12345)) { NewClient = OnNewClient };
                _server.StartListening();
            }

            void OnNewClient(IChannel channel)
            {
                _client = channel;
                _stopwatch.Start();
                Receive();
            }

            public void Run()
            {
                while (_receiveCounter != _messageCount && (_client == null || _client.Status != ChannelStatus.Disconnected)) Thread.Sleep(100);
                Thread.Sleep(100);
            }

            void Receive()
            {
                _client.OnReceive.Subscribe(message =>
                {
                    if (message.Length != _messageLen)
                    {
                        throw new InvalidOperationException(string.Format("Recived message of len {0} instead {1}", message.Length, _messageLen));
                    }
                    var ticks = Stopwatch.GetTimestamp();
                    ticks -= PackUnpack.UnpackInt64LE(message.Buffer, message.Offset);
                    _stats.Record(ticks);
                    _receiveCounter++;
                    if (_receiveCounter == _messageCount)
                    {
                        var timeSpan = _stopwatch.Elapsed;
                        Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Recv in {0:G6} s which is {1:G6} messages of size {2} per second", timeSpan.TotalSeconds, _messageCount / timeSpan.TotalSeconds, _messageLen));
                        Console.WriteLine(_stats.ToString());
                    }
                });
            }

            public void Shutdown()
            {
                _server.StopListening();
            }
        }

        public void Run(IEnumerable<string> args)
        {
            var tests = new List<ITestRun>();
            foreach (var s in args)
            {
                var p = s.Split('=');
                var r = GetType().GetNestedType(p[0]).GetConstructor(Type.EmptyTypes).Invoke(null) as ITestRun;
                r.Setup(p.Skip(1).ToArray());
                tests.Add(r);
            }
            Console.WriteLine("Running");
            tests.AsParallel().ForAll(t => t.Run());
            Console.WriteLine("Shutting down");
            tests.Reverse();
            tests.ForEach(t => t.Shutdown());
        }
    }
}
