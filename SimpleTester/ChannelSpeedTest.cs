using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Reactive;
using BTDB.Service;

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
                channel.OnReceive.FastSubscribe(channel.Send);
            }

            public void Run()
            {
            }

            public void Shutdown()
            {
                _server.StopListening();
            }
        }

        class Sender
        {
            readonly int _messageLen;
            readonly int _messageCount;
            readonly IChannel _client;
            TimeSpan _elapsedTime;

            public Sender(int messageLen, int messageCount, IChannel client)
            {
                _messageLen = messageLen;
                _client = client;
                _messageCount = messageCount;
            }

            public void MassSend()
            {
                var stopwatch = Stopwatch.StartNew();
                var message = new byte[_messageLen];
                for (int i = 0; i < _messageCount; i++)
                {
                    PackUnpack.PackInt64LE(message, 0, Stopwatch.GetTimestamp());
                    _client.Send(ByteBuffer.NewSync(message));
                }
                _elapsedTime = stopwatch.Elapsed;
            }

            public string SummaryInfo(string caption)
            {
                return string.Format(CultureInfo.InvariantCulture,
                    "{3} in {0:G6} s which is {1:G6} messages of size {2} per second",
                    _elapsedTime.TotalSeconds, _messageCount / _elapsedTime.TotalSeconds, _messageLen, caption);
            }
        }

        class Receiver
        {
            readonly int _messageLen;
            readonly int _messageCount;
            IChannel _client;
            readonly TimeStatCalculator _stats = new TimeStatCalculator();
            readonly Stopwatch _stopwatch = new Stopwatch();
            readonly TaskCompletionSource<Unit> _finished = new TaskCompletionSource<Unit>();
            TimeSpan _elapsedTime;
            int _receiveCounter;
            IDisposable _unlinker;

            public Receiver(int messageLen, int messageCount)
            {
                _messageLen = messageLen;
                _messageCount = messageCount;
            }

            public void StartReceive(IChannel client)
            {
                _client = client;
                _stopwatch.Start();
                _unlinker = _client.OnReceive.FastSubscribe(message =>
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
                        _elapsedTime = _stopwatch.Elapsed;
                        _finished.TrySetResult(Unit.Default);
                    }
                });

            }

            public void WaitTillFinish()
            {
                _finished.Task.Wait();
                _unlinker.Dispose();
            }

            public string SummaryInfo(string caption)
            {
                return string.Format(CultureInfo.InvariantCulture, "{3} in {0:G6} s which is {1:G6} messages of size {2} per second{4}{5}", _elapsedTime.TotalSeconds, _messageCount / _elapsedTime.TotalSeconds, _messageLen, caption, Environment.NewLine, _stats);
            }

        }

        public class EchoClient : ITestRun
        {
            int _messageLen;
            int _messageCount;
            IChannel _client;

            public void Setup(string[] parameters)
            {
                _messageLen = 1024;
                _messageCount = 1000;
                if (parameters.Length > 0) _messageLen = int.Parse(parameters[0]);
                if (parameters.Length > 1) _messageCount = int.Parse(parameters[1]);
            }

            public void Run()
            {
                _client = new TcpipClient(new IPEndPoint(IPAddress.Loopback, 12345));
                var sender = new Sender(_messageLen, _messageCount, _client);
                var receiver = new Receiver(_messageLen, _messageCount);
                receiver.StartReceive(_client);
                sender.MassSend();
                receiver.WaitTillFinish();
                Console.WriteLine(sender.SummaryInfo("Echo send"));
                Console.WriteLine(receiver.SummaryInfo("Echo recv"));
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

            public void Setup(string[] parameters)
            {
                _messageLen = 1024;
                _messageCount = 1000;
                if (parameters.Length > 0) _messageLen = int.Parse(parameters[0]);
                if (parameters.Length > 1) _messageCount = int.Parse(parameters[1]);
            }

            public void Run()
            {
                _client = new TcpipClient(new IPEndPoint(IPAddress.Loopback, 12345));
                var sender = new Sender(_messageLen, _messageCount, _client);
                sender.MassSend();
                Console.WriteLine(sender.SummaryInfo("Send"));
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
            Receiver _receiver;

            public void Setup(string[] parameters)
            {
                _messageLen = 1024;
                _messageCount = 1000;
                if (parameters.Length > 0) _messageLen = int.Parse(parameters[0]);
                if (parameters.Length > 1) _messageCount = int.Parse(parameters[1]);
                _server = new TcpipServer(new IPEndPoint(IPAddress.Any, 12345)) { NewClient = OnNewClient };
                _server.StartListening();
                _receiver = new Receiver(_messageLen, _messageCount);
            }

            void OnNewClient(IChannel channel)
            {
                _receiver.StartReceive(channel);
            }

            public void Run()
            {
                _receiver.WaitTillFinish();
                Console.WriteLine(_receiver.SummaryInfo("Recv"));
            }

            public void Shutdown()
            {
                _server.StopListening();
            }
        }

        public class PipeEcho : ITestRun
        {
            const int MessageCount = 50000000;
            PipedTwoChannels _twoChannels;

            public void Setup(string[] parameters)
            {
            }

            public void Run()
            {
                _twoChannels = new PipedTwoChannels();
                _twoChannels.Second.OnReceive.FastSubscribe(_twoChannels.Second.Send);
                const int messageLen = 1024;
                var sender = new Sender(messageLen, MessageCount, _twoChannels.First);
                var receiver = new Receiver(messageLen, MessageCount);
                receiver.StartReceive(_twoChannels.First);
                sender.MassSend();
                receiver.WaitTillFinish();
                Console.WriteLine(sender.SummaryInfo("PipeEcho send"));
                Console.WriteLine(receiver.SummaryInfo("PipeEcho recv"));
                _twoChannels.Disconnect();
            }

            public void Shutdown()
            {
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
