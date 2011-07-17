using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace BTDB.ServiceLayer
{
    public class PipedTwoChannels
    {
        readonly Channel _first;
        readonly Channel _second;

        public PipedTwoChannels()
        {
            _first = new Channel(this);
            _second = new Channel(this);
            _first.Other = _second;
            _second.Other = _first;
        }

        class Channel : IChannel
        {
            readonly PipedTwoChannels _owner;
            readonly object _sync = new object();
            readonly BlockingCollection<byte[]> _producerConsumer = new BlockingCollection<byte[]>(10);
            Channel _other;
            Action<IChannel> _statusChanged;
            TaskCompletionSource<byte[]> _receiveSource;
            ChannelStatus _channelStatus;

            public Channel(PipedTwoChannels owner)
            {
                _owner = owner;
                _channelStatus = ChannelStatus.Disconnected;
            }

            public void Dispose()
            {
                if (_channelStatus != ChannelStatus.Disconnected)
                {
                    _channelStatus = ChannelStatus.Disconnected;
                    _statusChanged(this);
                }
                _other.Dispose();
            }

            public Action<IChannel> StatusChanged
            {
                set { _statusChanged = value; }
            }

            public void Send(byte[] data)
            {
                _other.Receive(data);
            }

            void Receive(byte[] data)
            {
                _producerConsumer.Add(data);
                lock (_sync)
                {
                    if (_receiveSource != null)
                    {
                        _receiveSource.SetResult(_producerConsumer.Take());
                        _receiveSource = null;
                        return;
                    }
                }
            }

            public Task<byte[]> Receive()
            {
                Task<byte[]> result;
                lock (_sync)
                {
                    if (_receiveSource != null) throw new InvalidOperationException("Only one receive could be in fight");
                    _receiveSource = new TaskCompletionSource<byte[]>();
                    result = _receiveSource.Task;
                    byte[] data;
                    if (_producerConsumer.TryTake(out data))
                    {
                        _receiveSource.SetResult(data);
                        _receiveSource = null;
                    }
                }
                return result;
            }

            public ChannelStatus Status
            {
                get { return _channelStatus; }
            }

            internal Channel Other
            {
                set {
                    _other = value;
                }
            }

            internal void Connect()
            {
                _channelStatus = ChannelStatus.Connected;
                _statusChanged(this);
            }
        }

        public IChannel First { get { return _first; } }
        public IChannel Second { get { return _second; } }

        public void Connect()
        {
            _first.Connect();
            _second.Connect();
        }

        public void Disconnect()
        {
            _first.Dispose();
        }
    }
}
