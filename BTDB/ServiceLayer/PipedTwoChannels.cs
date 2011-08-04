using System;
using System.Reactive.Subjects;
using BTDB.Buffer;
using BTDB.Reactive;

namespace BTDB.ServiceLayer
{
    public class PipedTwoChannels
    {
        readonly Channel _first;
        readonly Channel _second;

        public PipedTwoChannels()
        {
            _first = new Channel();
            _second = new Channel();
            _first.Other = _second;
            _second.Other = _first;
        }

        class Channel : IChannel
        {
            Channel _other;
            readonly ISubject<ByteBuffer> _receiver = new FastSubject<ByteBuffer>();
            Action<IChannel> _statusChanged = ch => { };
            ChannelStatus _channelStatus;

            public Channel()
            {
                _channelStatus = ChannelStatus.Disconnected;
            }

            public void Dispose()
            {
                if (_channelStatus != ChannelStatus.Disconnected)
                {
                    _channelStatus = ChannelStatus.Disconnected;
                    _statusChanged(this);
                    _receiver.OnCompleted();
                    _other.Dispose();
                }
            }

            public Action<IChannel> StatusChanged
            {
                set { _statusChanged = value; }
            }

            public void Send(ByteBuffer data)
            {
                _other._receiver.OnNext(data);
            }

            public IObservable<ByteBuffer> OnReceive
            {
                get { return _receiver; }
            }

            public ChannelStatus Status
            {
                get { return _channelStatus; }
            }

            internal Channel Other
            {
                set
                {
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
