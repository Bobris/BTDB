using System;
using System.Reactive.Subjects;
using BTDB.Buffer;
using BTDB.Reactive;

namespace BTDB.Service
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
            bool _disposed;

            public void Dispose()
            {
                if (!_disposed)
                {
                    _disposed = true;
                    _receiver.OnCompleted();
                    _other.Dispose();
                }
            }

            public void Send(ByteBuffer data)
            {
                _other._receiver.OnNext(data);
            }

            public IObservable<ByteBuffer> OnReceive
            {
                get { return _receiver; }
            }

            internal Channel Other
            {
                set
                {
                    _other = value;
                }
            }
        }

        public IChannel First { get { return _first; } }
        public IChannel Second { get { return _second; } }

        public void Disconnect()
        {
            _first.Dispose();
            _second.Dispose();
        }
    }
}
