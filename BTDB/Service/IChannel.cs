using System;
using System.Reactive;
using BTDB.Buffer;

namespace BTDB.Service
{
    public interface IChannel : IDisposable
    {
        void Send(ByteBuffer data);
        IObservable<ByteBuffer> OnReceive { get; }
        IObservable<Unit> OnConnect { get; }
    }
}