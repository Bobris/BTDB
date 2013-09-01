using System;
using BTDB.Buffer;

namespace BTDB.Service
{
    public interface IChannel : IDisposable
    {
        void Send(ByteBuffer data);
        IObservable<ByteBuffer> OnReceive { get; }
        IObservable<bool> OnConnect { get; }
    }
}