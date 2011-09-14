using System;
using BTDB.Buffer;

namespace BTDB.ServiceLayer
{
    public interface IChannel : IDisposable
    {
        void Send(ByteBuffer data);
        IObservable<ByteBuffer> OnReceive { get; }
    }
}