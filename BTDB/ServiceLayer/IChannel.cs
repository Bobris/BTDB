using System;
using BTDB.Buffer;

namespace BTDB.ServiceLayer
{
    public interface IChannel : IDisposable
    {
        Action<IChannel> StatusChanged { set; }
        void Send(ByteBuffer data);
        IObservable<ByteBuffer> OnReceive { get; }
        ChannelStatus Status { get; }
    }
}