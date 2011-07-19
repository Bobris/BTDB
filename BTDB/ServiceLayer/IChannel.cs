using System;
using System.Threading.Tasks;

namespace BTDB.ServiceLayer
{
    public interface IChannel : IDisposable
    {
        Action<IChannel> StatusChanged { set; }
        void Send(ArraySegment<byte> data);
        Task<ArraySegment<byte>> Receive();
        ChannelStatus Status { get; }
    }
}