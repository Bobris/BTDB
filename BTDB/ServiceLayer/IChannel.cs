using System;
using System.Threading.Tasks;

namespace BTDB.ServiceLayer
{
    public interface IChannel : IDisposable
    {
        Action<IChannel> StatusChanged { set; }
        void Send(byte[] data);
        Task<byte[]> Receive();
        ChannelStatus Status { get; }
    }
}