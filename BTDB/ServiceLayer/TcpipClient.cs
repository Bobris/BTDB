using System;
using System.Threading.Tasks;

namespace BTDB.ServiceLayer
{
    public class TcpipClient : IChannel
    {
        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public Action<IChannel> StatusChanged
        {
            set { throw new NotImplementedException(); }
        }

        public void Send(byte[] data)
        {
            throw new NotImplementedException();
        }

        public Task<byte[]> Receive()
        {
            throw new NotImplementedException();
        }

        public ChannelStatus Status
        {
            get { throw new NotImplementedException(); }
        }
    }
}