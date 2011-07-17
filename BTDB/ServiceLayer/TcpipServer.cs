using System;

namespace BTDB.ServiceLayer
{
    public class TcpipServer : IServer
    {
        public Action<IChannel> NewClient
        {
            set { throw new NotImplementedException(); }
        }

        public void StartListening()
        {
            throw new NotImplementedException();
        }

        public void StopListening()
        {
            throw new NotImplementedException();
        }
    }
}