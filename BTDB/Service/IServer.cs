using System;

namespace BTDB.Service
{
    public interface IServer
    {
        Action<IChannel> NewClient { set; }
        void StartListening(int backLog = 10);
        void StopListening();
    }
}