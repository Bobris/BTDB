using System;

namespace BTDB.Service
{
    public interface IServer
    {
        Action<IChannel> NewClient { set; }
        void StartListening();
        void StopListening();
    }
}