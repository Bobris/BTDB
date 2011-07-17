using System;

namespace BTDB.ServiceLayer
{
    public interface IServer
    {
        Action<IChannel> NewClient { set; }
        void StartListening();
        void StopListening();
    }
}