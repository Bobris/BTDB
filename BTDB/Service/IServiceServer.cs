using System;

namespace BTDB.Service
{
    public interface IServiceServer : IDisposable
    {
        void RegisterMyService(object service);
        void UnregisterMyService(object service);
    }
}