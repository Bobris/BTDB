using System;

namespace BTDB.ServiceLayer
{
    public interface IServiceServer : IDisposable
    {
        void RegisterMyService(object service);
        void UnregisterMyService(object service);
    }
}