using System;

namespace BTDB.ServiceLayer
{
    public interface IServiceServer : IDisposable
    {
        void RegisterService(object service);
        void UnregisterService(object service);
    }
}