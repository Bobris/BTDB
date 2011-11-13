using System;

namespace BTDB.Service
{
    public interface IServiceServer : IDisposable
    {
        void RegisterLocalService(object service);
    }
}