using System;

namespace BTDB.Service
{
    public interface IServiceServer : IDisposable
    {
        void RegisterLocalService<T>(T service) where T : class;
        void RegisterLocalType(Type type);
    }
}