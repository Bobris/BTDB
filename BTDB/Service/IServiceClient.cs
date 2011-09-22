using System;

namespace BTDB.Service
{
    public interface IServiceClient : IDisposable
    {
        T QueryOtherService<T>() where T : class;
        object QueryOtherService(Type serviceType);
    }
}