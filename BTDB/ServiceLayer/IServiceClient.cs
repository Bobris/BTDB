using System;

namespace BTDB.ServiceLayer
{
    public interface IServiceClient : IDisposable
    {
        T QueryOtherService<T>() where T : class;
        object QueryOtherService(Type serviceType);
    }
}