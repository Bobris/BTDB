using System;
using System.Collections.Generic;

namespace BTDB.ServiceLayer
{
    public interface IServiceClient : IDisposable
    {
        T QueryService<T>() where T : class;
        object QueryService(Type serviceType);
        IEnumerable<object> EnumServices();
    }
}