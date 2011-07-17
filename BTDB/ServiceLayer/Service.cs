using System;
using System.Collections.Generic;

namespace BTDB.ServiceLayer
{
    public class Service : IService
    {
        readonly IChannel _channel;

        public Service(IChannel channel)
        {
            _channel = channel;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public T QueryService<T>() where T : class
        {
            throw new NotImplementedException();
        }

        public object QueryService(Type serviceType)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<object> EnumServices()
        {
            throw new NotImplementedException();
        }

        public void RegisterService(object service)
        {
            throw new NotImplementedException();
        }

        public void UnregisterService(object service)
        {
            throw new NotImplementedException();
        }

        public IChannel Channel
        {
            get { return _channel; }
        }
    }
}