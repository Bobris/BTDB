using System;
using System.Collections.Generic;

namespace BTDB.ServiceLayer
{
    public class Service : IService
    {
        readonly IChannel _channel;

        readonly object _serverServiceLock = new object();
        readonly Dictionary<object, ulong> _serverServices = new Dictionary<object, ulong>();
        ulong _lastServerServiceId;

        public Service(IChannel channel)
        {
            _channel = channel;
            _lastServerServiceId = 0;
        }

        public void Dispose()
        {
            _channel.Dispose();
        }

        public T QueryOtherService<T>() where T : class
        {
            return (T) QueryOtherService(typeof (T));
        }

        public object QueryOtherService(Type serviceType)
        {
            throw new NotImplementedException();
        }

        public void RegisterMyService(object service)
        {
            lock(_serverServiceLock)
            {
                var serviceId = ++_lastServerServiceId;
                _serverServices.Add(service, serviceId);
            }
        }

        public void UnregisterMyService(object service)
        {
            lock(_serverServiceLock)
            {
                ulong serviceId;
                if (_serverServices.TryGetValue(service, out serviceId))
                {
                    _serverServices.Remove(service);
                }
            }
        }

        public IChannel Channel
        {
            get { return _channel; }
        }
    }
}