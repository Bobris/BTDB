using System;

namespace BTDB.Service
{
    public interface IServiceClient : IDisposable
    {
        T QueryRemoteService<T>() where T : class;
        object QueryRemoteService(Type serviceType);
        IObservable<string> OnNewRemoteService { get; }
    }
}