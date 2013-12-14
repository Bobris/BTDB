using System;

namespace BTDB.DtoChannel
{
    public interface IDtoChannel : IDisposable
    {
        void Send(object dto);
        IObservable<object> OnReceive { get; }
    }
}