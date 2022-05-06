using System;
using System.Threading;

namespace BTDB.Reactive;

public class AnonymousObserver<T> : IObserver<T>
{
    Action<T> _onNext;
    Action _onCompleted;

    public AnonymousObserver(Action<T> onNext, Action onCompleted)
    {
        if (onNext == null) throw new ArgumentNullException(nameof(onNext));
        if (onCompleted == null) throw new ArgumentNullException(nameof(onCompleted));
        _onNext = onNext;
        _onCompleted = onCompleted;
    }

    public void OnNext(T value)
    {
        var onNext = _onNext;
        if (onNext != null)
            onNext(value);
    }

    public void OnError(Exception error)
    {
        throw new InvalidOperationException("OnError was not implemented on this subject", error);
    }

    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _onNext, null) != null)
        {
            _onCompleted();
            _onCompleted = null;
        }
    }
}
