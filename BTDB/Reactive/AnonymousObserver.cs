using System;
using System.Threading;

namespace BTDB.Reactive
{
    internal class AnonymousObserver<T> : IObserver<T>
    {
        static readonly Action<T> StoppedMark;
        static readonly Action<Exception> DefaultOnError;
        static readonly Action DefaultOnCompleted;

        Action<T> _onNext;
        Action<Exception> _onError;
        Action _onCompleted;

        static AnonymousObserver()
        {
            StoppedMark = _ => { };
            DefaultOnError = e => { throw e; };
            DefaultOnCompleted = () => { };
        }

        public AnonymousObserver(Action<T> onNext)
        {
            _onNext = onNext;
            _onError = DefaultOnError;
            _onCompleted = DefaultOnCompleted;
        }

        public AnonymousObserver(Action<T> onNext, Action<Exception> onError)
        {
            _onNext = onNext;
            _onError = onError;
            _onCompleted = DefaultOnCompleted;
        }

        public AnonymousObserver(Action<T> onNext, Action onCompleted)
        {
            _onNext = onNext;
            _onError = DefaultOnError;
            _onCompleted = onCompleted;
        }

        public AnonymousObserver(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            _onNext = onNext;
            _onError = onError;
            _onCompleted = onCompleted;
        }

        public void OnNext(T value)
        {
            _onNext(value);
        }

        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _onNext, StoppedMark) != StoppedMark)
            {
                _onError(error);
                _onError = null;
                _onCompleted = null;
            }
        }

        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _onNext, StoppedMark) != StoppedMark)
            {
                _onCompleted();
                _onError = null;
                _onCompleted = null;
            }
        }
    }
}
