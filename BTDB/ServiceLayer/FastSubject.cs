using System;
using System.Reactive.Disposables;
using System.Reactive.Subjects;

namespace BTDB.ServiceLayer
{
    public class FastSubject<T> : ISubject<T>
    {
        Action _onCompleted;
        Action<Exception> _onError;
        Action<T> _onNext;

        public void OnCompleted()
        {
            var e = _onCompleted;
            if (e != null) e();
        }

        public void OnError(Exception error)
        {
            var e = _onError;
            if (e != null) e(error);
        }

        public void OnNext(T value)
        {
            var e = _onNext;
            if (e != null) e(value);
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            _onCompleted += observer.OnCompleted;
            _onError += observer.OnError;
            _onNext += observer.OnNext;

            return Disposable.Create(() =>
                {
                    _onCompleted -= observer.OnCompleted;
                    _onError -= observer.OnError;
                    _onNext -= observer.OnNext;
                });
        }
    }
}