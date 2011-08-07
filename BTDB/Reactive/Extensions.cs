using System;

namespace BTDB.Reactive
{
    static public class Extensions
    {
        public static IDisposable FastSubscribe<T>(this IObservable<T> source, Action<T> onNext)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (onNext == null)
            {
                throw new ArgumentNullException("onNext");
            }
            return source.Subscribe(new AnonymousObserver<T>(onNext));
        }

        public static IDisposable FastSubscribe<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (onNext == null)
            {
                throw new ArgumentNullException("onNext");
            }
            if (onError == null)
            {
                throw new ArgumentNullException("onError");
            }
            return source.Subscribe(new AnonymousObserver<T>(onNext, onError));
        }

        public static IDisposable FastSubscribe<T>(this IObservable<T> source, Action<T> onNext, Action onCompleted)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (onNext == null)
            {
                throw new ArgumentNullException("onNext");
            }
            if (onCompleted == null)
            {
                throw new ArgumentNullException("onCompleted");
            }
            return source.Subscribe(new AnonymousObserver<T>(onNext, onCompleted));
        }

        public static IDisposable FastSubscribe<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }
            if (onNext == null)
            {
                throw new ArgumentNullException("onNext");
            }
            if (onError == null)
            {
                throw new ArgumentNullException("onError");
            }
            if (onCompleted == null)
            {
                throw new ArgumentNullException("onCompleted");
            }
            return source.Subscribe(new AnonymousObserver<T>(onNext, onError, onCompleted));
        }
    }
}
