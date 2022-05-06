using System;
using System.Threading;

namespace BTDB.Reactive;

interface IStoppedSubjectMarker { }

static class FastSubjectHelpers<T>
{
    internal interface IUnsubscribableSubject
    {
        void Unsubscribe(IObserver<T> observer);
    }

    internal interface IHasValue
    {
        T Value { get; }
    }

    internal static readonly DisposedSubject DisposedSubjectMarker = new DisposedSubject();

    internal sealed class DisposedSubject : IObserver<T>, IStoppedSubjectMarker
    {
        public void OnNext(T value)
        {
            throw new ObjectDisposedException("");
        }

        public void OnError(Exception error)
        {
            throw new ObjectDisposedException("");
        }

        public void OnCompleted()
        {
            throw new ObjectDisposedException("");
        }
    }

    internal sealed class ExceptionedSubject : IObserver<T>, IStoppedSubjectMarker
    {
        readonly Exception _error;

        public ExceptionedSubject(Exception error)
        {
            _error = error;
        }

        internal Exception Error => _error;

        public void OnNext(T value)
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    internal static readonly CompletedSubject CompletedSubjectMarker = new CompletedSubject();

    internal sealed class CompletedSubject : IObserver<T>, IStoppedSubjectMarker
    {
        public void OnNext(T value)
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    internal static readonly EmptySubject EmptySubjectMarker = new EmptySubject();

    internal sealed class EmptySubject : IObserver<T>
    {
        public void OnNext(T value)
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    internal sealed class MultiSubject : IObserver<T>
    {
        readonly IObserver<T>[] _array;

        public MultiSubject(IObserver<T>[] array)
        {
            _array = array;
        }

        public IObserver<T>[] Array => _array;

        public void OnNext(T value)
        {
            foreach (var observer in _array)
            {
                observer.OnNext(value);
            }
        }

        public void OnError(Exception error)
        {
            foreach (var observer in _array)
            {
                observer.OnError(error);
            }
        }

        public void OnCompleted()
        {
            foreach (var observer in _array)
            {
                observer.OnCompleted();
            }
        }
    }

    internal sealed class EmptySubjectWithValue : IObserver<T>, IHasValue
    {
        readonly T _value;

        internal EmptySubjectWithValue(T value)
        {
            _value = value;
        }

        public void OnNext(T value)
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        public T Value => _value;
    }

    internal sealed class MultiSubjectWithValue : IObserver<T>, IHasValue
    {
        readonly IObserver<T>[] _array;
        readonly T _value;

        public MultiSubjectWithValue(IObserver<T>[] array, T value)
        {
            _array = array;
            _value = value;
        }

        public IObserver<T>[] Array => _array;

        public void OnNext(T value)
        {
            foreach (var observer in _array)
            {
                observer.OnNext(value);
            }
        }

        public void OnError(Exception error)
        {
            foreach (var observer in _array)
            {
                observer.OnError(error);
            }
        }

        public void OnCompleted()
        {
            foreach (var observer in _array)
            {
                observer.OnCompleted();
            }
        }

        public T Value => _value;
    }

    internal sealed class SingleSubjectWithValue : IObserver<T>, IHasValue
    {
        readonly IObserver<T> _observer;
        readonly T _value;

        public SingleSubjectWithValue(IObserver<T> observer, T value)
        {
            _observer = observer;
            _value = value;
        }

        public void OnNext(T value)
        {
            Observer.OnNext(value);
        }

        public void OnError(Exception error)
        {
            Observer.OnError(error);
        }

        public void OnCompleted()
        {
            Observer.OnCompleted();
        }

        public T Value => _value;

        internal IObserver<T> Observer => _observer;
    }


    internal sealed class Subscription : IDisposable
    {
        IObserver<T> _observer;
        IUnsubscribableSubject _subject;

        public Subscription(IUnsubscribableSubject subject, IObserver<T> observer)
        {
            _subject = subject;
            _observer = observer;
        }

        public void Dispose()
        {
            var observer = Interlocked.Exchange(ref _observer, null);
            if (observer == null) return;
            _subject.Unsubscribe(observer);
            _subject = null;
        }
    }
}
