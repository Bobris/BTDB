using System;
using System.Threading;

namespace BTDB.Reactive;

public sealed class FastSubject<T> : IObservable<T>, IObserver<T>, IDisposable, FastSubjectHelpers<T>.IUnsubscribableSubject
{
    volatile IObserver<T> _current;

    public FastSubject()
    {
        _current = FastSubjectHelpers<T>.EmptySubjectMarker;
    }

    public void Dispose()
    {
        _current = FastSubjectHelpers<T>.DisposedSubjectMarker;
    }

    // disable invalid warning about using volatile inside Interlocked.CompareExchange
#pragma warning disable 420

    public void OnCompleted()
    {
        IObserver<T> original;
        do
        {
            original = _current;
            if (original is IStoppedSubjectMarker) break;
        } while (Interlocked.CompareExchange(ref _current, FastSubjectHelpers<T>.CompletedSubjectMarker, original) != original);
        original.OnCompleted();
    }

    public void OnError(Exception error)
    {
        if (error == null)
        {
            throw new ArgumentNullException(nameof(error));
        }
        IObserver<T> original;
        do
        {
            original = _current;
            if (original is IStoppedSubjectMarker) break;
        } while (Interlocked.CompareExchange(ref _current, new FastSubjectHelpers<T>.ExceptionedSubject(error), original) != original);
        original.OnError(error);
    }

    public void OnNext(T value)
    {
        _current.OnNext(value);
    }

    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }
        IObserver<T> original;
        IObserver<T> nextState;
        do
        {
            original = _current;
            if (original == FastSubjectHelpers<T>.DisposedSubjectMarker) throw new ObjectDisposedException("");
            if (original == FastSubjectHelpers<T>.CompletedSubjectMarker)
            {
                observer.OnCompleted();
                return Disposable.Empty;
            }
            if (original is FastSubjectHelpers<T>.ExceptionedSubject)
            {
                observer.OnError(((FastSubjectHelpers<T>.ExceptionedSubject)original).Error);
                return Disposable.Empty;
            }
            if (original == FastSubjectHelpers<T>.EmptySubjectMarker)
            {
                nextState = observer;
            }
            else if (original is FastSubjectHelpers<T>.MultiSubject)
            {
                var originalArray = ((FastSubjectHelpers<T>.MultiSubject)original).Array;
                var originalCount = originalArray.Length;
                var newArray = new IObserver<T>[originalCount + 1];
                Array.Copy(originalArray, newArray, originalCount);
                newArray[originalCount] = observer;
                nextState = new FastSubjectHelpers<T>.MultiSubject(newArray);
            }
            else
            {
                nextState = new FastSubjectHelpers<T>.MultiSubject(new[] { original, observer });
            }
        } while (Interlocked.CompareExchange(ref _current, nextState, original) != original);
        return new FastSubjectHelpers<T>.Subscription(this, observer);
    }

    public void Unsubscribe(IObserver<T> observer)
    {
        IObserver<T> original;
        IObserver<T> nextState;
        do
        {
            original = _current;
            if (original is IStoppedSubjectMarker) return;
            if (original is FastSubjectHelpers<T>.MultiSubject)
            {
                var originalArray = ((FastSubjectHelpers<T>.MultiSubject)original).Array;
                var indexOf = Array.IndexOf(originalArray, observer);
                if (indexOf < 0) return;
                if (originalArray.Length == 2)
                {
                    nextState = originalArray[1 - indexOf];
                }
                else
                {
                    var newArray = new IObserver<T>[originalArray.Length - 1];
                    Array.Copy(originalArray, newArray, indexOf);
                    Array.Copy(originalArray, indexOf + 1, newArray, indexOf, newArray.Length - indexOf);
                    nextState = new FastSubjectHelpers<T>.MultiSubject(newArray);
                }
            }
            else
            {
                if (original != observer) return;
                nextState = FastSubjectHelpers<T>.EmptySubjectMarker;
            }
        } while (Interlocked.CompareExchange(ref _current, nextState, original) != original);
    }
}
