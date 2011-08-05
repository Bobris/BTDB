using System;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Threading;

namespace BTDB.Reactive
{
    interface IStoppedSubjectMarker { }

    public sealed class FastSubject<T> : ISubject<T>, IDisposable
    {
        static readonly DisposedSubject DisposedSubjectMarker = new DisposedSubject();
        sealed class DisposedSubject : IObserver<T>, IStoppedSubjectMarker
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

        sealed class ExceptionedSubject : IObserver<T>, IStoppedSubjectMarker
        {
            readonly Exception _error;

            public ExceptionedSubject(Exception error)
            {
                _error = error;
            }

            internal Exception Error { get { return _error; } }

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

        static readonly CompletedSubject CompletedSubjectMarker = new CompletedSubject();
        sealed class CompletedSubject : IObserver<T>, IStoppedSubjectMarker
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

        static readonly EmptySubject EmptySubjectMarker = new EmptySubject();
        sealed class EmptySubject : IObserver<T>
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

        sealed class MultiSubject : IObserver<T>
        {
            readonly IObserver<T>[] _array;

            public MultiSubject(IObserver<T>[] array)
            {
                _array = array;
            }

            public IObserver<T>[] Array
            {
                get { return _array; }
            }

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

        volatile IObserver<T> _current;

        public FastSubject()
        {
            _current = EmptySubjectMarker;
        }

        public void Dispose()
        {
            _current = DisposedSubjectMarker;
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
            } while (Interlocked.CompareExchange(ref _current, CompletedSubjectMarker, original) != original);
            original.OnCompleted();
        }

        public void OnError(Exception error)
        {
            if (error == null)
            {
                throw new ArgumentNullException("error");
            }
            IObserver<T> original;
            do
            {
                original = _current;
                if (original is IStoppedSubjectMarker) break;
            } while (Interlocked.CompareExchange(ref _current, new ExceptionedSubject(error), original) != original);
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
                throw new ArgumentNullException("observer");
            }
            IObserver<T> original;
            IObserver<T> nextState;
            do
            {
                original = _current;
                if (original == DisposedSubjectMarker) throw new ObjectDisposedException("");
                if (original == CompletedSubjectMarker)
                {
                    observer.OnCompleted();
                    return Disposable.Empty;
                }
                if (original is ExceptionedSubject)
                {
                    observer.OnError(((ExceptionedSubject)original).Error);
                    return Disposable.Empty;
                }
                if (original == EmptySubjectMarker)
                {
                    nextState = observer;
                }
                else if (original is MultiSubject)
                {
                    var originalArray = ((MultiSubject)original).Array;
                    var originalCount = originalArray.Length;
                    var newArray = new IObserver<T>[originalCount + 1];
                    Array.Copy(originalArray, newArray, originalCount);
                    newArray[originalCount] = observer;
                    nextState = new MultiSubject(newArray);
                }
                else
                {
                    nextState = new MultiSubject(new[] { original, observer });
                }
            } while (Interlocked.CompareExchange(ref _current, nextState, original) != original);
            return new Subscription(this, observer);
        }

        void Unsubscribe(IObserver<T> observer)
        {
            IObserver<T> original;
            IObserver<T> nextState;
            do
            {
                original = _current;
                if (original is IStoppedSubjectMarker) return;
                if (original is MultiSubject)
                {
                    var originalArray = ((MultiSubject)original).Array;
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
                        nextState = new MultiSubject(newArray);
                    }
                }
                else
                {
                    if (original != observer) return;
                    nextState = EmptySubjectMarker;
                }
            } while (Interlocked.CompareExchange(ref _current, nextState, original) != original);
        }

        sealed class Subscription : IDisposable
        {
            IObserver<T> _observer;
            FastSubject<T> _subject;

            public Subscription(FastSubject<T> subject, IObserver<T> observer)
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
}