using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using BTDB.Reactive;
using Xunit;

namespace BTDBTest;

/// Compares FastSubject/FastBehaviourSubject with System.Reactive Subject/BehaviorSubject semantics.
public class FastSubjectTest
{
    interface ISubjectUnderTest : IObservable<int>, IObserver<int>, IDisposable
    {
    }

    sealed class Wrap<TSubject>(TSubject subject) : ISubjectUnderTest
        where TSubject : IObservable<int>, IObserver<int>, IDisposable
    {
        public IDisposable Subscribe(IObserver<int> observer) => subject.Subscribe(observer);
        public void OnCompleted() => subject.OnCompleted();
        public void OnError(Exception error) => subject.OnError(error);
        public void OnNext(int value) => subject.OnNext(value);
        public void Dispose() => subject.Dispose();
    }

    sealed class Harness
    {
        readonly ISubjectUnderTest _subject;
        readonly List<string> _log = new();
        readonly Dictionary<int, IDisposable> _subscriptions = new();
        int _nextId;
        readonly HashSet<int> _spawnedForValue = new();

        public Harness(ISubjectUnderTest subject) => _subject = subject;

        public List<string> Log => _log;

        sealed class Observer(Harness owner, int id) : IObserver<int>
        {
            public void OnCompleted() => owner._log.Add($"{id}:C");
            public void OnError(Exception error) => owner._log.Add($"{id}:E:{error.Message}");

            public void OnNext(int value)
            {
                owner._log.Add($"{id}:N:{value}");
                // values divisible by 7 make observer unsubscribe itself, values divisible by 11 subscribe new one
                if (value % 7 == 0) owner.Unsubscribe(id);
                if (value % 11 == 0 && owner._spawnedForValue.Add(value)) owner.Subscribe();
            }
        }

        void Try(string what, Action action)
        {
            try
            {
                action();
                _log.Add(what);
            }
            catch (Exception e)
            {
                _log.Add(what + " threw " + e.GetType().Name);
            }
        }

        public void Subscribe()
        {
            var id = _nextId++;
            Try($"sub {id}", () => _subscriptions[id] = _subject.Subscribe(new Observer(this, id)));
        }

        public void Unsubscribe(int id)
        {
            if (_subscriptions.Remove(id, out var subscription))
            {
                Try($"unsub {id}", subscription.Dispose);
                // second dispose must be no-op
                subscription.Dispose();
            }
        }

        public void Step(Random rnd, int step)
        {
            switch (rnd.Next(20))
            {
                case < 5:
                    Subscribe();
                    break;
                case < 8:
                    if (_nextId > 0) Unsubscribe(rnd.Next(_nextId));
                    break;
                case < 17:
                    Try($"next {step}", () => _subject.OnNext(step));
                    break;
                case 17:
                    Try("completed", _subject.OnCompleted);
                    break;
                case 18:
                    Try("error", () => _subject.OnError(new InvalidOperationException("err" + step)));
                    break;
                default:
                    if (rnd.Next(4) == 0) Try("dispose", _subject.Dispose);
                    break;
            }
        }
    }

    static void CompareRandomScripts(Func<ISubjectUnderTest> createFast, Func<ISubjectUnderTest> createRx)
    {
        for (var seed = 0; seed < 300; seed++)
        {
            var fast = new Harness(createFast());
            var rx = new Harness(createRx());
            var rndFast = new Random(seed);
            var rndRx = new Random(seed);
            for (var step = 0; step < 60; step++)
            {
                fast.Step(rndFast, step);
                rx.Step(rndRx, step);
            }

            Assert.Equal(rx.Log, fast.Log);
        }
    }

    [Fact]
    public void FastSubjectBehavesLikeRxSubject()
    {
        CompareRandomScripts(() => new Wrap<FastSubject<int>>(new()), () => new Wrap<Subject<int>>(new()));
    }

    [Fact]
    public void FastBehaviourSubjectWithValueBehavesLikeRxBehaviorSubject()
    {
        CompareRandomScripts(() => new Wrap<FastBehaviourSubject<int>>(new(-1)),
            () => new Wrap<BehaviorSubject<int>>(new(-1)));
    }

    [Fact]
    public void FastBehaviourSubjectWithoutValueBehavesLikeRxSubjectUntilFirstValue()
    {
        var subject = new FastBehaviourSubject<int>();
        var log = new List<string>();
        var first = subject.Subscribe(new AnonymousObserver<int>(v => log.Add("1:" + v), () => log.Add("1:C")));
        var second = subject.Subscribe(new AnonymousObserver<int>(v => log.Add("2:" + v), () => log.Add("2:C")));
        Assert.Empty(log);
        subject.OnNext(5);
        Assert.Equal(new[] { "1:5", "2:5" }, log);
        log.Clear();
        var third = subject.Subscribe(new AnonymousObserver<int>(v => log.Add("3:" + v), () => log.Add("3:C")));
        Assert.Equal(new[] { "3:5" }, log);
        log.Clear();
        second.Dispose();
        third.Dispose();
        first.Dispose();
        subject.OnNext(6);
        Assert.Empty(log);
        var fourth = subject.Subscribe(new AnonymousObserver<int>(v => log.Add("4:" + v), () => log.Add("4:C")));
        Assert.Equal(new[] { "4:6" }, log);
        log.Clear();
        subject.OnCompleted();
        subject.OnCompleted();
        Assert.Equal(new[] { "4:C" }, log);
        fourth.Dispose();
    }

    [Fact]
    public void FastBehaviourSubjectWithoutValueSingleObserverGetsValues()
    {
        var subject = new FastBehaviourSubject<int>();
        var log = new List<int>();
        var subscription = subject.Subscribe(new AnonymousObserver<int>(log.Add, () => { }));
        subject.OnNext(1);
        subject.OnNext(2);
        Assert.Equal(new[] { 1, 2 }, log);
        subscription.Dispose();
        subject.OnNext(3);
        Assert.Equal(new[] { 1, 2 }, log);
        var unsubscribedOther = subject.Subscribe(new AnonymousObserver<int>(_ => { }, () => { }));
        // disposing unrelated observer does not change anything
        subject.Unsubscribe(new AnonymousObserver<int>(_ => { }, () => { }));
        unsubscribedOther.Dispose();
    }

    [Fact]
    public void UnsubscribeOfUnknownObserverIsIgnored()
    {
        var unknown = new AnonymousObserver<int>(_ => { }, () => { });
        foreach (var subject in new IObserver<int>[] { new FastSubject<int>(), new FastBehaviourSubject<int>(1), new FastBehaviourSubject<int>() })
        {
            var log = new List<int>();
            var unsubscribable = (FastSubjectHelpers<int>.IUnsubscribableSubject)subject;
            var observable = (IObservable<int>)subject;
            unsubscribable.Unsubscribe(unknown);
            observable.Subscribe(new AnonymousObserver<int>(log.Add, () => { }));
            unsubscribable.Unsubscribe(unknown);
            observable.Subscribe(new AnonymousObserver<int>(log.Add, () => { }));
            unsubscribable.Unsubscribe(unknown);
            subject.OnNext(10);
            Assert.Contains(10, log);
            Assert.Equal(2, log.FindAll(v => v == 10).Count);
        }
    }

    [Fact]
    public void ArgumentValidation()
    {
        var subject = new FastSubject<int>();
        Assert.Throws<ArgumentNullException>(() => subject.Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => subject.OnError(null!));
        var behaviour = new FastBehaviourSubject<int>();
        Assert.Throws<ArgumentNullException>(() => behaviour.Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => behaviour.OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new AnonymousObserver<int>(null!, () => { }));
        Assert.Throws<ArgumentNullException>(() => new AnonymousObserver<int>(_ => { }, null!));
    }

    [Fact]
    public void AnonymousObserverCompletesOnlyOnce()
    {
        var values = new List<int>();
        var completed = 0;
        var observer = new AnonymousObserver<int>(values.Add, () => completed++);
        observer.OnNext(1);
        observer.OnCompleted();
        observer.OnCompleted();
        observer.OnNext(2);
        Assert.Equal(new[] { 1 }, values);
        Assert.Equal(1, completed);
        var error = new Exception("x");
        var ex = Assert.Throws<InvalidOperationException>(() => observer.OnError(error));
        Assert.Same(error, ex.InnerException);
    }

    [Fact]
    public void EmptyDisposableAndMarkersDoNothing()
    {
        BTDB.Reactive.Disposable.Empty.Dispose();
        IObserver<int> completed = FastSubjectHelpers<int>.CompletedSubjectMarker;
        completed.OnNext(1);
        completed.OnError(new Exception());
        completed.OnCompleted();
        IObserver<int> empty = FastSubjectHelpers<int>.EmptySubjectMarker;
        empty.OnNext(1);
        empty.OnError(new Exception());
        empty.OnCompleted();
        IObserver<int> exceptioned = new FastSubjectHelpers<int>.ExceptionedSubject(new Exception());
        exceptioned.OnNext(1);
        exceptioned.OnError(new Exception());
        exceptioned.OnCompleted();
        IObserver<int> emptyWithValue = new FastSubjectHelpers<int>.EmptySubjectWithValue(1);
        emptyWithValue.OnNext(1);
        emptyWithValue.OnError(new Exception());
        emptyWithValue.OnCompleted();
        IObserver<int> disposed = FastSubjectHelpers<int>.DisposedSubjectMarker;
        Assert.Throws<ObjectDisposedException>(() => disposed.OnNext(1));
        Assert.Throws<ObjectDisposedException>(() => disposed.OnError(new Exception()));
        Assert.Throws<ObjectDisposedException>(() => disposed.OnCompleted());
    }

    [Fact]
    public void ErrorIsPropagatedToAllObserversAndLateSubscribers()
    {
        foreach (var subject in new ISubjectUnderTest[]
                 {
                     new Wrap<FastSubject<int>>(new()), new Wrap<FastBehaviourSubject<int>>(new()),
                     new Wrap<FastBehaviourSubject<int>>(new(1))
                 })
        {
            var errors = new List<string>();
            var observers = new List<IObserver<int>>();
            for (var i = 0; i < 3; i++)
            {
                var id = i;
                var observer = System.Reactive.Observer.Create<int>(_ => { }, e => errors.Add(id + e.Message));
                observers.Add(observer);
                subject.Subscribe(observer);
            }

            subject.OnNext(2);
            subject.OnError(new Exception("boom"));
            subject.OnError(new Exception("ignored"));
            subject.OnCompleted();
            subject.OnNext(3);
            subject.Subscribe(System.Reactive.Observer.Create<int>(_ => { }, e => errors.Add("late" + e.Message)));
            Assert.Equal(new[] { "0boom", "1boom", "2boom", "lateboom" }, errors);
        }
    }
}
