using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Reactive;
using Xunit;

namespace BTDBTest;

// Compares FastSubject and FastBehaviourSubject with a model under random subscriptions, unsubscriptions, values,
// completion, errors and disposal. Values go to the current observers in subscription order; a behaviour subject also
// sends its last value to a new observer.
public class FastSubjectDifferentialTest
{
    sealed class Recorder(int id, List<string> log) : IObserver<int>
    {
        public void OnNext(int value) => log.Add($"{id}:{value}");
        public void OnError(Exception error) => log.Add($"{id}:E:{error.Message}");
        public void OnCompleted() => log.Add($"{id}:C");
    }

    public static IEnumerable<object[]> Cases() =>
        Enumerable.Range(1, 40).SelectMany(seed => new[] { new object[] { seed, false }, new object[] { seed, true } });

    [Theory]
    [MemberData(nameof(Cases))]
    public void MatchesModel(int seed, bool behaviour)
    {
        var random = new Random(seed);
        int? initial = behaviour && random.Next(2) == 0 ? -1 : null;
        IObservable<int> observable;
        IObserver<int> observer;
        IDisposable disposable;
        if (behaviour)
        {
            var subject = initial.HasValue ? new FastBehaviourSubject<int>(initial.Value) : new FastBehaviourSubject<int>();
            (observable, observer, disposable) = (subject, subject, subject);
        }
        else
        {
            var subject = new FastSubject<int>();
            (observable, observer, disposable) = (subject, subject, subject);
        }

        var log = new List<string>();
        var expected = new List<string>();
        var subscriptions = new List<(int Id, IDisposable Subscription)>();
        var active = new List<int>();
        var value = initial;
        string? stopped = null;
        var nextId = 0;
        for (var step = 0; step < 300; step++)
        {
            switch (random.Next(stopped == null ? 10 : 12))
            {
                case < 4:
                {
                    var id = nextId++;
                    if (stopped == "D")
                    {
                        Assert.Throws<ObjectDisposedException>(() => observable.Subscribe(new Recorder(id, log)));
                        break;
                    }

                    subscriptions.Add((id, observable.Subscribe(new Recorder(id, log))));
                    if (stopped != null) expected.Add($"{id}:{stopped}");
                    else
                    {
                        active.Add(id);
                        if (behaviour && value.HasValue) expected.Add($"{id}:{value}");
                    }

                    break;
                }
                case < 6:
                {
                    if (subscriptions.Count == 0) break;
                    var (id, subscription) = subscriptions[random.Next(subscriptions.Count)];
                    subscription.Dispose();
                    active.Remove(id);
                    break;
                }
                case < 9:
                {
                    var v = step;
                    if (stopped == "D")
                    {
                        Assert.Throws<ObjectDisposedException>(() => observer.OnNext(v));
                        break;
                    }

                    observer.OnNext(v);
                    if (stopped != null) break;
                    value = v;
                    expected.AddRange(active.Select(id => $"{id}:{v}"));
                    break;
                }
                default:
                {
                    if (stopped == "D") break;
                    var kind = random.Next(3);
                    if (kind == 2)
                    {
                        disposable.Dispose();
                        stopped = "D";
                        active.Clear();
                        break;
                    }

                    if (kind == 0) observer.OnCompleted();
                    else observer.OnError(new InvalidOperationException("e" + step));
                    if (stopped != null) break;
                    stopped = kind == 0 ? "C" : "E:e" + step;
                    expected.AddRange(active.Select(id => $"{id}:{stopped}"));
                    active.Clear();
                    break;
                }
            }

            Assert.Equal(expected, log);
        }
    }
}
