using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.EventStoreLayer;
using Disruptor;

namespace SimpleTester
{
    public class EventStorageSpeedTestAwaitable
    {
        const int RepetitionCount = 10000;
        const int ParallelTasks = 1000;

        public class MyAwaitable : INotifyCompletion
        {
            volatile Action _continuation;

            public MyAwaitable GetAwaiter() { return this; }
            public bool IsCompleted { get { return false; } }
            public void GetResult() { }

            public void OnCompleted(Action continuation)
            {
                _continuation = continuation;
            }

            public void RunContinuation()
            {
                while (_continuation == null)
                {
                    Thread.Yield();
                }
                Task.Run(_continuation);
                _continuation = null;
            }
        }

        public class Event
        {
            public ulong Id { get; set; }
            public DateTime Time { get; set; }
            public string Payload { get; set; }
        }

        readonly Stopwatch _sw = new Stopwatch();
        IWriteEventStore _writeStore;

        RingBuffer<ValueEvent> _ring;
        ISequenceBarrier _sequenceBarrier;
        Sequence _sequence;

        public MyAwaitable PublishEvent(object obj)
        {
            var seq = _ring.Next();
            var valueEvent = _ring[seq];
            valueEvent.Event = obj;
            _ring.Publish(seq);
            return valueEvent.Awaitable;
        }

        public class ValueEvent
        {
            public ValueEvent()
            {
                Awaitable = new MyAwaitable();
            }
            public object Event;
            public readonly MyAwaitable Awaitable;
        }

        public void EventConsumer()
        {
            var lw = new ReadOnlyListRingWrapper(_ring);
            var nextSequence = _sequence.Value + 1L;
            while (true)
            {
                try
                {
                    var availableSequence = _sequenceBarrier.WaitFor(nextSequence);
                    var count = (int)(availableSequence - nextSequence + 1);
                    if (count == 0)
                    {
                        continue;
                    }
                    lw.SetStartAndCount(nextSequence, count);
                    nextSequence = availableSequence + 1;
                    _writeStore.Store(null, lw);
                    lw.RunContinuations();
                    _sequence.LazySet(nextSequence - 1L);
                }
                catch (AlertException)
                {
                    break;
                }
            }
        }

        class ReadOnlyListRingWrapper : IReadOnlyList<object>
        {
            readonly RingBuffer<ValueEvent> _ring;
            long _start;

            public ReadOnlyListRingWrapper(RingBuffer<ValueEvent> ring)
            {
                _ring = ring;
            }

            public void SetStartAndCount(long start, int count)
            {
                _start = start;
                Count = count;
            }

            public void RunContinuations()
            {
                for (var i = 0; i < Count; i++)
                {
                    var valueEvent = _ring[_start + i];
                    valueEvent.Awaitable.RunContinuation();
                    valueEvent.Event = null;
                }
            }

            public IEnumerator<object> GetEnumerator()
            {
                for (int i = 0; i < Count; i++)
                {
                    yield return this[i];
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public int Count { get; private set; }

            public object this[int index]
            {
                get { return _ring[_start + index].Event; }
            }
        }

        public void Run()
        {
            _ring = new RingBuffer<ValueEvent>(() => new ValueEvent(), new MultiThreadedClaimStrategy(2048), new YieldingWaitStrategy());
            _sequenceBarrier = _ring.NewBarrier();
            _sequence = new Sequence(Sequencer.InitialCursorValue);
            _ring.SetGatingSequences(_sequence);
            var manager = new EventStoreManager();
            //manager.CompressionStrategy = new NoCompressionStrategy();
            using (var stream = new FileStream("0.event", FileMode.Create, FileAccess.ReadWrite, FileShare.None, 1))
            {
                var file = new StreamEventFileStorage(stream);
                _writeStore = manager.AppendToStore(file);
                var consumerTask = Task.Factory.StartNew(EventConsumer,
                                                         TaskCreationOptions.LongRunning |
                                                         TaskCreationOptions.HideScheduler);
                _sw.Start();
                var tasks = new Task[ParallelTasks];
                Parallel.For(0, tasks.Length, i =>
                    {
                        tasks[i] = PublishSampleEvents(RepetitionCount);
                    });
                Task.WaitAll(tasks);
                _sequenceBarrier.Alert();
                consumerTask.Wait();
                _sw.Stop();
                Console.WriteLine("Write {0}ms events per second:{1:f0} total len:{2}", _sw.ElapsedMilliseconds,
                                  tasks.Length * RepetitionCount / _sw.Elapsed.TotalSeconds, stream.Length);
                _sw.Restart();
                var allObserverCounter = new AllObserverCounter();
                manager.OpenReadOnlyStore(file).ReadFromStartToEnd(allObserverCounter);
                _sw.Stop();
                Console.WriteLine("Read {0}ms events per second:{1:f0} events:{2}", _sw.ElapsedMilliseconds,
                                  allObserverCounter.Count / _sw.Elapsed.TotalSeconds, allObserverCounter.Count);
            }
        }

        public class AllObserverCounter : IEventStoreObserver
        {
            ulong _count;

            public bool ObservedMetadata(object metadata, uint eventCount)
            {
                return true;
            }

            public bool ShouldStopReadingNextEvents()
            {
                return false;
            }

            public void ObservedEvents(object[] events)
            {
                if (events != null) _count += (ulong)events.Length;
            }

            public ulong Count
            {
                get { return _count; }
            }
        }

        async Task PublishSampleEvents(int count)
        {
            for (var i = 0; i < count; i++)
            {
                var e = new Event { Id = (ulong)i, Time = DateTime.UtcNow, Payload = "Payload" };
                await PublishEvent(e);
            }
        }
    }
}