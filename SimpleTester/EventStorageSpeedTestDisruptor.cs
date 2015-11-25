using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.EventStoreLayer;
using Disruptor;

namespace SimpleTester
{
    public class EventStorageSpeedTestDisruptor
    {
        const int RepetitionCount = 10000;
        const int ParallelTasks = 1000;

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

        public Task PublishEvent(object obj)
        {
            var seq = _ring.Next();
            var tcs = new TaskCompletionSource<bool>();
            var valueEvent = _ring[seq];
            valueEvent.Event = obj;
            valueEvent.TaskCompletionSource = tcs;
            _ring.Publish(seq);
            return tcs.Task;
        }

        public class ValueEvent
        {
            public object Event;
            public TaskCompletionSource<bool> TaskCompletionSource;
        }

        public void EventConsumer()
        {
            var l = new object[_ring.BufferSize];
            var t = new TaskCompletionSource<bool>[_ring.BufferSize];
            var lw = new ReadOnlyListArrayWrapper<object>(l);

            var nextSequence = _sequence.Value + 1L;
            while (true)
            {
                try
                {
                    var availableSequence = _sequenceBarrier.WaitFor(nextSequence);
                    var count = 0;
                    while (nextSequence <= availableSequence)
                    {
                        var evt = _ring[nextSequence];
                        l[count] = evt.Event;
                        t[count] = evt.TaskCompletionSource;
                        count++;
                        nextSequence++;
                    }
                    if (count == 0)
                    {
                        continue;
                    }
                    lw.Count = count;
                    _writeStore.Store(null, lw);
                    for (var i = 0; i < count; i++)
                    {
                        Task.Factory.StartNew(o => ((TaskCompletionSource<bool>)o).SetResult(true), t[i], CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                        t[i] = null;
                        l[i] = null;
                    }
                    _sequence.LazySet(nextSequence - 1L);
                }
                catch (AlertException)
                {
                    break;
                }
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

            public ulong Count => _count;
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