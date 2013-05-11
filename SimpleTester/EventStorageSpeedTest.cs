using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.EventStoreLayer;

namespace SimpleTester
{
    public class EventStorageSpeedTest
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
        readonly BlockingCollection<ValueEvent> _bc = new BlockingCollection<ValueEvent>(1024);

        public Task PublishEvent(object obj)
        {
            var tcs = new TaskCompletionSource<bool>();
            _bc.Add(new ValueEvent { Event = obj, TaskCompletionSource = tcs });
            return tcs.Task;
        }

        public struct ValueEvent
        {
            public object Event;
            public TaskCompletionSource<bool> TaskCompletionSource;
        }

        public void EventConsumer()
        {
            var l = new List<object>();
            var t = new List<TaskCompletionSource<bool>>();
            while (!_bc.IsCompleted)
            {
                l.Clear();
                t.Clear();
                ValueEvent valueEvent;
                var wait = -1;
                while (_bc.TryTake(out valueEvent, wait))
                {
                    l.Add(valueEvent.Event);
                    t.Add(valueEvent.TaskCompletionSource);
                    wait = 0;
                }
                if (l.Count == 0) continue;
                _writeStore.Store(null, l);
                foreach (var tcs in t)
                {
                    Task.Factory.StartNew(o => ((TaskCompletionSource<bool>)o).SetResult(true), tcs, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                }
            }
        }

        public void Run()
        {
            var manager = new EventStoreManager();
            using (var stream = new FileStream("0.event", FileMode.Create, FileAccess.ReadWrite, FileShare.None, 1))
            {
                var file = new StreamEventFileStorage(stream);
                _writeStore = manager.AppendToStore(file);
                var consumerTask = Task.Factory.StartNew(EventConsumer, TaskCreationOptions.LongRunning | TaskCreationOptions.HideScheduler);
                _sw.Start();
                var tasks = new Task[ParallelTasks];
                Parallel.For(0, tasks.Length, i =>
                {
                    tasks[i] = PublishSampleEvents(RepetitionCount);
                });
                Task.WaitAll(tasks);
                _bc.CompleteAdding();
                consumerTask.Wait();
                _sw.Stop();
                Console.WriteLine("Write {0}ms events per second:{1:f0} total len:{2}", _sw.ElapsedMilliseconds, tasks.Length * RepetitionCount / _sw.Elapsed.TotalSeconds, stream.Length);
                _sw.Restart();
                var allObserverCounter = new AllObserverCounter();
                manager.OpenReadOnlyStore(file).ReadFromStartToEnd(allObserverCounter);
                _sw.Stop();
                Console.WriteLine("Read {0}ms events per second:{1:f0} events:{2}", _sw.ElapsedMilliseconds, allObserverCounter.Count / _sw.Elapsed.TotalSeconds, allObserverCounter.Count);
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