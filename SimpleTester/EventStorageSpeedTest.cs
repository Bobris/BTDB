using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using BTDB.EventStoreLayer;

namespace SimpleTester
{
    public class EventStorageSpeedTest
    {
        public class Event
        {
            public ulong Id { get; set; }
            public DateTime Time { get; set; }
            public string Payload { get; set; }
        }

        readonly Stopwatch _sw = new Stopwatch();
        IWriteEventStore _writeStore;
        readonly BlockingCollection<ValueEvent> _bc = new BlockingCollection<ValueEvent>(2048);

        public Task PublishEvent(object obj)
        {
            var tcs = new TaskCompletionSource<bool>();
            _bc.Add(new ValueEvent {Event = obj, TaskCompletionSource = tcs});
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
                while (_bc.TryTake(out valueEvent))
                {
                    l.Add(valueEvent.Event);
                    t.Add(valueEvent.TaskCompletionSource);
                }
                _writeStore.Store(null, l.ToArray());
                foreach (var tcs in t)
                {
                    Task.Run(()=>tcs.SetResult(true));
                }
            }
        }

        public void Run()
        {
            var manager = new EventStoreManager();
            var stream = new FileStream("0.event", FileMode.Create, FileAccess.ReadWrite, FileShare.None, 1);
            var file = new StreamEventFileStorage(stream);
            _writeStore = manager.AppendToStore(file);
            var consumerTask = Task.Factory.StartNew(EventConsumer,TaskCreationOptions.LongRunning|TaskCreationOptions.HideScheduler);
            _sw.Start();
            var tasks = new Task[1000];
            Parallel.For(0, tasks.Length, i =>
                {
                    tasks[i] = PublishSampleEvents(1000);
                });
            Task.WaitAll(tasks);
            _bc.CompleteAdding();
            _sw.Stop();
            consumerTask.Wait();
            Console.WriteLine(_sw.ElapsedMilliseconds + "ms event per second:" + tasks.Length * 1000 / _sw.Elapsed.TotalSeconds + " len:" + stream.Length);
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