using System;
using System.Diagnostics;
using System.Threading;

namespace BTDB.Replication.ProcessTests;

// Real elapsed time for short-lived, non-suspended subprocess tests only. This is not a qualified production
// clock: Stopwatch's OS-suspend behavior is platform-dependent. Callbacks are serialized; disposing pending work never waits for an active callback.
internal sealed class TestScheduler : IReplicationScheduler
{
    readonly Stopwatch _clock = Stopwatch.StartNew();
    readonly object _lock = new();
    readonly object _callbacks = new();
    public TimeSpan Elapsed => _clock.Elapsed;
    public IDisposable Schedule(TimeSpan delay, Action callback, string description)
    {
        lock (_lock) return new Work(_lock, _callbacks, delay, callback);
    }

    sealed class Work : IDisposable
    {
        readonly object _lock;
        readonly Timer _timer;
        readonly object _callbacks;
        readonly Action _callback;
        bool _closed;
        public Work(object gate, object callbacks, TimeSpan delay, Action callback)
        {
            _lock = gate;
            _callbacks = callbacks;
            _callback = callback;
            _timer = new Timer(_ => Run(), null, System.Threading.Timeout.InfiniteTimeSpan, System.Threading.Timeout.InfiniteTimeSpan);
            _timer.Change(delay, System.Threading.Timeout.InfiniteTimeSpan);
        }
        void Run()
        {
            lock (_callbacks)
            {
                lock (_lock)
                {
                    if (_closed) return;
                    _closed = true;
                    _timer.Dispose();
                }
                _callback();
            }
        }
        public void Dispose()
        {
            lock (_lock) { _closed = true; _timer.Dispose(); }
        }
    }
}
