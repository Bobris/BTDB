using System;
using System.Threading;

namespace BTDB.KVDBLayer
{
    class CompactorScheduler: IDisposable
    {
        readonly Func<CancellationToken, bool> _coreAction;
        readonly Timer _timer;
        readonly CancellationTokenSource _cancellationSource = new CancellationTokenSource();
        readonly object _lock = new object();
        bool _running;
        bool _advicedRunning;
        bool _firstTime;
        internal TimeSpan WaitTime { get; set; }

        internal CompactorScheduler(Func<CancellationToken,bool> coreAction)
        {
            _coreAction = coreAction;
            _timer = new Timer(OnTimer);
            _firstTime = true;
            WaitTime = TimeSpan.FromMinutes(5);
        }

        internal void AdviceRunning()
        {
            lock(_lock)
            {
                if (_running)
                {
                    _advicedRunning = true;
                    return;
                }
                if (_firstTime)
                {
                    _firstTime = false;
                    _timer.Change(WaitTime, TimeSpan.FromMilliseconds(-1));
                }
                else
                {
                    _timer.Change(1, -1);
                }
            }
        }

        void OnTimer(object state)
        {
            lock (_lock)
            {
                if (_running) return;
                _running = true;
            }
            try
            {
                var needed = false;
                do
                {
                    if (_cancellationSource.IsCancellationRequested) break;
                    _advicedRunning = false;
                    needed = _coreAction(_cancellationSource.Token);
                } while (_advicedRunning);
                lock(_lock)
                {
                    _running = false;
                    Monitor.PulseAll(_lock);
                    needed |= _advicedRunning;
                    if (needed && !_cancellationSource.IsCancellationRequested)
                    {
                        _timer.Change(WaitTime, TimeSpan.FromMilliseconds(-1));
                    }
                }
            }
            catch (Exception)
            {
                lock (_lock)
                {
                    _running = false;
                    Monitor.PulseAll(_lock);
                }
            }
        }

        public void Dispose()
        {
            _cancellationSource.Cancel();
            lock(_lock)
            {
                while (_running)
                {
                    Monitor.Wait(_lock);
                }
            }
            _timer.Dispose();
        }
    }
}