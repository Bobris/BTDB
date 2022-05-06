using System;
using System.Threading;

namespace BTDB.KVDBLayer;

public class CompactorScheduler : IDisposable, ICompactorScheduler
{
    Func<CancellationToken, bool>[] _coreActions = new Func<CancellationToken, bool>[0];
    readonly Timer _timer;
    CancellationTokenSource _cancellationSource = new CancellationTokenSource();
    readonly object _lock = new object();
    bool _running;  //compacting in progress
    bool _advicedRunning;  //was advised again during compacting
    bool _firstTime;  //in time period before first compact (try save resource during app startup)
    bool _timerStarted; //timer is planned
    bool _disposed;
    internal TimeSpan WaitTime { get; set; }

    static ICompactorScheduler _instance;

    public static ICompactorScheduler Instance
    {
        get
        {
            if (_instance == null)
            {
                Interlocked.CompareExchange(ref _instance, new CompactorScheduler(), null);
            }
            return _instance;
        }
        set { _instance = value; }
    }

    internal CompactorScheduler()
    {
        _timer = new Timer(OnTimer);
        _firstTime = true;
        WaitTime = TimeSpan.FromMinutes(10 + new Random().NextDouble() * 5);
    }

    public Func<CancellationToken, bool> AddCompactAction(Func<CancellationToken, bool> compactAction)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(CompactorScheduler));
        while (true)
        {
            var oldA = _coreActions;
            var newA = oldA;
            Array.Resize(ref newA, oldA.Length + 1);
            newA[oldA.Length] = compactAction;
            if (Interlocked.CompareExchange(ref _coreActions, newA, oldA) == oldA) break;
        }
        return compactAction;
    }

    public void RemoveCompactAction(Func<CancellationToken, bool> compactAction)
    {
        if (_disposed) return;
        lock (_lock)
        {
            _cancellationSource.Cancel();
            while (_running)
            {
                Monitor.Wait(_lock);
            }
            _cancellationSource = new CancellationTokenSource();
            while (true)
            {
                var oldA = _coreActions;
                var newA = oldA;
                var idx = Array.IndexOf(newA, compactAction);
                if (idx < 0) break;
                Array.Resize(ref newA, oldA.Length - 1);
                if (idx > 0) Array.Copy(oldA, 0, newA, 0, idx);
                if (idx < newA.Length) Array.Copy(oldA, idx + 1, newA, idx, newA.Length - idx);
                if (Interlocked.CompareExchange(ref _coreActions, newA, oldA) == oldA) break;
            }
        }
    }

    public void AdviceRunning(bool openingDb)
    {
        lock (_lock)
        {
            if (_running)
            {
                _advicedRunning = true;
                return;
            }
            if (openingDb)
            {
                if (_timerStarted)
                    return;
                _timer.Change(WaitTime, TimeSpan.FromMilliseconds(-1));
                _timerStarted = true;
            }
            else if (!_firstTime || !_timerStarted) //!_timerStared only when not AdviceRunning(true) was called
            {
                _timer.Change(1, -1);
                _timerStarted = true;
            }
        }
    }

    void OnTimer(object state)
    {
        lock (_lock)
        {
            _firstTime = false;
            _timerStarted = false;
            if (_running) return;
            _running = true;

        }
        try
        {
            var needed = false;
            do
            {
                _advicedRunning = false;
                var actions = _coreActions;
                for (var i = 0; i < actions.Length; i++)
                {
                    if (_cancellationSource.IsCancellationRequested) break;
                    needed |= actions[i](_cancellationSource.Token);
                }
            } while (_advicedRunning);
            lock (_lock)
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
        _disposed = true;
        lock (_lock)
        {
            _cancellationSource.Cancel();
            while (_running)
            {
                Monitor.Wait(_lock);
            }
        }
        _timer.Dispose();
    }
}
