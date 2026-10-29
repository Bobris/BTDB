using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer;

public class CompactorScheduler : IDisposable, ICompactorScheduler
{
    sealed class DbCompactionInfo
    {
        public required IKeyValueDB KeyValueDB { get; init; }
        public bool FirstTime = true;
        public long DueAtTicks;
    }

    static readonly TimeSpan ImmediateRunDelay = TimeSpan.FromMilliseconds(1);
    readonly List<DbCompactionInfo> _dbCompactions = [];
    readonly Timer _timer;
    CancellationTokenSource _cancellationSource = new CancellationTokenSource();
    readonly object _lock = new();
    bool _running; // compacting in progress
    bool _disposed;
    internal TimeSpan WaitTime { get; set; }

    // ReSharper disable once ReplaceWithFieldKeyword nullable mismatch
    static ICompactorScheduler? _instance;

    public static ICompactorScheduler Instance
    {
        get
        {
            if (_instance == null)
            {
                Interlocked.CompareExchange(ref _instance, new CompactorScheduler(), null);
            }

            return _instance!;
        }
        set => _instance = value;
    }

    internal CompactorScheduler()
    {
        _timer = new Timer(OnTimer);
        WaitTime = TimeSpan.FromMinutes(10 + new Random().NextDouble() * 5);
    }

    public void AddCompactAction(IKeyValueDB keyValueDB)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(CompactorScheduler));
        lock (_lock)
        {
            if (FindDbCompactionUnsafe(keyValueDB) != null)
                return;
            _dbCompactions.Add(new DbCompactionInfo { KeyValueDB = keyValueDB });
        }
    }

    public void RemoveCompactAction(IKeyValueDB keyValueDB)
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
            var dbCompaction = FindDbCompactionUnsafe(keyValueDB);
            if (dbCompaction == null) return;
            _dbCompactions.Remove(dbCompaction);
            RescheduleTimerUnsafe();
        }
    }

    public void AdviceRunning(IKeyValueDB keyValueDB, bool openingDb)
    {
        lock (_lock)
        {
            var dbCompaction = FindDbCompactionUnsafe(keyValueDB);
            if (dbCompaction == null) return;
            ScheduleDbCompactionUnsafe(dbCompaction, openingDb);
            if (!_running)
            {
                RescheduleTimerUnsafe();
            }
        }
    }

    // ReSharper disable once AsyncVoidMethod
    async void OnTimer(object? state)
    {
        List<DbCompactionInfo>? dueCompactions;
        lock (_lock)
        {
            if (_running) return;
            dueCompactions = TakeDueCompactionsUnsafe();
            if (dueCompactions == null)
            {
                RescheduleTimerUnsafe();
                return;
            }

            _running = true;
        }

        var next = 0;
        List<DbCompactionInfo>? neededAgain = null;
        try
        {
            while (true)
            {
                neededAgain = null;
                for (next = 0; next < dueCompactions.Count; next++)
                {
                    if (_cancellationSource.IsCancellationRequested) break;
                    var dbCompaction = dueCompactions[next];
                    if (await dbCompaction.KeyValueDB.Compact(_cancellationSource.Token))
                    {
                        neededAgain ??= [];
                        neededAgain.Add(dbCompaction);
                    }
                }

                lock (_lock)
                {
                    if (_cancellationSource.IsCancellationRequested)
                    {
                        StopRunningUnsafe(dueCompactions, next, neededAgain, false);
                        return;
                    }

                    if (neededAgain != null)
                    {
                        foreach (var dbCompaction in neededAgain)
                        {
                            ScheduleDbCompactionAfterWaitUnsafe(dbCompaction);
                        }
                    }

                    dueCompactions = TakeDueCompactionsUnsafe();
                    next = 0;
                    if (dueCompactions == null)
                    {
                        _running = false;
                        Monitor.PulseAll(_lock);
                        RescheduleTimerUnsafe();
                        return;
                    }
                }
            }
        }
        catch (Exception)
        {
            lock (_lock)
            {
                StopRunningUnsafe(dueCompactions, next, neededAgain, !_cancellationSource.IsCancellationRequested);
            }
        }
    }

    // Removing one database cancels the compaction of all, so those not finished stay due; a failed one waits.
    void StopRunningUnsafe(List<DbCompactionInfo> dueCompactions, int next, List<DbCompactionInfo>? neededAgain,
        bool failed)
    {
        if (neededAgain != null)
        {
            foreach (var dbCompaction in neededAgain)
            {
                ScheduleDbCompactionAfterWaitUnsafe(dbCompaction);
            }
        }

        for (var i = next; i < dueCompactions.Count; i++)
        {
            if (failed && i == next)
                ScheduleDbCompactionAfterWaitUnsafe(dueCompactions[i]);
            else
                ScheduleDbCompactionUnsafe(dueCompactions[i], false);
        }

        _running = false;
        Monitor.PulseAll(_lock);
        RescheduleTimerUnsafe();
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

    DbCompactionInfo? FindDbCompactionUnsafe(IKeyValueDB keyValueDB)
    {
        for (var i = 0; i < _dbCompactions.Count; i++)
        {
            if (ReferenceEquals(_dbCompactions[i].KeyValueDB, keyValueDB))
                return _dbCompactions[i];
        }

        return null;
    }

    void ScheduleDbCompactionUnsafe(DbCompactionInfo dbCompaction, bool openingDb)
    {
        if (openingDb)
        {
            if (dbCompaction.DueAtTicks != 0)
                return;
            dbCompaction.DueAtTicks = DateTime.UtcNow.Add(WaitTime).Ticks;
            return;
        }

        if (!dbCompaction.FirstTime || dbCompaction.DueAtTicks == 0)
        {
            dbCompaction.DueAtTicks = DateTime.UtcNow.Add(ImmediateRunDelay).Ticks;
        }
    }

    void ScheduleDbCompactionAfterWaitUnsafe(DbCompactionInfo dbCompaction)
    {
        var dueAtTicks = DateTime.UtcNow.Add(WaitTime).Ticks;
        if (dbCompaction.DueAtTicks == 0 || dbCompaction.DueAtTicks > dueAtTicks)
        {
            dbCompaction.DueAtTicks = dueAtTicks;
        }
    }

    List<DbCompactionInfo>? TakeDueCompactionsUnsafe()
    {
        List<DbCompactionInfo>? dueCompactions = null;
        var nowTicks = DateTime.UtcNow.Ticks;
        for (var i = 0; i < _dbCompactions.Count; i++)
        {
            var dbCompaction = _dbCompactions[i];
            if (dbCompaction.DueAtTicks == 0 || dbCompaction.DueAtTicks > nowTicks)
                continue;
            dbCompaction.FirstTime = false;
            dbCompaction.DueAtTicks = 0;
            dueCompactions ??= [];
            dueCompactions.Add(dbCompaction);
        }

        return dueCompactions;
    }

    void RescheduleTimerUnsafe()
    {
        if (_disposed)
            return;

        long nextDueAtTicks = 0;
        for (var i = 0; i < _dbCompactions.Count; i++)
        {
            var dueAtTicks = _dbCompactions[i].DueAtTicks;
            if (dueAtTicks == 0)
                continue;
            if (nextDueAtTicks == 0 || dueAtTicks < nextDueAtTicks)
            {
                nextDueAtTicks = dueAtTicks;
            }
        }

        if (nextDueAtTicks == 0)
        {
            _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            return;
        }

        var wait = new DateTime(nextDueAtTicks, DateTimeKind.Utc) - DateTime.UtcNow;
        if (wait < ImmediateRunDelay)
        {
            wait = ImmediateRunDelay;
        }

        _timer.Change(wait, Timeout.InfiniteTimeSpan);
    }
}
