using System;

namespace BTDB.Replication;

/// <summary>Opt-in no-progress budgets. The application selects measured deployment values; no default timeout
/// is inferred from event execution time. Restore and idle databases are not watched. RestartDelay is the fixed
/// wait between immediate fencing on expiry and the fatal host callback; it must be positive. Optional Maintenance
/// watches completed checkpoint/cleanup steps, excluding idle intervals and application leak-event submission.</summary>
public sealed record ReplicationProgressTimeouts(TimeSpan Activation, TimeSpan Publication, TimeSpan RestartDelay, TimeSpan? Maintenance = null)
{
    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Activation.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Publication.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RestartDelay.Ticks);
        if (Maintenance is { } maintenance) ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maintenance.Ticks);
    }
}

/// <summary>A pending-work timer. Completion/progress and an already dispatched timeout arbitrate under one lock.
/// Retrying an unchanged operation must not call Progress. No callback runs under this object's lock.</summary>
internal sealed class ReplicationProgressWatchdog(IReplicationScheduler scheduler, TimeSpan timeout,
    Action expired, string description) : IDisposable
{
    readonly object _lock = new();
    IDisposable? _timer;
    TimeSpan _deadline;
    bool _closed;

    public void Progress()
    {
        lock (_lock)
        {
            if (_closed) return;
            _deadline = scheduler.Elapsed + timeout;
            // Validation may report every 256 KiB; do not allocate/cancel a timer for each chunk.
            _timer ??= scheduler.Schedule(timeout, Expire, description);
        }
    }

    void Expire()
    {
        lock (_lock)
        {
            if (_closed) return;
            _timer?.Dispose();
            var remaining = _deadline - scheduler.Elapsed;
            if (remaining > TimeSpan.Zero)
            {
                _timer = scheduler.Schedule(remaining, Expire, description);
                return;
            }
            _closed = true;
            _timer = null;
        }
        expired();
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _closed = true;
            _timer?.Dispose();
            _timer = null;
        }
    }
}

/// <summary>One maintenance lane. Retries of earlier stages cannot extend the deadline; idle lanes have no timer.</summary>
internal sealed class ReplicationMaintenanceWatchdog(IReplicationScheduler scheduler, TimeSpan timeout, Action expired) : IDisposable
{
    readonly object _lock = new();
    ReplicationProgressWatchdog? _watchdog;
    (int Phase, int Step, int Item) _progress = (-1, -1, -1);
    bool _closed;

    public void Observe((int Phase, int Step, int Item)? progress)
    {
        lock (_lock)
        {
            if (_closed) return;
            if (progress == null)
            {
                _watchdog?.Dispose();
                _watchdog = null;
                _progress = (-1, -1, -1);
                return;
            }
            if (progress.Value.CompareTo(_progress) <= 0) return;
            _progress = progress.Value;
            _watchdog ??= new(scheduler, timeout, expired, "replication maintenance watchdog");
            _watchdog.Progress();
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _closed = true;
            _watchdog?.Dispose();
            _watchdog = null;
        }
    }
}
