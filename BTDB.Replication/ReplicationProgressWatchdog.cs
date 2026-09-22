using System;

namespace BTDB.Replication;

/// <summary>Opt-in no-progress budgets. The application selects measured deployment values; no default timeout
/// is inferred from event execution time. Restore and idle databases are not watched.</summary>
public sealed record ReplicationProgressTimeouts(TimeSpan Activation, TimeSpan Publication)
{
    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Activation.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Publication.Ticks);
    }
}

/// <summary>Required on the application host when progress deadlines are enabled. Called independently of the
/// stuck worker after lease fencing. Initiate bounded non-graceful process termination/restart without waiting
/// for handlers, storage calls or coordinator cleanup. Never reuse this node session. This callback must not block.</summary>
public interface IReplicationFatalRecovery
{
    void RequestFatalRestart(string reason);
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
