using System;
using System.Collections.Generic;
using System.Threading;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>Sampled completed cuts, not reader-visible roots or client durability acknowledgements.
/// Compared is historical byte equality, not a live confirmation grant. Null means unavailable.</summary>
public sealed record ReplicationDatabaseStatus(string Name, LeaderTrlProgress? LocalCommitted,
    LeaderTrlProgress? Compared, TransactionLogPosition? Published, bool Removed, bool Detached);

/// <summary>Credential-free local status. SampledAt uses the injected node-local monotonic clock.
/// Ready means restored and available for ordinary local work, not caught up or authorized to publish.</summary>
public sealed record ReplicationNodeStatus(ReplicationNodeRole Role, bool Ready, TimeSpan SampledAt,
    IReadOnlyList<ReplicationDatabaseStatus> Databases);

/// <summary>Thread-safe sampled diagnostics. No storage or peer I/O occurs on reads.</summary>
public sealed class ReplicationStatus
{
    ReplicationNodeStatus _current = new(ReplicationNodeRole.Restoring, false, TimeSpan.Zero,
        Array.Empty<ReplicationDatabaseStatus>());
    int _stopping;

    public ReplicationNodeStatus Current
    {
        get
        {
            var current = Volatile.Read(ref _current);
            return current.Ready && Volatile.Read(ref _stopping) != 0 ? current with { Ready = false } : current;
        }
    }

    long _restoreAttempts, _restoreFailures, _lastRestoreTicks = -1, _leaderSessions, _lostLeaderSessions, _failedSteps;

    /// <summary>Calls of the host's restore since this node started, including retries.</summary>
    public long RestoreAttempts => Interlocked.Read(ref _restoreAttempts);
    /// <summary>Restore attempts that failed with retryable I/O and were retried (for example a deleted file).</summary>
    public long RestoreFailures => Interlocked.Read(ref _restoreFailures);
    /// <summary>Duration of the successful restore, including failed attempts before it; null until it completes.</summary>
    public TimeSpan? RestoreDuration => Interlocked.Read(ref _lastRestoreTicks) is var ticks and >= 0 ? TimeSpan.FromTicks(ticks) : null;
    /// <summary>Leader sessions this node activated (takeovers and handoffs received).</summary>
    public long LeaderSessions => Interlocked.Read(ref _leaderSessions);
    /// <summary>Activated leader sessions that ended while the node kept running (lease loss, fencing, handoff).</summary>
    public long LostLeaderSessions => Interlocked.Read(ref _lostLeaderSessions);
    /// <summary>Follower or leader steps that failed with I/O or a request timeout, such as an unreachable leader.</summary>
    public long FailedSteps => Interlocked.Read(ref _failedSteps);

    internal void Update(ReplicationNodeStatus status) => Volatile.Write(ref _current, status);
    internal void RestoreStarted() => Interlocked.Increment(ref _restoreAttempts);
    internal void RestoreFailed() => Interlocked.Increment(ref _restoreFailures);
    internal void Restored(TimeSpan duration) => Interlocked.Exchange(ref _lastRestoreTicks, duration.Ticks);
    internal void LeaderSessionStarted() => Interlocked.Increment(ref _leaderSessions);
    internal void LeaderSessionEnded() => Interlocked.Increment(ref _lostLeaderSessions);
    internal void StepFailed() => Interlocked.Increment(ref _failedSteps);
    // Sticky: late coordinator callbacks cannot make a stopping host ready again.
    internal void Stop() => Interlocked.Exchange(ref _stopping, 1);
}
