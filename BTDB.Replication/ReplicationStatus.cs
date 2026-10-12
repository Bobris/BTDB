using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

/// <summary>Sampled completed cuts, not reader-visible roots or client durability acknowledgements.
/// Compared is historical byte equality, not a live confirmation grant. Null means unavailable.
/// Canonical is the end of this node's local TRL known to be canonical (restored, published by this node as leader,
/// or compared with a leader that had published it), on every role: a restart restores at least this history.
/// </summary>
public sealed record ReplicationDatabaseStatus(string Name, LeaderTrlProgress? LocalCommitted,
    LeaderTrlProgress? Compared, TransactionLogPosition? Published, bool Removed, bool Detached,
    TransactionLogPosition? Canonical = null);

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
    readonly object _changeLock = new();
    TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ReplicationNodeStatus Current
    {
        get
        {
            ReplicationNodeStatus current;
            lock (_changeLock) current = _current;
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

    /// <summary>Completes once the database's canonical cut reaches <paramref name="position"/>, a local TRL position
    /// such as <see cref="LeaderTrlProgress.Position"/> of a completed application cut. From then on a restart of this
    /// node restores at least that history. Fails when the database is removed or detached, or the node stops or
    /// requires a restart, because its canonical cut can no longer advance. Coordinator steps report the cut, so
    /// completion lags publication by up to one poll interval.</summary>
    public async Task WaitForCanonicalAsync(string database, TransactionLogPosition position,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        while (true)
        {
            Task changed;
            ReplicationNodeStatus current;
            lock (_changeLock)
            {
                changed = _changed.Task;
                current = _current;
            }
            ReplicationDatabaseStatus? status = null;
            foreach (var candidate in current.Databases)
                if (candidate.Name == database) status = candidate;
            if (status?.Canonical is { } canonical && canonical >= position) return;
            if (status is { Removed: true } or { Detached: true })
                throw new InvalidOperationException($"Database '{database}' is no longer replicated.");
            if (current.Role is ReplicationNodeRole.Stopped or ReplicationNodeRole.RestartRequired)
                throw new InvalidOperationException("The replication node stopped before the history became canonical.");
            await changed.WaitAsync(cancellation).ConfigureAwait(false);
        }
    }

    internal void Update(ReplicationNodeStatus status)
    {
        TaskCompletionSource changed;
        lock (_changeLock)
        {
            _current = status;
            changed = _changed;
            _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        changed.TrySetResult();
    }
    internal void RestoreStarted() => Interlocked.Increment(ref _restoreAttempts);
    internal void RestoreFailed() => Interlocked.Increment(ref _restoreFailures);
    internal void Restored(TimeSpan duration) => Interlocked.Exchange(ref _lastRestoreTicks, duration.Ticks);
    internal void LeaderSessionStarted() => Interlocked.Increment(ref _leaderSessions);
    internal void LeaderSessionEnded() => Interlocked.Increment(ref _lostLeaderSessions);
    internal void StepFailed() => Interlocked.Increment(ref _failedSteps);
    // Sticky: late coordinator callbacks cannot make a stopping host ready again.
    internal void Stop() => Interlocked.Exchange(ref _stopping, 1);
}
