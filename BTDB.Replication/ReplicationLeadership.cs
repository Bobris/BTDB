using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>Runs application work once per activated local leader session. HTTP hosting registers this node-scoped
/// service automatically. Cancellation is cooperative; external effects still need their own fencing/idempotency.</summary>
public sealed class ReplicationLeadership(IReplicationScheduler scheduler)
{
    readonly IReplicationScheduler _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
    readonly object _lock = new();
    TaskCompletionSource _changed = Signal();
    Session? _current;
    bool _stopped;

    static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Waits as a follower, runs the callback once per active session, and cancels its token on fencing,
    /// lease expiry, handoff or shutdown. Normal callback completion waits for the next session. Callbacks for this
    /// subscription never overlap; callback failure faults the returned task and ends this subscription only.
    /// Caller cancellation unregisters and awaits callback cleanup; node shutdown completes after cleanup.</summary>
    public Task RunWhileLeaderAsync(Func<CancellationToken, Task> action, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        // Never inherit a borrowed Ambient transaction or execute user code on the coordinator's calling stack.
        if (ExecutionContext.IsFlowSuppressed()) return Task.Run(() => RunAsync(action, cancellation));
        using (ExecutionContext.SuppressFlow()) return Task.Run(() => RunAsync(action, cancellation));
    }

    async Task RunAsync(Func<CancellationToken, Task> action, CancellationToken cancellation)
    {
        Session? previous = null;
        while (true)
        {
            cancellation.ThrowIfCancellationRequested();
            Session? session;
            Task changed;
            lock (_lock)
            {
                if (_stopped) return;
                session = _current;
                changed = _changed.Task;
            }
            if (session != null && !ReferenceEquals(session, previous))
            {
                previous = session;
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, session.Token);
                if (!session.IsActive || linked.IsCancellationRequested) continue;
                try
                {
                    await (action(linked.Token) ?? throw new InvalidOperationException("The leader callback returned no task."))
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
                // A completed callback never starts again under the same session, including transient role reports.
                continue;
            }
            await changed.WaitAsync(cancellation).ConfigureAwait(false);
        }
    }

    /// <summary>The token of the active session, cancelled exactly when a <see cref="RunWhileLeaderAsync"/> callback's
    /// is; null on followers, while activating, draining for handoff, after fencing or shutdown.</summary>
    internal CancellationToken? ActiveToken
    {
        get
        {
            lock (_lock) return _current is { IsActive: true } session ? session.Token : null;
        }
    }

    internal void Activate(LeaseAuthority authority, CancellationToken shutdown)
    {
        lock (_lock)
        {
            if (_stopped || shutdown.IsCancellationRequested || !authority.IsValid) return;
            if (_current != null) throw new InvalidOperationException("A leader session is already active.");
            _current = new(authority, _scheduler, shutdown);
            Changed();
        }
    }

    internal void EndSession()
    {
        lock (_lock)
            if (_current != null) EndSessionLocked();
    }

    internal void Stop()
    {
        lock (_lock)
        {
            if (_stopped) return;
            _stopped = true;
            EndSessionLocked();
        }
    }

    void EndSessionLocked()
    {
        _current?.Cancel();
        _current = null;
        Changed();
    }

    void Changed()
    {
        var signal = _changed;
        _changed = Signal();
        signal.TrySetResult();
    }

    sealed class Session
    {
        readonly LeaseAuthority _authority;
        readonly IReplicationScheduler _scheduler;
        readonly object _lock = new();
        readonly CancellationTokenSource _cancel = new();
        readonly CancellationTokenRegistration _shutdown;
        IDisposable? _deadline;

        internal Session(LeaseAuthority authority, IReplicationScheduler scheduler, CancellationToken shutdown)
        {
            _authority = authority;
            _scheduler = scheduler;
            Token = _cancel.Token;
            _shutdown = shutdown.Register(Cancel);
            CheckDeadline();
            _ = ObserveRevocationAsync();
        }

        internal CancellationToken Token { get; }
        internal bool IsActive => !Token.IsCancellationRequested && _authority.IsValid;

        void CheckDeadline()
        {
            lock (_lock)
            {
                _deadline?.Dispose();
                _deadline = null;
                if (Token.IsCancellationRequested) return;
                if (_authority.IsValid)
                {
                    // Renewal may extend the deadline while a provider ignores cancellation. Check the proven
                    // deadline independently of that provider and of publication; a held-renewal test covers expiry.
                    var delay = _authority.Deadline - _scheduler.Elapsed;
                    _deadline = _scheduler.Schedule(delay > TimeSpan.Zero ? delay : TimeSpan.Zero,
                        CheckDeadline, "application leadership deadline");
                    return;
                }
            }
            Cancel();
        }

        internal void Cancel()
        {
            lock (_lock)
            {
                _deadline?.Dispose();
                _deadline = null;
                if (Token.IsCancellationRequested) return;
                // CancelAsync marks the token immediately, but runs arbitrary application callbacks off this lane.
                _ = ObserveCancellationAsync(_cancel.CancelAsync());
            }
        }

        static async Task ObserveCancellationAsync(Task cancellation)
        {
            try { await cancellation.ConfigureAwait(false); }
            catch (Exception) { } // Throwing token callbacks must not interfere with lease fencing or other jobs.
        }

        async Task ObserveRevocationAsync()
        {
            try { await _authority.Revoked.WaitAsync(Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (Token.IsCancellationRequested) { }
            finally { Cancel(); _shutdown.Dispose(); }
        }
    }
}
