using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace BTDB.Replication.Test;

public class ReplicationLeadershipTest
{
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    sealed class Clock : IReplicationScheduler
    {
        readonly object _lock = new();
        readonly List<Work> _work = new();
        TimeSpan _elapsed;
        public TimeSpan Elapsed { get { lock (_lock) return _elapsed; } }
        public IDisposable Schedule(TimeSpan delay, Action callback, string description)
        {
            lock (_lock)
            {
                var work = new Work(this, _elapsed + delay, callback);
                _work.Add(work);
                return work;
            }
        }
        public void Advance(long ticks)
        {
            var end = Elapsed + TimeSpan.FromTicks(ticks);
            while (true)
            {
                Work? work;
                lock (_lock)
                {
                    work = _work.Where(w => w.Due <= end).MinBy(w => w.Due);
                    if (work == null) { _elapsed = end; return; }
                    _elapsed = work.Due;
                    _work.Remove(work);
                }
                work.Callback();
            }
        }
        sealed class Work(Clock clock, TimeSpan due, Action callback) : IDisposable
        {
            internal TimeSpan Due => due;
            internal Action Callback => callback;
            public void Dispose() { lock (clock._lock) clock._work.Remove(this); }
        }
    }

    static LeaseAuthority Authority(Clock clock)
    {
        var authority = new LeaseAuthority(clock, 0, TimeSpan.Zero);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromTicks(100)));
        return authority;
    }

    [Fact]
    public async Task NormalCompletionRunsOncePerSessionAndLateSubscriptionStartsImmediately()
    {
        var clock = new Clock();
        var leadership = new ReplicationLeadership(clock);
        var first = Signal();
        var second = Signal();
        var count = 0;
        var work = leadership.RunWhileLeaderAsync(_ =>
        {
            if (Interlocked.Increment(ref count) == 1) first.SetResult();
            else second.SetResult();
            return Task.CompletedTask;
        });
        Assert.False(first.Task.IsCompleted); // Waiting as a follower.
        leadership.Activate(Authority(clock), CancellationToken.None);
        await first.Task.WaitAsync(Timeout);
        clock.Advance(50);
        Assert.Equal(1, Volatile.Read(ref count));
        Assert.False(work.IsCompleted);
        leadership.EndSession();
        leadership.Activate(Authority(clock), CancellationToken.None);
        await second.Task.WaitAsync(Timeout);
        var lateStarted = Signal();
        var late = leadership.RunWhileLeaderAsync(_ => { lateStarted.SetResult(); return Task.CompletedTask; });
        await lateStarted.Task.WaitAsync(Timeout);
        leadership.Stop();
        await Task.WhenAll(work, late).WaitAsync(Timeout);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task CallbackFailureEndsOnlyItsSubscriptionAndSuppressesBorrowedExecutionContext()
    {
        var local = new AsyncLocal<string?> { Value = "borrowed-context" };
        var clock = new Clock();
        var leadership = new ReplicationLeadership(clock);
        var authority = Authority(clock);
        leadership.Activate(authority, CancellationToken.None);
        var failed = leadership.RunWhileLeaderAsync(_ =>
        {
            Assert.Null(local.Value);
            throw new InvalidOperationException("Application failed");
        });
        await Assert.ThrowsAsync<InvalidOperationException>(() => failed);
        Assert.True(authority.IsValid);
        var entered = Signal();
        var stopped = Signal();
        var healthy = leadership.RunWhileLeaderAsync(async cancellation =>
        {
            entered.SetResult();
            try { await Task.Delay(System.Threading.Timeout.Infinite, cancellation); }
            finally { stopped.SetResult(); }
        });
        await entered.Task.WaitAsync(Timeout);
        authority.Fence();
        await stopped.Task.WaitAsync(Timeout);
        leadership.Stop();
        await healthy.WaitAsync(Timeout);
    }

    sealed class HeldRenewal : IReplicationLeaderStorage
    {
        internal readonly TaskCompletionSource Release = Signal();
        public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation) =>
            ValueTask.FromResult<LeaseGrant?>(new("lease", TimeSpan.FromTicks(100)));
        public async ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
        {
            await Release.Task; // Provider ignores cancellation and does not complete before expiry.
            return TimeSpan.FromTicks(100);
        }
        public ValueTask TransferAsync(string currentHandle, string proposedHandle, CancellationToken cancellation) => throw new NotSupportedException();
        public ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation) => throw new NotSupportedException();
        public ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string expectedToken, string json, CancellationToken cancellation) => throw new NotSupportedException();
    }

    [Fact]
    public async Task DeadlineCancelsWorkWhileRenewalIsStillBlockedAndCannotReviveTheSession()
    {
        var clock = new Clock();
        var storage = new HeldRenewal();
        var leases = new LeaseSessionController(storage, clock, 0, TimeSpan.Zero);
        var authority = (await leases.MaintainAsync())!;
        var leadership = new ReplicationLeadership(clock);
        leadership.Activate(authority, CancellationToken.None);
        var entered = Signal();
        var canceled = Signal();
        var work = leadership.RunWhileLeaderAsync(async cancellation =>
        {
            entered.SetResult();
            try { await Task.Delay(System.Threading.Timeout.Infinite, cancellation); }
            finally { canceled.SetResult(); }
        });
        await entered.Task.WaitAsync(Timeout);
        var renewal = leases.MaintainAsync().AsTask();
        clock.Advance(100);
        await canceled.Task.WaitAsync(Timeout);
        Assert.False(renewal.IsCompleted);
        Assert.False(authority.IsValid);
        storage.Release.SetResult();
        Assert.Null(await renewal.WaitAsync(Timeout));
        leadership.Stop();
        await work.WaitAsync(Timeout);
        leases.Close();
    }

    [Fact]
    public async Task ConfirmedRenewalExtendsTheApplicationDeadline()
    {
        var clock = new Clock();
        var authority = Authority(clock);
        var leadership = new ReplicationLeadership(clock);
        leadership.Activate(authority, CancellationToken.None);
        var entered = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ended = Signal();
        var work = leadership.RunWhileLeaderAsync(async cancellation =>
        {
            entered.SetResult(cancellation);
            try { await Task.Delay(System.Threading.Timeout.Infinite, cancellation); }
            finally { ended.SetResult(); }
        });
        var token = await entered.Task.WaitAsync(Timeout);
        clock.Advance(80);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromTicks(100)));
        clock.Advance(20);
        Assert.False(token.IsCancellationRequested);
        clock.Advance(80);
        await ended.Task.WaitAsync(Timeout);
        Assert.False(authority.IsValid);
        leadership.Stop();
        await work.WaitAsync(Timeout);
    }

    [Fact]
    public async Task ReplacementSessionWaitsForPriorCallbackCleanupAndCallerCancellationUnsubscribes()
    {
        var clock = new Clock();
        var leadership = new ReplicationLeadership(clock);
        leadership.Activate(Authority(clock), CancellationToken.None);
        using var subscription = new CancellationTokenSource();
        var first = Signal();
        var second = Signal();
        var cleaning = Signal();
        var cleanup = Signal();
        var calls = 0;
        var work = leadership.RunWhileLeaderAsync(async cancellation =>
        {
            var call = Interlocked.Increment(ref calls);
            (call == 1 ? first : second).SetResult();
            try { await Task.Delay(System.Threading.Timeout.Infinite, cancellation); }
            finally
            {
                if (call == 1) { cleaning.SetResult(); await cleanup.Task; }
            }
        }, subscription.Token);
        await first.Task.WaitAsync(Timeout);
        leadership.EndSession();
        await cleaning.Task.WaitAsync(Timeout);
        leadership.Activate(Authority(clock), CancellationToken.None);
        Assert.Equal(1, Volatile.Read(ref calls));
        cleanup.SetResult();
        await second.Task.WaitAsync(Timeout);
        subscription.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => work.WaitAsync(Timeout));
        leadership.Stop();
    }

    sealed class Host : IReplicationNodeHost
    {
        public ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation) => throw new NotSupportedException();
        public LeaderCandidate CreateCandidate() => throw new NotSupportedException();
        public LeaderTrlProgress? GetProgress(string database) => null;
        public void RequestRestart(string reason) => throw new NotSupportedException();
        public void RequestFatalRestart(string reason) => throw new NotSupportedException();
        public void ReportStatus(ReplicationNodeRole role) { }
        public void DatabaseRemoved(string database) { }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HandoffCancelsAtTheStartOfDrainingAndPreventsLateApplicationActivation(bool offerBeforeActivation)
    {
        var clock = new Clock();
        var authority = Authority(clock);
        var leadership = new ReplicationLeadership(clock);
        var identity = new ReplicationPeerIdentity("cluster", 1, "session", "node", "key");
        var serving = new ServingLeader(identity, authority, [], [], new Host(), clock, 1, leadership);
        using var peer = serving.Connect(identity);
        await peer.PollAsync([], 1, TimeSpan.FromTicks(10), 0, CancellationToken.None);
        var entered = Signal();
        var ended = Signal();
        var work = leadership.RunWhileLeaderAsync(async cancellation =>
        {
            entered.SetResult();
            try { await Task.Delay(System.Threading.Timeout.Infinite, cancellation); }
            finally { ended.SetResult(); }
        });
        if (!offerBeforeActivation)
        {
            serving.StartApplicationLeadership(CancellationToken.None);
            await entered.Task.WaitAsync(Timeout);
        }
        await peer.OfferHandoffAsync(new(2, "10000000-0000-0000-0000-000000000002"), CancellationToken.None);
        if (offerBeforeActivation) serving.StartApplicationLeadership(CancellationToken.None);
        else await ended.Task.WaitAsync(Timeout);
        Assert.True(authority.IsValid);
        Assert.False(serving.IsDrained);
        leadership.Stop();
        await work.WaitAsync(Timeout);
        Assert.Equal(!offerBeforeActivation, entered.Task.IsCompleted);
        serving.Close();
        authority.Fence();
    }
}
