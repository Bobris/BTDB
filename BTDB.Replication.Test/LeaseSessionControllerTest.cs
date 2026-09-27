using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class LeaseSessionControllerTest
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisqualificationRejectsLateAcquireOrRenewalAndAllFutureAttempts(bool renewal)
    {
        var clock = new DeterministicScheduler(505);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var previous = renewal ? await controller.MaintainAsync() : null;
        Func<Task> disqualify = () => { controller.Disqualify(); return Task.CompletedTask; };
        if (renewal) storage.BeforeRenew = disqualify;
        else storage.BeforeAcquire = disqualify;
        Assert.Null(await controller.MaintainAsync());
        Assert.Null(controller.Current);
        if (previous != null) Assert.True(previous.IsFenced);
        var attempts = storage.Acquires + storage.Renews;
        clock.AdvanceBy(TimeSpan.FromTicks(1000));
        Assert.Null(await controller.MaintainAsync());
        Assert.Equal(attempts, storage.Acquires + storage.Renews);
    }

    // Advances time between two reads of one maintenance attempt, like a pause right after a validity check.
    sealed class SteppingClock : IReplicationScheduler
    {
        public long Ticks;
        public Action? AfterRead;
        public TimeSpan Elapsed
        {
            get
            {
                var result = TimeSpan.FromTicks(Ticks);
                var after = AfterRead;
                AfterRead = null;
                after?.Invoke();
                return result;
            }
        }
        public IDisposable Schedule(TimeSpan delay, Action callback, string description) => throw new NotSupportedException();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SessionFencedJustAfterValidityCheckIsReplacedInsteadOfFailingMaintenance(bool disqualify)
    {
        var clock = new SteppingClock();
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock, 0, TimeSpan.Zero);
        var first = (await controller.MaintainAsync())!;
        clock.Ticks = 99;
        clock.AfterRead = () =>
        {
            if (disqualify) controller.Disqualify();
            else clock.Ticks = 100; // The lease deadline passes before the renewal is dispatched.
        };
        var next = await controller.MaintainAsync();
        Assert.True(first.IsFenced);
        Assert.Equal(0, storage.Renews);
        if (disqualify)
        {
            Assert.Null(next);
            Assert.Equal(1, storage.Acquires); // No acquisition that nobody would renew.
        }
        else
        {
            Assert.NotNull(next);
            Assert.NotSame(first, next);
        }
    }

    // Like a real timer: disposal stops pending work, but a callback that already started still runs afterwards.
    sealed class LateCallbackScheduler : IReplicationScheduler
    {
        public readonly List<(string Description, Action Callback)> Started = new();
        public TimeSpan Elapsed => TimeSpan.Zero;
        public IDisposable Schedule(TimeSpan delay, Action callback, string description)
        {
            Started.Add((description, callback));
            return new Registration();
        }
        sealed class Registration : IDisposable { public void Dispose() { } }
    }

    [Fact]
    public async Task FailedRenewalRetriesBeforeTheLeaseExpiresEvenWithALongRetryInterval()
    {
        var clock = new DeterministicScheduler(506);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        using var stop = new CancellationTokenSource();
        var failures = 1;
        storage.BeforeRenew = () => failures-- > 0 ? throw new System.IO.IOException("transient") : Task.CompletedTask;
        LeaseAuthority? first = null;
        // Without xUnit's context, continuations run inline inside the deterministic pump.
        var context = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(null);
        Task run;
        try
        {
            run = controller.RunAsync(TimeSpan.FromTicks(1000), current => first ??= current, stop.Token);
            clock.AdvanceBy(TimeSpan.FromTicks(90));
            Assert.Equal(2, storage.Renews); // The first renewal failed; the retry did not wait 1000 ticks.
            Assert.Same(first, controller.Current);
            Assert.Equal(1, storage.Acquires);
            stop.Cancel();
        }
        finally { SynchronizationContext.SetSynchronizationContext(context); }
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
    }

    [Fact]
    public async Task LateRequestTimeoutAfterCompletedMaintenanceIsHarmless()
    {
        var clock = new LateCallbackScheduler();
        var controller = new LeaseSessionController(new Storage(), clock, 0, TimeSpan.Zero);
        using var stop = new CancellationTokenSource();
        var run = controller.RunAsync(TimeSpan.FromTicks(10), _ => { }, stop.Token, TimeSpan.FromTicks(5));
        var timeout = Assert.Single(clock.Started, w => w.Description == "lease request timeout");
        timeout.Callback(); // The request finished and disposed its cancellation source first.
        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
    }

    sealed class Storage : IReplicationLeaseStorage
    {
        public bool Available = true;
        public int Acquires, Renews;
        public Func<Task>? BeforeAcquire, BeforeRenew;
        public async ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation)
        {
            Acquires++;
            if (BeforeAcquire != null) await BeforeAcquire();
            return Available ? new LeaseGrant(Acquires.ToString(), TimeSpan.FromTicks(100)) : null;
        }
        public async ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
        {
            Assert.Equal(Acquires.ToString(), handle);
            Renews++;
            if (BeforeRenew != null) await BeforeRenew();
            return Available ? TimeSpan.FromTicks(100) : null;
        }
    }

    sealed class TransferStorage : IReplicationLeaseStorage
    {
        public readonly List<string> Renewed = new();
        public string? Transferred = "transfer";
        public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation) =>
            ValueTask.FromResult<LeaseGrant?>(new("acquired", TimeSpan.FromTicks(100)));
        public ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
        {
            Renewed.Add(handle);
            return ValueTask.FromResult<TimeSpan?>(handle == Transferred ? TimeSpan.FromTicks(100) : null);
        }
    }

    [Fact]
    public async Task TransferredHandleIsConsumedAndNotRenewedOnLaterReacquisition()
    {
        var clock = new DeterministicScheduler(506);
        var storage = new TransferStorage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        controller.ProposeTransfer("transfer");
        var transferred = await controller.MaintainAsync();
        Assert.NotNull(transferred);
        Assert.Equal("transfer", controller.GetHandle(transferred));
        storage.Transferred = null;
        clock.AdvanceBy(TimeSpan.FromTicks(100));
        var acquired = await controller.MaintainAsync();
        Assert.NotNull(acquired);
        Assert.Equal("acquired", controller.GetHandle(acquired));
        Assert.Equal(new[] { "transfer" }, storage.Renewed);
    }

    [Fact]
    public async Task ShortOutageRenewsTheSameAuthorityBeforeItsDeadline()
    {
        var clock = new DeterministicScheduler(501);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var first = await controller.MaintainAsync();
        Assert.NotNull(first);
        clock.AdvanceBy(TimeSpan.FromTicks(30));
        storage.Available = false;
        Assert.Same(first, await controller.MaintainAsync());
        Assert.Equal(TimeSpan.FromTicks(100), first.Deadline);
        clock.AdvanceBy(TimeSpan.FromTicks(30));
        storage.Available = true;
        Assert.Same(first, await controller.MaintainAsync());
        Assert.Equal(TimeSpan.FromTicks(160), first.Deadline);
        Assert.Equal(1, storage.Acquires);
    }

    [Fact]
    public async Task OutagePastExpiryAcquiresFreshAuthorityAndCannotReviveOldReferences()
    {
        var clock = new DeterministicScheduler(502);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var first = (await controller.MaintainAsync())!;
        storage.Available = false;
        clock.AdvanceBy(TimeSpan.FromTicks(100));
        Assert.Null(controller.Current);
        Assert.Null(await controller.MaintainAsync());
        storage.Available = true;
        var next = await controller.MaintainAsync();
        Assert.NotNull(next);
        Assert.NotSame(first, next);
        Assert.True(next.IsValid);
        Assert.True(first.IsFenced);
        Assert.Throws<InvalidOperationException>(() => first.BeginRequest());
        Assert.Equal(3, storage.Acquires);
        Assert.Equal(0, storage.Renews);
    }

    [Fact]
    public async Task LateSuccessfulRenewalCannotReviveExpiredSession()
    {
        var clock = new DeterministicScheduler(503);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        var first = (await controller.MaintainAsync())!;
        storage.BeforeRenew = () => { clock.AdvanceBy(TimeSpan.FromTicks(100)); return Task.CompletedTask; };
        Assert.Null(await controller.MaintainAsync());
        Assert.True(first.IsFenced);
        Assert.NotNull(await controller.MaintainAsync());
        Assert.Equal(2, storage.Acquires);
    }

    [Fact]
    public async Task ClosingDuringAcquireRejectsItsLateSuccess()
    {
        var clock = new DeterministicScheduler(504);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        storage.BeforeAcquire = () => { controller.Close(); return Task.CompletedTask; };
        Assert.Null(await controller.MaintainAsync());
        Assert.Null(controller.Current);
        await Assert.ThrowsAsync<InvalidOperationException>(() => controller.MaintainAsync().AsTask());
    }
}
