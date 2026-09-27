using System;
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
