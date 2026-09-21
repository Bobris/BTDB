using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class LeaseSessionControllerTest
{
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
