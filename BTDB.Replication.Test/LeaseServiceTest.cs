using System;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class LeaseServiceTest
{
    static readonly WriteFaults Immediate = new(TimeSpan.Zero, TimeSpan.Zero);

    [Fact]
    public void LeaderLeaseDoesNotFenceAnotherBlobAndChangingLeaseDoesNotChangeEtag()
    {
        var scheduler = new DeterministicScheduler(300);
        var caller = scheduler.CreateScope("node");
        var storage = new SimulatedBlobStore(scheduler);
        var leases = new SimulatedLeases(scheduler, storage);
        storage.Dispatch(caller, "a", "leader", MutationKind.Create, null, [1], Immediate, _ => { });
        scheduler.RunUntilIdle();
        var version = storage.Read("leader")!.Version;
        leases.Dispatch(caller, "leader", LeaseOperation.Acquire, "a", TimeSpan.FromTicks(100), Immediate,
            result => Assert.Equal(WriteOutcome.Applied, result));
        scheduler.RunUntilIdle();
        leases.Dispatch(caller, "leader", LeaseOperation.Acquire, "b", TimeSpan.FromTicks(100), Immediate,
            result => Assert.Equal(WriteOutcome.Rejected, result));
        storage.Dispatch(caller, "b", "leader", MutationKind.Replace, version, [2], Immediate,
            result => Assert.Equal(WriteOutcome.Rejected, result.Outcome));
        storage.Dispatch(caller, "b", "tail", MutationKind.Create, null, [3], Immediate,
            result => Assert.Equal(WriteOutcome.Applied, result.Outcome));
        leases.Dispatch(caller, "leader", LeaseOperation.Change, "a", TimeSpan.Zero, Immediate,
            result => Assert.Equal(WriteOutcome.Applied, result), "b");
        scheduler.RunUntilIdle();
        Assert.Equal(version, storage.Read("leader")!.Version);
        Assert.True(leases.IsOwned("leader", "b"));
        scheduler.AdvanceBy(TimeSpan.FromTicks(100));
        Assert.False(leases.IsOwned("leader", "b"));
    }

    [Fact]
    public void RemoteRenewAfterExpiryCannotReviveLocallyFencedSession()
    {
        var scheduler = new DeterministicScheduler(301);
        var caller = scheduler.CreateScope("node");
        var storage = new SimulatedBlobStore(scheduler);
        var leases = new SimulatedLeases(scheduler, storage);
        storage.Dispatch(caller, "a", "leader", MutationKind.Create, null, [], Immediate, _ => { });
        scheduler.RunUntilIdle();
        var authority = new LeaseAuthority(caller, 0, TimeSpan.FromTicks(1));
        var acquire = authority.BeginRequest();
        leases.Dispatch(caller, "leader", LeaseOperation.Acquire, "a", TimeSpan.FromTicks(100), Immediate,
            result => Assert.True(authority.AcceptSuccess(acquire, TimeSpan.FromTicks(100))));
        scheduler.RunUntilIdle();
        scheduler.AdvanceBy(TimeSpan.FromTicks(90));
        var renew = authority.BeginRequest();
        leases.Dispatch(caller, "leader", LeaseOperation.Renew, "a", TimeSpan.Zero,
            new(TimeSpan.FromTicks(20), TimeSpan.Zero), result =>
            {
                Assert.Equal(WriteOutcome.Applied, result);
                Assert.False(authority.AcceptSuccess(renew, TimeSpan.FromTicks(100)));
            });
        scheduler.RunUntilIdle();
        Assert.True(leases.IsOwned("leader", "a"));
        Assert.True(authority.IsFenced);
    }
}
