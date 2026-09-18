using System;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class AuthorityTest
{
    static TimeSpan T(long ticks) => TimeSpan.FromTicks(ticks);

    [Fact]
    public void DelayedAcquireDoesNotStartANewLeasePeriodAtResponseTime()
    {
        var scheduler = new DeterministicScheduler(100);
        var authority = new LeaseAuthority(scheduler.CreateScope("node"), 0, T(1));
        var request = authority.BeginRequest();
        scheduler.AdvanceBy(T(80));
        Assert.True(authority.AcceptSuccess(request, T(100)));
        Assert.Equal(T(99), authority.Deadline);
        scheduler.AdvanceBy(T(19));
        Assert.False(authority.IsValid);
        Assert.True(authority.IsFenced);
        Assert.False(authority.AcceptSuccess(request, T(100)));
    }

    [Fact]
    public void AmbiguousRenewalAndProcessPauseCannotExtendAuthority()
    {
        var scheduler = new DeterministicScheduler(101);
        var node = scheduler.CreateScope("node");
        var authority = new LeaseAuthority(node, 0, T(1));
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), T(100)));
        scheduler.AdvanceBy(T(50));
        var renewal = authority.BeginRequest();
        node.Paused = true;
        scheduler.AdvanceBy(T(60));
        node.Paused = false;
        Assert.False(authority.AcceptSuccess(renewal, T(100)));
        Assert.Throws<InvalidOperationException>(() => authority.BeginRequest());
    }

    [Fact]
    public void OlderResponseCannotOverrideTheCurrentRequest()
    {
        var scheduler = new DeterministicScheduler(102);
        var authority = new LeaseAuthority(scheduler.CreateScope("node"), 0, T(1));
        var first = authority.BeginRequest();
        scheduler.AdvanceBy(T(10));
        var second = authority.BeginRequest();
        Assert.False(authority.AcceptSuccess(first, T(100)));
        Assert.False(authority.IsValid);
        Assert.True(authority.AcceptSuccess(second, T(100)));
    }

    [Fact]
    public void DelayedChallengeRepliesAndClosedConnectionsCannotConfirm()
    {
        var scheduler = new DeterministicScheduler(103);
        var window = new ConfirmationWindow(scheduler.CreateScope("follower"));
        var old = window.BeginChallenge(T(10));
        scheduler.AdvanceBy(T(10));
        Assert.False(window.Accept(old));
        var current = window.BeginChallenge(T(10));
        Assert.False(window.Accept(old));
        Assert.True(window.Accept(current));
        window.Close();
        Assert.False(window.IsValid);
        Assert.False(window.Accept(current));
    }

    [Fact]
    public void OneDrainDeadlineWaitsOutEveryGrantAndStopsNewGrants()
    {
        var scheduler = new DeterministicScheduler(104);
        var node = scheduler.CreateScope("leader");
        var authority = new LeaseAuthority(node, 100_000, T(1));
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), T(100)));
        var grants = new ConfirmationGrants(node, authority);
        Assert.True(grants.TryIssue(T(10))); // ceil(10 * 1.1 / 0.9) = 13
        scheduler.AdvanceBy(T(5));
        Assert.True(grants.TryIssue(T(20))); // expires no later than leader tick 30
        grants.BeginDrain();
        Assert.False(grants.TryIssue(T(1)));
        scheduler.AdvanceBy(T(24));
        Assert.False(grants.IsDrained);
        scheduler.AdvanceBy(T(1));
        Assert.True(grants.IsDrained);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(100)]
    [InlineData(100_000)]
    public void WorstClockRatesKeepPeerExpiryInsideServiceLease(int drift)
    {
        var scheduler = new DeterministicScheduler(105);
        var authority = new LeaseAuthority(scheduler.CreateScope("leader"), drift, T(1));
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), T(10000)));
        var slowRate = (1_000_000m - drift) / 1_000_000;
        var fastRate = (1_000_000m + drift) / 1_000_000;
        // Independent real-time inequalities at both rate extremes, including integer rounding.
        Assert.True(authority.Deadline.Ticks / slowRate < 10000);
        Assert.True(authority.BoundPeerWindow(T(100)).Ticks / fastRate >= 100 / slowRate);
        scheduler.AdvanceBy(authority.Deadline - T(100));
        Assert.False(authority.CanGrant(T(100)));
    }
}
