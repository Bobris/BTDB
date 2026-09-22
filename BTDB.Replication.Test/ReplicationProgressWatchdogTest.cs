using System;
using System.Collections.Generic;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class ReplicationProgressWatchdogTest
{
    [Fact]
    public void ForwardProgressExtendsDeadlineAndExpiryCannotBeRevived()
    {
        var clock = new DeterministicScheduler(909);
        var expired = 0;
        using var watchdog = new ReplicationProgressWatchdog(clock.CreateScope("node"), TimeSpan.FromTicks(10),
            () => expired++, "progress");
        watchdog.Progress();
        clock.AdvanceBy(TimeSpan.FromTicks(9));
        watchdog.Progress();
        clock.AdvanceBy(TimeSpan.FromTicks(9));
        Assert.Equal(0, expired);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        Assert.Equal(1, expired);
        watchdog.Progress();
        clock.AdvanceBy(TimeSpan.FromTicks(100));
        Assert.Equal(1, expired);
    }

    // Model a timer callback already taken from the scheduler queue when cancellation/progress wins.
    sealed class DispatchedClock : IReplicationScheduler
    {
        public TimeSpan Elapsed => TimeSpan.Zero;
        public readonly List<Action> Callbacks = new();
        public IDisposable Schedule(TimeSpan delay, Action callback, string description)
        { Callbacks.Add(callback); return new Dispatched(); }
        sealed class Dispatched : IDisposable { public void Dispose() { } }
    }

    [Fact]
    public void DispatchedOldTimeoutCannotOverrideProgressOrCompletion()
    {
        var clock = new DispatchedClock();
        var expired = 0;
        var watchdog = new ReplicationProgressWatchdog(clock, TimeSpan.FromTicks(10), () => expired++, "progress");
        watchdog.Progress();
        watchdog.Progress();
        clock.Callbacks[0]();
        Assert.Equal(0, expired);
        watchdog.Dispose();
        clock.Callbacks[1]();
        Assert.Equal(0, expired);
    }
}
