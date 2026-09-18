using System;
using System.Collections.Generic;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class SchedulerTest
{
    [Fact]
    public void StableOrderCancellationAndPausedNodesUseOnlyVirtualTime()
    {
        var scheduler = new DeterministicScheduler(123);
        var a = scheduler.CreateScope("a");
        var b = scheduler.CreateScope("b");
        var observed = new List<string>();
        a.Schedule(TimeSpan.FromTicks(2), () => observed.Add("a"), "a");
        a.Schedule(TimeSpan.FromTicks(2), () => observed.Add("cancelled"), "cancel").Dispose();
        b.Schedule(TimeSpan.FromTicks(1), () => observed.Add("b"), "b");
        a.Schedule(TimeSpan.FromTicks(2), () => observed.Add("a2"), "a2");
        b.Paused = true;
        scheduler.AdvanceBy(TimeSpan.FromTicks(3));
        Assert.Equal(new[] { "a", "a2" }, observed);
        b.Paused = false;
        scheduler.RunUntilIdle();
        Assert.Equal(new[] { "a", "a2", "b" }, observed);
        Assert.Equal(TimeSpan.FromTicks(3), scheduler.Elapsed);
    }

    [Fact]
    public void FailureIncludesSeedScheduleAndArtifactSink()
    {
        string? artifact = null;
        var scheduler = new DeterministicScheduler(42, trace => artifact = trace);
        var scope = scheduler.CreateScope("node");
        scope.Schedule(TimeSpan.Zero, () => throw new InvalidOperationException("broken invariant"), "fault");
        var error = Assert.Throws<SimulationFailureException>(() => scheduler.RunNext());
        Assert.Contains("seed=42", error.Message);
        Assert.Contains("scope=node fault", artifact);
        Assert.IsType<InvalidOperationException>(error.InnerException);
    }

    [Fact]
    public void ZeroDelayLoopsAreBoundedAndFailedOracleRunsAfterEachStep()
    {
        var scheduler = new DeterministicScheduler(1, _ => { });
        var scope = scheduler.CreateScope("node");
        var checks = 0;
        scheduler.CheckInvariants = () => checks++;
        void Repeat() => scope.Schedule(TimeSpan.Zero, Repeat, "repeat");
        Repeat();
        Assert.Throws<SimulationFailureException>(() => scheduler.RunUntilIdle(5));
        Assert.Equal(5, checks);

        var other = new DeterministicScheduler(1, _ => { });
        other.CheckInvariants = () => throw new InvalidOperationException("oracle failure");
        other.CreateScope("node").Schedule(TimeSpan.Zero, () => { }, "noop");
        Assert.Contains("oracle failure", Assert.Throws<SimulationFailureException>(() => other.RunNext()).Message);
    }

    [Fact]
    public void SeededSchedulesAreReproducibleAndScopesDoNotShareRandomState()
    {
        static string Run(ulong seed)
        {
            var scheduler = new DeterministicScheduler(seed);
            var scope = scheduler.CreateScope("node");
            var random = new SeededRandom(seed);
            for (var i = 0; i < 20; i++)
                scope.Schedule(random.Backoff(TimeSpan.FromTicks(100)), () => { }, $"operation {i}");
            scheduler.RunUntilIdle();
            return scheduler.Trace;
        }
        Assert.Equal(Run(7), Run(7));
        Assert.NotEqual(Run(7), Run(8));
        Assert.Equal(0xE220A8397B1DCDAFul, new SeededRandom(0).NextUInt64());
    }

    [Fact]
    public void StoppedScopeCancelsQueuedCallbacksAndCannotScheduleAgain()
    {
        var scheduler = new DeterministicScheduler(1);
        var scope = scheduler.CreateScope("node");
        scope.Schedule(TimeSpan.Zero, () => throw new Exception("must not run"), "old callback");
        scope.Dispose();
        Assert.False(scheduler.RunNext());
        Assert.Throws<ObjectDisposedException>(() => scope.Schedule(TimeSpan.Zero, () => { }, "new callback"));
    }
}
