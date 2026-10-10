using System;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.Test.EventLog;

public class CommitLingerEstimatorTest
{
    static readonly TimeSpan Max = TimeSpan.FromMilliseconds(3);
    static readonly TimeSpan Write = TimeSpan.FromMilliseconds(9);

    static TimeSpan Ms(double value) => TimeSpan.FromTicks((long)(value * TimeSpan.TicksPerMillisecond));

    /// <summary>Runs commit cycles of one write; each cycle gets arrivals at the given offsets after its commit.
    /// <paramref name="queued"/> says records already wait at each commit, as for a busy owner.</summary>
    static TimeSpan Cycles(CommitLingerEstimator estimator, int count, Func<int, double[]> offsets, TimeSpan now,
        bool queued = false)
    {
        for (var cycle = 0; cycle < count; cycle++)
        {
            var linger = estimator.Current;
            var commit = now;
            var times = offsets(cycle);
            foreach (var offset in times) estimator.Arrival(commit + Ms(offset));
            var writeStarted = commit + linger;
            now = writeStarted + Write;
            estimator.Committed(writeStarted, now, queued);
        }
        return now;
    }

    static double[] Uniform(TimeSpan cycle, int count)
    {
        var result = new double[count];
        for (var i = 0; i < count; i++) result[i] = (i + 0.5) * cycle.TotalMilliseconds / count;
        return result;
    }

    [Fact]
    public void IndependentArrivalsKeepTheWindowClosed()
    {
        var estimator = new CommitLingerEstimator(Max);
        Cycles(estimator, 500, _ => Uniform(Write, 40), TimeSpan.Zero, queued: true);
        Assert.Equal(TimeSpan.Zero, estimator.Current);
    }

    [Fact]
    public void SparseIndependentArrivalsDoNotOpenAWindowByChance()
    {
        var random = new Random(42);
        for (var run = 0; run < 20; run++)
        {
            var estimator = new CommitLingerEstimator(Max);
            var opened = 0;
            var now = TimeSpan.Zero;
            for (var cycle = 0; cycle < 500; cycle++)
            {
                // About one record per cycle at a random point of a 10 ms gap, as at 100 records/s.
                now = Cycles(estimator, 1, _ => random.Next(2) == 0 ? [] : [random.NextDouble() * 10], now);
                if (estimator.Current > TimeSpan.Zero) opened++;
            }
            Assert.True(opened < 25, $"run {run}: window open in {opened} of 500 cycles");
        }
    }

    [Fact]
    public void ARecordReachingAnIdleOwnerAlwaysAtTheSameDelayGainsNothing()
    {
        // A periodic source whose period nearly matches the write: each record arrives 0.5 ms after the commit, finds
        // the owner idle and starts the next write itself.
        var estimator = new CommitLingerEstimator(Max);
        Cycles(estimator, 500, _ => [0.5], TimeSpan.Zero);
        Assert.Equal(TimeSpan.Zero, estimator.Current);
    }

    [Fact]
    public void AnswersFromOtherNodesOpenAWindowCoveringThem()
    {
        var estimator = new CommitLingerEstimator(Max);
        // Local answers after 50 µs start the write; remote ones follow 1.15 ms later.
        Cycles(estimator, 200, _ => [0.05, 0.05, 0.05, 0.05, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2], TimeSpan.Zero);
        Assert.InRange(estimator.Current, Ms(1.15), Ms(1.6));
    }

    [Fact]
    public void AnswersAmongIndependentTrafficStillOpenAWindow()
    {
        var estimator = new CommitLingerEstimator(Max);
        Cycles(estimator, 300, _ => [.. Uniform(Write, 10), 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6], TimeSpan.Zero, queued: true);
        Assert.InRange(estimator.Current, Ms(0.6), Ms(0.8));
    }

    [Fact]
    public void WindowClosesWhenTheLoadBecomesIndependent()
    {
        var estimator = new CommitLingerEstimator(Max);
        var now = Cycles(estimator, 200, _ => [0.4, 0.4, 0.4, 0.4, 0.4, 0.4], TimeSpan.Zero, queued: true);
        Assert.True(estimator.Current > TimeSpan.Zero);
        Cycles(estimator, 400, _ => Uniform(Write, 30), now, queued: true);
        Assert.Equal(TimeSpan.Zero, estimator.Current);
    }

    [Fact]
    public void AnswersLaterThanTheMaximumAreIgnored()
    {
        var estimator = new CommitLingerEstimator(Max);
        Cycles(estimator, 200, _ => [5, 5, 5, 5, 5, 5], TimeSpan.Zero, queued: true);
        Assert.Equal(TimeSpan.Zero, estimator.Current);
    }
}
