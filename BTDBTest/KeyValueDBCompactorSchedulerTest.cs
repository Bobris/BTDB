using System;
using System.Threading;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class KeyValueDBCompactorSchedulerTest
{
    [Fact]
    public void ItWillNotStartImmediately()
    {
        var run = false;
        using (var sch = new CompactorScheduler())
        {
            sch.AddCompactAction(token =>
            {
                run = true;
                return false;
            });
        }
        Assert.False(run, "Should not Start imidietly");
    }

    [Fact]
    public void ItWillRunFirstTimeAfterWaitTime()
    {
        var e = new AutoResetEvent(false);
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(token =>
            {
                e.Set();
                return false;
            });
            s.WaitTime = TimeSpan.FromMilliseconds(1);
            s.AdviceRunning(true);
            Assert.True(e.WaitOne(1000));
        }
    }

    [Fact]
    public void ItShouldRunAgainSoonIfCompactionSuccessfull()
    {
        var e = new AutoResetEvent(false);
        var first = true;
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(token =>
            {
                if (first)
                {
                    first = false;
                    return true;
                }
                e.Set();
                return false;
            });
            s.WaitTime = TimeSpan.FromMilliseconds(1);
            s.AdviceRunning(true);
            Assert.True(e.WaitOne(1000));
            Assert.False(first);
        }
    }

    [Fact]
    public void ItShouldCancelRunningCompaction()
    {
        var e = new AutoResetEvent(false);
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(token =>
            {
                e.Set();
                while (true)
                {
                    if (token.IsCancellationRequested)
                    {
                        e.Set();
                        token.ThrowIfCancellationRequested();
                    }
                }
            });
            s.WaitTime = TimeSpan.FromMilliseconds(1);
            s.AdviceRunning(true);
            Assert.True(e.WaitOne(1000));
        }
        Assert.True(e.WaitOne(1000));
    }

    [Fact]
    public void AdviceRunningWorksWellForSharedInstance()
    {
        var e = new AutoResetEvent(false);
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(token =>
            {
                e.Set();
                return false;
            });
            s.WaitTime = TimeSpan.FromMilliseconds(150);
            s.AdviceRunning(true);
            s.AdviceRunning(true);
            s.AdviceRunning(false);
            Assert.False(e.WaitOne(100));
            Assert.True(e.WaitOne(100));
            s.AdviceRunning(true);
            Assert.False(e.WaitOne(50));
            s.AdviceRunning(false);
            Assert.True(e.WaitOne(50));
        }
    }

    [Fact]
    public void DbOpenedAfterFirstCompactingIsCorrectlyPlanned()
    {
        var e = new AutoResetEvent(false);
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(token =>
            {
                e.Set();
                return false;
            });
            s.WaitTime = TimeSpan.FromMilliseconds(50);
            s.AdviceRunning(true);
            Assert.True(e.WaitOne(100));
            s.AdviceRunning(true);
            Assert.True(e.WaitOne(100));
        }
    }

    [Fact]
    public void MissingOpeningAdviceDoesNotBlockScheduling()
    {
        var e = new AutoResetEvent(false);
        using (var s = new CompactorScheduler())
        {
            s.AddCompactAction(token =>
            {
                e.Set();
                return false;
            });
            s.WaitTime = TimeSpan.FromMilliseconds(50);
            s.AdviceRunning(false);
            Assert.True(e.WaitOne(100));
        }
    }
}
