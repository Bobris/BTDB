using System;
using System.Threading;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest
{
    public class KeyValueDBCompactorSchedulerTest
    {
        [Fact]
        public void ItWillNotStartImidietly()
        {
            var run = false;
            using (new CompactorScheduler(token =>
                {
                    run = true;
                    return false;
                }))
            {
            }
            Assert.False(run, "Should not Start imidietly");
        }

        [Fact]
        public void ItWillRunFirstTimeAfterWaitTime()
        {
            var e = new AutoResetEvent(false);
            using (var s = new CompactorScheduler(token =>
                {
                    e.Set();
                    return false;
                }))
            {
                s.WaitTime = TimeSpan.FromMilliseconds(1);
                s.AdviceRunning();
                Assert.True(e.WaitOne(1000));
            }
        }

        [Fact]
        public void ItShouldRunAgainSoonIfCompactionSuccessfull()
        {
            var e = new AutoResetEvent(false);
            var first = true;
            using (var s = new CompactorScheduler(token =>
                {
                    if (first)
                    {
                        first = false;
                        return true;
                    }
                    e.Set();
                    return false;
                }))
            {
                s.WaitTime = TimeSpan.FromMilliseconds(1);
                s.AdviceRunning();
                Assert.True(e.WaitOne(1000));
                Assert.False(first);
            }
        }

        [Fact]
        public void ItShouldCancelRunningCompaction()
        {
            var e = new AutoResetEvent(false);
            using (var s = new CompactorScheduler(token =>
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
                }))
            {
                s.WaitTime = TimeSpan.FromMilliseconds(1);
                s.AdviceRunning();
                Assert.True(e.WaitOne(1000));
            }
            Assert.True(e.WaitOne(1000));
        }
    }
}