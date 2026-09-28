using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace BTDB.Replication.Test;

public class SystemReplicationSchedulerTest
{
    [Fact]
    public async Task ElapsedIsMonotonicAndRunsAtWallRate()
    {
        var scheduler = new SystemReplicationScheduler();
        var wall = Stopwatch.StartNew();
        var start = scheduler.Elapsed;
        var previous = start;
        while (wall.Elapsed < TimeSpan.FromMilliseconds(300))
        {
            var now = scheduler.Elapsed;
            Assert.True(now >= previous);
            previous = now;
            await Task.Yield();
        }
        await Task.Delay(200);
        var measured = scheduler.Elapsed - start;
        var reference = wall.Elapsed;
        // Loose: the platform counter and Stopwatch agree to far better than this over half a second.
        Assert.InRange(measured.TotalMilliseconds, reference.TotalMilliseconds - 50, reference.TotalMilliseconds + 50);
    }

    [Fact]
    public async Task CallbacksRunAfterTheirDelayOneAtATimeAndNeverInline()
    {
        var scheduler = new SystemReplicationScheduler();
        var active = 0;
        var overlapped = false;
        var done = new CountdownEvent(8);
        var start = scheduler.Elapsed;
        var early = false;
        for (var i = 0; i < 8; i++)
            scheduler.Schedule(TimeSpan.FromMilliseconds(20), () =>
            {
                if (Interlocked.Increment(ref active) != 1) overlapped = true;
                if (scheduler.Elapsed - start < TimeSpan.FromMilliseconds(15)) early = true;
                Thread.Sleep(5);
                Interlocked.Decrement(ref active);
                done.Signal();
            }, "test");
        Assert.True(await Task.Run(() => done.Wait(TimeSpan.FromSeconds(10))));
        Assert.False(overlapped);
        Assert.False(early);
        var zero = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var scheduling = true;
        var caller = Environment.CurrentManagedThreadId;
        scheduler.Schedule(TimeSpan.Zero, () => zero.SetResult(scheduling && Environment.CurrentManagedThreadId == caller), "zero");
        scheduling = false;
        Assert.False(await zero.Task.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task DisposedWorkNeverStarts()
    {
        var scheduler = new SystemReplicationScheduler();
        var ran = false;
        scheduler.Schedule(TimeSpan.FromMilliseconds(30), () => ran = true, "cancelled").Dispose();
        var later = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        scheduler.Schedule(TimeSpan.FromMilliseconds(80), () => later.SetResult(), "later");
        await later.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(ran);
    }
}
