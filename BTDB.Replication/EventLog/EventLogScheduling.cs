using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

static class EventLogScheduling
{
    /// <summary>Wait on the injected scheduler, so simulations control time.</summary>
    public static Task DelayAsync(this IReplicationScheduler scheduler, TimeSpan delay, string description,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var work = scheduler.Schedule(delay, () => completion.TrySetResult(), description);
        if (cancellation.CanBeCanceled)
            cancellation.Register(() =>
            {
                work.Dispose();
                completion.TrySetCanceled(cancellation);
            });
        return completion.Task;
    }
}
