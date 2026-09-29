using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

static class EventLogScheduling
{
    /// <summary>Wait on the injected scheduler, so simulations control time. The cancellation registration is released
    /// when the delay ends, because callers pass long-lived tokens in retry loops.</summary>
    public static async Task DelayAsync(this IReplicationScheduler scheduler, TimeSpan delay, string description,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var work = scheduler.Schedule(delay, () => completion.TrySetResult(), description);
        await using var registration = cancellation.Register(() => completion.TrySetCanceled(cancellation));
        await completion.Task.ConfigureAwait(false);
    }
}
