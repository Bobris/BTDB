using System;
using System.Threading.Channels;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class LeaseMaintenanceTest
{
    sealed class Storage : IReplicationLeaseStorage
    {
        public bool Available = true;
        public int Acquires, Renews;
        public TaskCompletionSource<TimeSpan?>? PendingRenewal;
        public readonly TaskCompletionSource RenewalStarted = new();
        public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation)
        {
            Acquires++;
            if (!Available) throw new IOException("Storage outage");
            return ValueTask.FromResult<LeaseGrant?>(new(Acquires.ToString(), TimeSpan.FromTicks(100)));
        }
        public ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
        {
            Renews++;
            RenewalStarted.TrySetResult();
            if (!Available) throw new IOException("Storage outage");
            return PendingRenewal != null
                ? new(PendingRenewal.Task.WaitAsync(cancellation))
                : ValueTask.FromResult<TimeSpan?>(TimeSpan.FromTicks(100));
        }
    }

    [Fact]
    public async Task ScheduledOutageRetriesReacquireWithoutRevivingOldAuthority()
    {
        var clock = new DeterministicScheduler(601);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        using var cancellation = new CancellationTokenSource();
        var observations = Channel.CreateUnbounded<LeaseAuthority?>();
        var run = controller.RunAsync(TimeSpan.FromTicks(20), value => observations.Writer.TryWrite(value), cancellation.Token);
        await observations.Reader.ReadAsync();
        var first = controller.Current!;
        storage.Available = false;
        for (var i = 0; i < 6; i++)
        {
            clock.AdvanceBy(TimeSpan.FromTicks(20));
            await observations.Reader.ReadAsync();
        }
        Assert.False(run.IsCompleted);
        Assert.True(first.IsFenced);
        Assert.Null(controller.Current);

        storage.Available = true;
        clock.AdvanceBy(TimeSpan.FromTicks(20));
        await observations.Reader.ReadAsync();
        Assert.NotNull(controller.Current);
        Assert.NotSame(first, controller.Current);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        var attempts = storage.Acquires + storage.Renews;
        clock.AdvanceBy(TimeSpan.FromTicks(1000));
        Assert.Equal(attempts, storage.Acquires + storage.Renews);
        Assert.Null(controller.Current);
    }

    [Fact]
    public async Task HealthyRenewalRunsBeforeDeadlineEvenWithLongRetryInterval()
    {
        var clock = new DeterministicScheduler(602);
        var storage = new Storage();
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        using var cancellation = new CancellationTokenSource();
        var observations = Channel.CreateUnbounded<LeaseAuthority?>();
        var run = controller.RunAsync(TimeSpan.FromTicks(1000), value => observations.Writer.TryWrite(value), cancellation.Token);
        await observations.Reader.ReadAsync();
        var first = controller.Current!;
        for (var i = 0; i < 5; i++)
        {
            clock.AdvanceBy(TimeSpan.FromTicks(50));
            await observations.Reader.ReadAsync();
        }
        Assert.Equal(5, storage.Renews);
        Assert.Same(first, controller.Current);
        Assert.Equal(1, storage.Acquires);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
    }

    [Fact]
    public async Task PendingRenewalDoesNotOverlapAndShutdownCancelsIt()
    {
        var clock = new DeterministicScheduler(603);
        var storage = new Storage { PendingRenewal = new() };
        var controller = new LeaseSessionController(storage, clock.CreateScope("node"), 0, TimeSpan.Zero);
        using var cancellation = new CancellationTokenSource();
        var run = controller.RunAsync(TimeSpan.FromTicks(20), _ => { }, cancellation.Token);
        clock.AdvanceBy(TimeSpan.FromTicks(20));
        await storage.RenewalStarted.Task;
        clock.AdvanceBy(TimeSpan.FromTicks(180));
        Assert.Equal(1, storage.Renews);
        Assert.Null(controller.Current);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        storage.PendingRenewal.SetResult(TimeSpan.FromTicks(100));
        Assert.Null(controller.Current);
        Assert.Equal(1, storage.Acquires);
    }
}
