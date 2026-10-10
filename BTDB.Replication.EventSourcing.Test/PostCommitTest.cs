using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class PostCommitTest
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActionsSeeTheCompleteVisibleBatchAndDiscardTheRolledBackEvent(bool failLast)
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var handler = new Handler { Fail = failLast };
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var buffered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        data.AfterRead = offset => { if (offset == 2) buffered.TrySetResult(); };
        var executed = new List<ulong>();
        await using var publisher = db.Publisher(data, metadata, handler);
        handler.Consuming = ev =>
        {
            var id = ev.EventId;
            Ambient.RegisterPostCommit(() =>
            {
                Assert.True(Ambient.IsQuery);
                Assert.False(Ambient.IsReplay);
                var expected = failLast ? 2ul : 3ul;
                Assert.Equal(expected, publisher.LastAppliedEventId);
                Assert.Equal((int)expected, Ambient.Relation<IEntries>().Count());
                executed.Add(id);
            });
            if (id != 1) return;
            entered.TrySetResult();
            if (!release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Batch gate was not released.");
        };
        await publisher.StartAsync();
        using (publisher.StartQuery()) Assert.Throws<InvalidOperationException>(() => Ambient.RegisterPostCommit(() => { }));
        var first = publisher.PublishAsync(new AddPure { Amount = 1 }).AsTask();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var second = publisher.PublishAsync(new AddPure { Amount = 2 }).AsTask();
            var third = publisher.PublishAsync(new AddPure { Amount = failLast ? -3 : 3 }).AsTask();
            await buffered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            release.Set();
            await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(10));
            if (failLast)
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.Completion);
                await Assert.ThrowsAnyAsync<Exception>(() => third);
                Assert.Equal(new[] { 1ul, 2ul }, executed);
                await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.DisposeAsync().AsTask());
            }
            else
            {
                await third.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.Equal(new[] { 1ul, 2ul, 3ul }, executed);
            }
        }
        finally { release.Set(); }
    }

    [Fact]
    public async Task ReplayIgnoresRegistrationsAndLiveEventsRunActionsOnEveryNode()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var ah = new Handler();
        var bh = new Handler();
        var aActions = 0;
        var bActions = 0;
        ah.Consuming = _ => Ambient.RegisterPostCommit(() => Interlocked.Increment(ref aActions));
        bh.Consuming = _ => Ambient.RegisterPostCommit(() => Interlocked.Increment(ref bActions));
        await using var ap = a.Publisher(data, metadata, ah);
        await ap.StartAsync();
        await ap.PublishAsync(new AddPure { Amount = 1 });
        await using var bp = b.Publisher(data, metadata, bh);
        await bp.StartAsync();
        Assert.Equal(1, aActions);
        Assert.Equal(0, bActions);
        await ap.PublishAsync(new AddPure { Amount = 2 });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (Volatile.Read(ref bActions) != 1) await Task.Delay(1, timeout.Token);
        Assert.Equal(2, aActions);
        Assert.Equal(1, bActions);
    }

    [Fact]
    public async Task ActionFailurePreservesCommittedStateRunsRemainingActionsAndDoesNotRepeatOnRestart()
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var handler = new Handler();
        var remaining = false;
        handler.Consuming = _ =>
        {
            Ambient.RegisterPostCommit(() => throw new ArgumentException("External effect failed"));
            Ambient.RegisterPostCommit(() => remaining = true);
        };
        var publisher = db.Publisher(data, metadata, handler);
        await publisher.StartAsync();
        await publisher.PublishAsync(new AddPure { Amount = 1 });
        await Assert.ThrowsAsync<ArgumentException>(() => publisher.Completion);
        Assert.True(remaining);
        Assert.Single(db.Read());
        Assert.Equal(1ul, publisher.LastAppliedEventId);
        await Assert.ThrowsAsync<ArgumentException>(() => publisher.DisposeAsync().AsTask());
        await using var resumed = db.Publisher(data, metadata, handler);
        await resumed.StartAsync();
        using var coldDb = await TestDatabase.OpenAsync();
        await using var cold = coldDb.Publisher(data, metadata, handler);
        await cold.StartAsync();
    }
}
