using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class WaitForAppliedTest
{
    [Fact]
    public async Task AWaiterCompletesWhenItsEventIsVisibleAndFailsWhenThePublisherStops()
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var publisher = db.Publisher(data, metadata, new Handler());
        var first = publisher.WaitForAppliedAsync(1); // waiting before the start is fine
        await publisher.StartAsync();
        Assert.False(first.IsCompleted);
        var second = publisher.WaitForAppliedAsync(2);
        await publisher.PublishAsync(new AddPure { Amount = 1 });
        await first.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(second.IsCompleted);
        await publisher.WaitForAppliedAsync(1).WaitAsync(TimeSpan.FromSeconds(10)); // already visible
        await publisher.PublishAsync(new AddPure { Amount = 2 });
        await second.WaitAsync(TimeSpan.FromSeconds(10));

        using var cancel = new CancellationTokenSource();
        var cancelled = publisher.WaitForAppliedAsync(10, cancel.Token);
        var never = publisher.WaitForAppliedAsync(10);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);
        await publisher.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => never.WaitAsync(TimeSpan.FromSeconds(10)));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publisher.WaitForAppliedAsync(3));
    }
}
