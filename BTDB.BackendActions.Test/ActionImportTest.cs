using BTDB.Replication.EventSourcing;
using Xunit;

namespace BTDB.BackendActions.Test;

public class ActionImportTest
{
    readonly MemoryTopic _data = new("data");
    readonly MemoryTopic _metadata = new("metadata");
    readonly TestTime _time = new();

    static readonly DateTime Queued = new(2026, 9, 1, 8, 0, 0, DateTimeKind.Utc);

    /// <summary>Old rows, out of order: their old Order decides the new one.</summary>
    internal static List<ImportedAction> OldRows() =>
    [
        new() { TenantId = 1, ActionId = 500, Order = 30, Input = new ProcessOrder { Amount = 3 }, QueuedAt = Queued, RunAttempts = 2 },
        new() { TenantId = 2, ActionId = 400, Order = 10, Input = new RefreshCache { Key = "k" }, Priority = ActionPriority.Low, QueuedAt = Queued },
        new() { TenantId = 2, ActionId = 410, Order = 20, Input = new CollectItems { Key = "c", Items = [1] }, QueuedAt = Queued, IsAggregable = false },
        new() { TenantId = 2, ActionId = 420, Order = 25, Input = new CollectItems { Key = "c", Items = [2] }, QueuedAt = Queued },
        new() { TenantId = 1, ActionId = 900, Order = 40, Input = new ProcessOrder { Amount = 9 }, QueuedAt = Queued, StartAt = Queued.AddDays(60) },
        new() { TenantId = 1, ActionId = 950, Order = 50, Input = new ProcessOrder { Amount = 5 }, QueuedAt = Queued, IsCancelled = true, RunAttempts = 1 }
    ];

    static void AssertImported(Node node)
    {
        Assert.Equal([(2ul, 400ul, 1ul, ActionPriority.Low, 0ul), (2, 410, 2, ActionPriority.Normal, 0),
                (2, 420, 3, ActionPriority.Normal, 0), (1, 500, 4, ActionPriority.Normal, 2)],
            node.Queued.OrderBy(q => q.Order).Select(q => (q.TenantId, q.ActionId, q.Order, q.Priority, q.RunAttempts)));
        Assert.All(node.Queued, q => Assert.False(q.IsStarted));
        Assert.Equal([false, true], node.Queued.Where(q => q.TenantId == 2 && q.ActionId != 400).Select(q => q.IsAggregable));
        Assert.Equal(Queued, node.Queued[0].QueuedAt);
        Assert.Equal([(2ul, "c", 420ul), (2, "k", 400)], node.Indexes.Select(i => (i.TenantId, i.UniqueKey, i.ActionId)));
        Assert.Equal((900ul, Queued.AddDays(60)), (Assert.Single(node.Delayed).ActionId, node.Delayed[0].StartAt));
        var cancelled = Assert.Single(node.Recorder.Outcomes);
        Assert.Equal((new QualifiedActionId(1, 950), ActionOutcomeKind.Cancelled, 1ul), (cancelled.Id, cancelled.Kind, cancelled.Attempt));
        Assert.Equal(3, Assert.IsType<ProcessOrder>(node.Queued.Single(q => q.ActionId == 500).Input).Amount);
    }

    [Fact]
    public async Task AnImportEventKeepsIdsOrderIndexesAndRaisesTheAllocator()
    {
        await using var node = await Node.StartAsync(_data, _metadata, _time);
        await node.Actions.ImportAsync(node.Publisher, OldRows(),
            new Dictionary<string, DateTime> { [typeof(HourlyOrder).FullName!] = Queued }, ["refresh"]);
        AssertImported(node);
        var hourly = node.Read<IScheduledActionRuns, ScheduledActionRun>().Single();
        Assert.Equal((typeof(HourlyOrder).FullName, Queued), (hourly.Name, hourly.LastRun));
        using (node.Publisher.StartQuery()) Assert.Equal(["refresh"], node.Actions.SuspendedActions);

        // New actions get IDs above every imported one, merge into imported keys and queue after them.
        var fresh = await node.Actions.EnqueueAsync(node.Publisher, new ProcessOrder { TenantId = 1 });
        Assert.Equal(new QualifiedActionId(1, 951), fresh);
        Assert.Equal(new QualifiedActionId(2, 420),
            await node.Actions.EnqueueAsync(node.Publisher, new CollectItems { TenantId = 2, Key = "c", Items = [3] }));
        Assert.Equal(5ul, node.Queued.Single(q => q.ActionId == 951).Order);
    }

    [Fact]
    public async Task AnApplicationConsumeImportsItsOldTablesTheSameWay()
    {
        await using var node = await Node.StartAsync(_data, _metadata, _time);
        await node.Publisher.PublishAsync(new PlaceOrder { Migrate = true });
        AssertImported(node);
        Assert.Equal(_time.Now.UtcDateTime, node.Read<IScheduledActionRuns, ScheduledActionRun>().Single().LastRun);
    }

    [Fact]
    public async Task ImportingAnExistingIdFailsTheWholeEvent()
    {
        await using var node = await Node.StartAsync(_data, _metadata, _time);
        await node.Actions.ImportAsync(node.Publisher, OldRows());
        var queued = node.Queued.Length;
        await Assert.ThrowsAsync<InvalidOperationException>(() => node.Actions.ImportAsync(node.Publisher,
            [new() { TenantId = 7, ActionId = 1, Input = new ProcessOrder() }, OldRows()[0]]));
        Assert.Equal(queued, node.Queued.Length);
        Assert.DoesNotContain(node.Queued, q => q.TenantId == 7);
    }

    [Fact]
    public async Task ReplicasAndAColdReplayImportTheSameBytes()
    {
        await using var first = await Node.StartAsync(_data, _metadata, _time, batch: 1);
        var genesis = first.CopyFiles();
        await using var second = await Node.StartAsync(_data, _metadata, _time, 64, first.CopyFiles());
        await first.Actions.ImportAsync(first.Publisher, OldRows()[..3]);
        await second.Publisher.PublishAsync(new PlaceOrder { Input = new ProcessOrder { Amount = 1 } });
        await first.Actions.ImportAsync(first.Publisher, OldRows()[3..]);
        var end = first.Publisher.LastAppliedEventId;
        using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            while (second.Publisher.LastAppliedEventId != end) await Task.Delay(1, timeout.Token);
        await using var cold = await Node.StartAsync(_data, _metadata, _time, 7, genesis);
        Assert.Equal(first.NativeTrl(), second.NativeTrl());
        Assert.Equal(first.NativeTrl(), cold.NativeTrl());
        Assert.Equal(first.Recorder.Outcomes, cold.Recorder.Outcomes);
    }
}
