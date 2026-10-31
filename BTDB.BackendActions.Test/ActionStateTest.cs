using BTDB.Replication.EventSourcing;
using Xunit;

namespace BTDB.BackendActions.Test;

public class ActionStateTest
{
    readonly MemoryTopic _data = new("data");
    readonly MemoryTopic _metadata = new("metadata");
    readonly TestTime _time = new();

    Task<Node> Start(int batch = 32) => Node.StartAsync(_data, _metadata, _time, batch);

    static Task Place(Node node, IActionInput? input = null, ActionPriority priority = ActionPriority.Normal,
        DateTime? startAt = null, QualifiedActionId? cancel = null) => node.Publisher.PublishAsync(new PlaceOrder
    {
        Input = input, Priority = priority, StartAt = startAt, CancelTenantId = cancel?.TenantId ?? 0,
        CancelActionId = cancel?.ActionId ?? 0
    }).AsTask();

    static async Task<Guid> Activate(Node node)
    {
        var session = Guid.NewGuid();
        await node.Publisher.PublishAsync(new CoordinatorActivated { Session = session });
        return session;
    }

    static Task Started(Node node, Guid session, QualifiedActionId id, ulong attempt) =>
        node.Publisher.PublishAsync(new ActionsStarted
        {
            Session = session,
            Actions = [new() { TenantId = id.TenantId, ActionId = id.ActionId, Attempt = attempt, WorkerSession = session }]
        }).AsTask();

    static Task Finished(Node node, QualifiedActionId id, ulong attempt, ActionOutcomeKind kind, string? text = null) =>
        node.Publisher.PublishAsync(new ActionFinished
        {
            TenantId = id.TenantId, ActionId = id.ActionId, Attempt = attempt, Kind = kind,
            Result = text == null ? null : new OrderResult { Text = text },
            Error = kind == ActionOutcomeKind.Failed ? new() { Code = "Boom", Message = "failed" } : null
        }).AsTask();

    [Fact]
    public async Task EnqueueInAConsumeQueuesInItsTransactionWithADeterministicId()
    {
        await using var node = await Start();
        await Place(node, new ProcessOrder { TenantId = 7, Amount = 5 }, ActionPriority.Low);
        await Place(node, new ProcessOrder { TenantId = 7, Amount = 6 });
        Assert.Equal([new(7, 1), new QualifiedActionId(7, 2)], node.Recorder.Enqueued);
        var queued = node.Queued;
        Assert.Equal([(1ul, ActionPriority.Low, 0ul, 1ul), (2ul, ActionPriority.Normal, 1ul, 2ul)],
            queued.Select(q => (q.ActionId, q.Priority, q.Order, q.LatestInputEventId)));
        Assert.All(queued, q => Assert.Equal(typeof(ProcessOrder).FullName, q.Name));
        Assert.Equal(5, Assert.IsType<ProcessOrder>(queued[0].Input).Amount);
        Assert.All(queued, q => Assert.False(q.IsStarted || q.IsAggregable || q.IsCancelled));
        Assert.Throws<InvalidOperationException>(() =>
        {
            using (node.Publisher.StartQuery()) node.Actions.Enqueue(new ProcessOrder());
        });
    }

    [Fact]
    public async Task EnqueueAsyncAndCancelAsyncPublishFromOutsideAConsume()
    {
        await using var node = await Start();
        var id = await node.Actions.EnqueueAsync(node.Publisher, new ProcessOrder { TenantId = 3, Amount = 1 });
        Assert.Equal(new QualifiedActionId(3, 1), id);
        Assert.Single(node.Queued);
        Assert.True(await node.Actions.CancelAsync(node.Publisher, id));
        Assert.False(await node.Actions.CancelAsync(node.Publisher, id));
        Assert.Empty(node.Queued);
        var outcome = Assert.Single(node.Recorder.Outcomes);
        Assert.Equal((ActionOutcomeKind.Cancelled, 0ul), (outcome.Kind, outcome.Attempt));
        await Assert.ThrowsAsync<InvalidOperationException>(() => node.Actions.EnqueueAsync(node.Publisher, new UnregisteredInput()));
    }

    [Fact]
    public async Task UnificatorMergesIntoTheQueuedActionUntilItStarts()
    {
        await using var node = await Start();
        var session = await Activate(node);
        await Place(node, new RefreshCache { Key = "a" });
        await Place(node, new RefreshCache { Key = "a" });
        await Place(node, new RefreshCache { Key = "b" });
        Assert.Equal([new(0, 1), new(0, 1), new QualifiedActionId(0, 3)], node.Recorder.Enqueued);
        Assert.Equal([1ul, 3ul], node.Queued.Select(q => q.ActionId));
        Assert.Equal(3ul, node.Queued[0].LatestInputEventId); // the merged request's event
        Assert.Equal("refresh", node.Queued[0].Name);

        await Started(node, session, new(0, 1), 1);
        Assert.Equal(["b"], node.Indexes.Select(i => i.UniqueKey));
        await Place(node, new RefreshCache { Key = "a" }); // the running action computes before this request
        Assert.Equal(new QualifiedActionId(0, 4), node.Recorder.Enqueued.Last());
        await Place(node, new RefreshCache { Key = "a" });
        Assert.Equal(new QualifiedActionId(0, 4), node.Recorder.Enqueued.Last());
        Assert.Equal([1ul, 3ul, 4ul], node.Queued.Select(q => q.ActionId));
    }

    [Fact]
    public async Task AggregatorAbsorbsInputsUntilItRefusesOrStarts()
    {
        await using var node = await Start();
        var session = await Activate(node);
        await Place(node, new CollectItems { Key = "k", Items = [1] });
        await Place(node, new CollectItems { Key = "k", Items = [2, 3] });
        Assert.Equal("1,2,3", string.Join(",", Assert.IsType<CollectItems>(Assert.Single(node.Queued).Input).Items));
        await Place(node, new CollectItems { Key = "k", Items = [4] }); // refused: the first stops aggregating
        var queued = node.Queued;
        Assert.Equal([(1ul, false), (3ul, true)], queued.Select(q => (q.ActionId, q.IsAggregable)));
        Assert.Equal(3ul, Assert.Single(node.Indexes).ActionId);
        await Started(node, session, new(0, 3), 1);
        Assert.Empty(node.Indexes);
        Assert.False(node.Queued[1].IsAggregable);
        await Place(node, new CollectItems { Key = "k", Items = [5] });
        Assert.Equal([1ul, 3ul, 4ul], node.Queued.Select(q => q.ActionId));
    }

    [Fact]
    public async Task DelayedActionsQueueOnlyWhenDueAndThenUnify()
    {
        await using var node = await Start();
        var due = _time.Now.UtcDateTime.AddMinutes(5);
        await Place(node, new RefreshCache { Key = "a" }, ActionPriority.Low, due);
        await Place(node, new RefreshCache { Key = "a" }, startAt: _time.Now.UtcDateTime); // not in the future: queued now
        Assert.Equal([2ul], node.Queued.Select(q => q.ActionId));
        Assert.Equal((1ul, due), (Assert.Single(node.Delayed).ActionId, node.Delayed[0].StartAt));

        var key = new ActionKey { TenantId = 0, ActionId = 1 };
        await node.Publisher.PublishAsync(new DelayedActionsDue { Actions = [key] });
        Assert.Single(node.Delayed); // not due yet at this event's time
        _time.Now = _time.Now.AddMinutes(5);
        await node.Publisher.PublishAsync(new DelayedActionsDue { Actions = [key, new() { ActionId = 99 }] });
        Assert.Empty(node.Delayed);
        Assert.Equal([2ul], node.Queued.Select(q => q.ActionId)); // unified into the queued request

        await Place(node, new ProcessOrder(), startAt: due.AddHours(1));
        Assert.True(await node.Actions.CancelAsync(node.Publisher, new(0, 3)));
        Assert.Empty(node.Delayed);
        Assert.Empty(node.Recorder.Outcomes); // a delayed action is removed without an outcome
    }

    [Fact]
    public async Task CancelFinishesAnUnstartedActionAndMarksAStartedOne()
    {
        await using var node = await Start();
        var session = await Activate(node);
        await Place(node, new RefreshCache { Key = "a" });
        await Place(node, new ProcessOrder { Amount = 2 });
        await Place(node, cancel: new(0, 1));
        Assert.Empty(node.Indexes);
        var outcome = Assert.Single(node.Recorder.Outcomes);
        Assert.Equal(("RefreshCache", ActionOutcomeKind.Cancelled, 0ul), (outcome.Name, outcome.Kind, outcome.Attempt));

        await Started(node, session, new(0, 2), 1);
        await Place(node, cancel: new(0, 2));
        await Place(node, cancel: new(0, 2));
        await Place(node, cancel: new(0, 77));
        Assert.Equal([true, true, true, false], node.Recorder.Cancelled);
        var cancelled = Assert.Single(node.Queued);
        Assert.True(cancelled.IsCancelled && cancelled.IsStarted);
        await Started(node, session, new(0, 2), 2); // a cancelled action is never started again
        Assert.Equal(1ul, node.Queued[0].RunAttempts);
        await Finished(node, new(0, 2), 1, ActionOutcomeKind.Cancelled);
        Assert.Empty(node.Queued);
        Assert.Equal(2, node.Recorder.Outcomes.Count);
    }

    [Fact]
    public async Task StartsAreAcceptedOnlyFromTheCurrentSessionAndForTheNextAttempt()
    {
        await using var node = await Start();
        await Place(node, new ProcessOrder { Amount = 1 });
        var id = new QualifiedActionId(0, 1);
        await Started(node, Guid.NewGuid(), id, 1); // no coordinator activated yet
        Assert.False(node.Queued[0].IsStarted);
        var old = await Activate(node);
        var current = await Activate(node);
        await Started(node, old, id, 1); // a deposed coordinator
        await Started(node, current, id, 2); // skips an attempt
        Assert.False(node.Queued[0].IsStarted);
        await Started(node, current, id, 1);
        var row = node.Queued[0];
        Assert.Equal((true, 1ul, current), (row.IsStarted, row.RunAttempts, row.WorkerSession));
        Assert.Equal(node.Publisher.LastAppliedEventId, row.StartEventId);
        await Started(node, current, id, 2); // supersedes attempt 1 after its lease expired
        Assert.Equal(2ul, node.Queued[0].RunAttempts);
    }

    [Fact]
    public async Task OnlyTheCurrentAttemptFinishesAndPartialResultsKeepTheAction()
    {
        await using var node = await Start();
        var session = await Activate(node);
        await Place(node, new ProcessOrder { Amount = 9 });
        var id = new QualifiedActionId(0, 1);
        await Finished(node, id, 1, ActionOutcomeKind.Completed, "early"); // not started yet
        await Started(node, session, id, 1);
        await Finished(node, id, 1, ActionOutcomeKind.Partial, "part");
        await Started(node, session, id, 2);
        await Finished(node, id, 1, ActionOutcomeKind.Completed, "stale"); // the superseded attempt
        await Finished(node, id, 2, ActionOutcomeKind.Partial, "part2");
        Assert.Single(node.Queued);
        await Finished(node, id, 2, ActionOutcomeKind.Completed, "done");
        await Finished(node, id, 2, ActionOutcomeKind.Completed, "duplicate");
        Assert.Empty(node.Queued);
        Assert.Equal([(ActionOutcomeKind.Partial, 1ul, "part"), (ActionOutcomeKind.Partial, 2ul, "part2"),
                (ActionOutcomeKind.Completed, 2ul, "done")],
            node.Recorder.Outcomes.Select(o => (o.Kind, o.Attempt, o.Result)));
        Assert.All(node.Recorder.Outcomes, o => Assert.Equal("9", o.Input));
    }

    [Fact]
    public async Task FailedBeforeStartFinishesTheAttempt()
    {
        await using var node = await Start();
        var session = await Activate(node);
        await Place(node, new ValidatedOrder { Reject = false });
        await Place(node, new ValidatedOrder { Reject = true });
        await node.Publisher.PublishAsync(new ActionsStarted
        {
            Session = session,
            Actions = [new() { ActionId = 1, Attempt = 1 }, new() { ActionId = 2, Attempt = 1 }]
        });
        Assert.Equal([new(0, 1), new QualifiedActionId(0, 2)], node.Recorder.BeforeStarts);
        Assert.True(Assert.Single(node.Queued).IsStarted);
        var failed = Assert.Single(node.Recorder.Outcomes);
        Assert.Equal((new QualifiedActionId(0, 2), ActionOutcomeKind.Failed, 1ul, "BeforeStartFailed"),
            (failed.Id, failed.Kind, failed.Attempt, failed.Error));
    }

    [Fact]
    public async Task MonitoringQueriesReadTheLocalSnapshot()
    {
        await using var node = await Start();
        var session = await Activate(node);
        await Place(node, new ProcessOrder { TenantId = 1, Amount = 1 });
        await Place(node, new RefreshCache { TenantId = 2, Key = "k" });
        await Place(node, new ProcessOrder { TenantId = 1 }, ActionPriority.Low, _time.Now.UtcDateTime.AddHours(1));
        await Started(node, session, new(1, 1), 1);
        await node.Actions.SuspendAsync(node.Publisher, ["refresh", "other"]);
        await node.Actions.SuspendAsync(node.Publisher, ["other"], suspended: false);
        using var query = node.Publisher.StartQuery();
        Assert.Equal([(new QualifiedActionId(1, 3), ActionState.Delayed), (new(1, 1), ActionState.Started),
                (new(2, 2), ActionState.Queued)],
            node.Actions.ListActions().Select(a => (a.Id, a.State)));
        Assert.Equal([3ul, 1ul], node.Actions.ListActions(1).Select(a => a.Id.ActionId));
        var started = node.Actions.FindAction(new(1, 1))!;
        Assert.Equal((1ul, "", 1), (started.RunAttempts, started.WorkerNode, Assert.IsType<ProcessOrder>(started.Input).Amount));
        Assert.Equal(_time.Now.UtcDateTime.AddHours(1), node.Actions.FindAction(new(1, 3))!.StartAt);
        Assert.Null(node.Actions.FindAction(new(1, 2)));
        Assert.True(node.Actions.HasStartedActions(1));
        Assert.False(node.Actions.HasStartedActions(2));
        Assert.Equal(new QualifiedActionId(2, 2), node.Actions.FindByUniqueKey(2, "refresh", "k"));
        Assert.Null(node.Actions.FindByUniqueKey(1, "refresh", "k"));
        Assert.Equal(["refresh"], node.Actions.SuspendedActions);
    }

    [Fact]
    public async Task ReplicasWithDifferentBatchSizesWriteIdenticalTransactionLogs()
    {
        var first = await Start(batch: 1);
        await using var _ = first;
        var genesis = first.CopyFiles();
        await using var second = await Node.StartAsync(_data, _metadata, _time, 64, first.CopyFiles());
        var session = await Activate(first);
        for (var i = 1; i <= 12; i++)
        {
            var publisher = (i % 2 == 0 ? first : second).Publisher;
            await publisher.PublishAsync(new PlaceOrder
            {
                Input = (i % 3) switch
                {
                    0 => new RefreshCache { Key = (i % 2).ToString() },
                    1 => new CollectItems { Key = "k", Items = [i] },
                    _ => new ProcessOrder { TenantId = (ulong)i % 2, Amount = i }
                },
                StartAt = i == 5 ? _time.Now.UtcDateTime.AddMinutes(1) : null
            });
        }
        await Started(first, session, new(0, 1), 1);
        await Finished(second, new(0, 1), 1, ActionOutcomeKind.Completed, "ok");
        await first.Publisher.PublishAsync(new PlaceOrder { CancelActionId = 3 });
        var end = first.Publisher.LastAppliedEventId;
        await UntilAsync(() => second.Publisher.LastAppliedEventId == end);
        Assert.Equal(first.NativeTrl(), second.NativeTrl());
        Assert.Equal(first.Recorder.Outcomes, second.Recorder.Outcomes);
        Assert.Equal(first.Queued.Select(q => (q.TenantId, q.ActionId, q.Order)),
            second.Queued.Select(q => (q.TenantId, q.ActionId, q.Order)));
        // A cold replica replays the whole history to the same bytes, outcome callbacks included.
        await using var cold = await Node.StartAsync(_data, _metadata, _time, 7, genesis);
        Assert.Equal(end, cold.Publisher.LastAppliedEventId);
        Assert.Equal(first.NativeTrl(), cold.NativeTrl());
        Assert.Equal(first.Recorder.Outcomes, cold.Recorder.Outcomes);
    }

    static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!predicate()) await Task.Delay(1, timeout.Token);
    }
}

public class UnregisteredInput : ActionInput<OrderResult>;
