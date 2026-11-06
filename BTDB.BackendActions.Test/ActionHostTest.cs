using Xunit;

namespace BTDB.BackendActions.Test;

public class ActionHostTest : IAsyncLifetime
{
    readonly MemoryTopic _data = new("data");
    readonly MemoryTopic _metadata = new("metadata");
    readonly TestTime _time = new();
    readonly FakeCluster _cluster = new() { Leader = "a" };
    readonly List<(CancellationTokenSource Stop, Task Run)> _hosts = new();
    Node _node = null!;

    public async Task InitializeAsync() => _node = await Node.StartAsync(_data, _metadata, _time);

    public async Task DisposeAsync()
    {
        foreach (var (stop, run) in _hosts)
        {
            stop.Cancel();
            await run.WaitAsync(TimeSpan.FromSeconds(10));
        }
        await _node.DisposeAsync();
    }

    BackendActionsOptions Options(TimeSpan? lease = null) => new()
    {
        NodeId = "a", WorkerCpu = 1000, PollTimeout = TimeSpan.FromMilliseconds(200),
        RetryDelay = TimeSpan.FromMilliseconds(20), PlanInterval = TimeSpan.FromMilliseconds(20),
        DelayedInterval = TimeSpan.FromMilliseconds(50), TimeProvider = _time,
        Planner = new() { Lease = lease ?? TimeSpan.FromSeconds(30), WorkerTimeout = TimeSpan.FromSeconds(2) }
    };

    BackendActionsHost? _host;

    CancellationTokenSource StartHost(BackendActionsOptions? options = null, ulong? generation = 0)
    {
        var stop = new CancellationTokenSource();
        _host = new BackendActionsHost(_node.Actions, _node.Publisher, _cluster.Node("a", generation), options ?? Options());
        _hosts.Add((stop, _host.RunAsync(stop.Token)));
        return stop;
    }

    Task<QualifiedActionId> Enqueue(IActionInput input, DateTime? startAt = null, ActionPriority? priority = null) =>
        _node.Query(a => a.EnqueueAsync(input, priority, startAt));

    static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!predicate()) await Task.Delay(5, timeout.Token);
    }

    Outcome[] Outcomes => _node.Recorder.Outcomes.ToArray();

    [Fact]
    public async Task EnqueuedActionsRunOnceAndFinishThroughTheirHandler()
    {
        StartHost();
        var ids = new List<QualifiedActionId>();
        for (var i = 1; i <= 5; i++) ids.Add(await Enqueue(new ProcessOrder { TenantId = (ulong)i % 2, Amount = i }));
        await UntilAsync(() => Outcomes.Length == 5 && _node.Queued.Length == 0);
        Assert.Equal(ids.OrderBy(i => i.ActionId), Outcomes.Select(o => o.Id).OrderBy(i => i.ActionId));
        Assert.All(Outcomes, o => Assert.Equal((ActionOutcomeKind.Completed, 1ul, o.Input), (o.Kind, o.Attempt, o.Result)));
    }

    [Fact]
    public async Task AConcurrencyLimitRunsOneAtATimeAndCancelStopsTheRunningOne()
    {
        StartHost();
        var first = await Enqueue(new SlowOrder { Mode = "wait" });
        var second = await Enqueue(new SlowOrder { Mode = "wait" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        Assert.True(await _node.Query(a => a.CancelAsync(first)));
        await UntilAsync(() => Outcomes.Length == 1 && _node.Recorder.Started.Count == 2);
        Assert.Equal((first, ActionOutcomeKind.Cancelled), (Outcomes[0].Id, Outcomes[0].Kind));
        _node.Recorder.Release.SetResult();
        await UntilAsync(() => Outcomes.Length == 2);
        Assert.Equal((second, ActionOutcomeKind.Completed, "done 1"), (Outcomes[1].Id, Outcomes[1].Kind, Outcomes[1].Result));
        Assert.Equal(1, _node.Recorder.MaxRunning);
    }

    [Fact]
    public async Task ACoordinatorReplacedByALateActivationStartsAgain()
    {
        StartHost();
        await Enqueue(new ProcessOrder { Amount = 1 });
        await UntilAsync(() => Outcomes.Length == 1);
        var replaced = _host!.Coordinator!.Session;
        // A previous leader's activation lands late and rejects every start of the running coordinator.
        await _node.Publisher.PublishAsync(new CoordinatorActivated { Session = Guid.NewGuid() });
        await Enqueue(new ProcessOrder { Amount = 2 });
        await UntilAsync(() => Outcomes.Length == 2);
        Assert.NotEqual(replaced, _host.Coordinator!.Session);
    }

    [Fact]
    public async Task HandlerFailuresFinishTheActionWithTheirCode()
    {
        StartHost();
        await Enqueue(new SlowOrder { Mode = "fail" });
        await Enqueue(new SlowOrder { Mode = "crash" });
        _node.Recorder.Release.SetResult();
        await UntilAsync(() => Outcomes.Length == 2);
        Assert.Equal([(ActionOutcomeKind.Failed, "Rejected"), (ActionOutcomeKind.Failed, "Unexpected")],
            Outcomes.Select(o => (o.Kind, o.Error)));
        Assert.Empty(_node.Queued);
    }

    [Fact]
    public async Task DelayedActionsRunOnceDue()
    {
        StartHost();
        await Enqueue(new ProcessOrder { Amount = 3 }, _time.Now.UtcDateTime.AddMinutes(1));
        await Task.Delay(200);
        Assert.Empty(Outcomes);
        Assert.Single(_node.Delayed);
        _time.Now = _time.Now.AddMinutes(1);
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Empty(_node.Delayed);
    }

    [Fact]
    public async Task PartialResultsArriveInOrderBeforeTheFinalOne()
    {
        StartHost();
        var id = await Enqueue(new ExportOrder { Parts = 3 });
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Equal((ActionOutcomeKind.Completed, "all", 1ul), (Outcomes[0].Kind, Outcomes[0].Result, Outcomes[0].Attempt));
        Assert.Equal([(id, 1ul, "part 1", 0), (id, 1ul, "part 2", 0), (id, 1ul, "part 3", 0)], _node.Recorder.Parts);
        Assert.Empty(_node.Queued);
    }

    [Fact]
    public async Task AnOutcomeTooLargeToPublishFailsTheAttemptWithoutRunningItAgain()
    {
        await _node.DisposeAsync();
        _node = await Node.StartAsync(_data, _metadata, _time, maximumEventSize: 64 * 1024);
        StartHost();
        var id = await Enqueue(new ExportOrder { Parts = 1, ResultSize = 100_000 });
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Equal((id, ActionOutcomeKind.Failed, 1ul, "OutcomeTooLarge"),
            (Outcomes[0].Id, Outcomes[0].Kind, Outcomes[0].Attempt, Outcomes[0].Error));
        await Task.Delay(200);
        Assert.Single(_node.Recorder.Parts); // executed once
        Assert.Empty(_node.Queued);
    }

    [Fact]
    public async Task AResultReturnedDespiteCancellationCompletesTheAction()
    {
        StartHost();
        var id = await Enqueue(new WaitingOrder { Mode = "finish" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        Assert.True(await _node.Query(a => a.CancelAsync(id)));
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Equal((ActionOutcomeKind.Completed, "done 1"), (Outcomes[0].Kind, Outcomes[0].Result));
        Assert.Equal([ActionCancellationReason.Cancelled], _node.Recorder.Reasons);
    }

    [Fact]
    public async Task AHandlerAcknowledgesCancellationByReturningWithoutAnException()
    {
        StartHost();
        var id = await Enqueue(new WaitingOrder { Mode = "acknowledge" });
        var cancelling = await Enqueue(new WaitingOrder { Mode = "self-cancel" });
        await UntilAsync(() => _node.Recorder.Running == 1 && Outcomes.Length == 1);
        Assert.True(await _node.Query(a => a.CancelAsync(id)));
        await UntilAsync(() => Outcomes.Length == 2);
        Assert.Equal([(cancelling, ActionOutcomeKind.Cancelled, (string?)null), (id, ActionOutcomeKind.Cancelled, null)],
            Outcomes.Select(o => (o.Id, o.Kind, o.Error)));
        Assert.Equal([ActionCancellationReason.Cancelled], _node.Recorder.Reasons);
    }

    [Fact]
    public async Task AReturnedInterruptionOfAStoppingWorkerUsesNoAttempt()
    {
        var stop = StartHost();
        await Enqueue(new WaitingOrder { Mode = "acknowledge" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        stop.Cancel();
        await _hosts[^1].Run.WaitAsync(TimeSpan.FromSeconds(10));
        var row = Assert.Single(_node.Queued);
        Assert.Equal((false, 1ul, 1ul), (row.IsStarted, row.RunAttempts, row.InterruptedAttempts));
        Assert.Equal([ActionCancellationReason.Interrupted], _node.Recorder.Reasons);
    }

    [Fact]
    public async Task AnInterruptionCommitsItsProgressInTheSameEvent()
    {
        var stop = StartHost();
        var id = await Enqueue(new WaitingOrder { Mode = "progress" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        var before = _node.Publisher.LastAppliedEventId;
        stop.Cancel();
        await _hosts[^1].Run.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal([(id, ActionOutcomeKind.Progress, 1ul, "progress 1")],
            Outcomes.Select(o => (o.Id, o.Kind, o.Attempt, o.Result)));
        var row = Assert.Single(_node.Queued);
        Assert.Equal((false, 1ul), (row.IsStarted, row.InterruptedAttempts));
        Assert.Equal(before + 1, _node.Publisher.LastAppliedEventId); // one event: the interruption with its progress
    }

    [Fact]
    public async Task AnInterruptionTheHandlerChoseItselfCountsAsAnAttempt()
    {
        var options = Options();
        options.Planner.MaxAttempts = 2;
        StartHost(options);
        var id = await Enqueue(new WaitingOrder { Mode = "self-interrupt" });
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Equal((id, ActionOutcomeKind.Interrupted, 2ul, "AttemptsExhausted"),
            (Outcomes[0].Id, Outcomes[0].Kind, Outcomes[0].Attempt, Outcomes[0].Error));
        Assert.Equal(2, _node.Recorder.Started.Count);
    }

    [Fact]
    public async Task AResultReturnedWhileTheWorkerStopsIsPublished()
    {
        var stop = StartHost();
        var id = await Enqueue(new WaitingOrder { Mode = "finish" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        stop.Cancel();
        await _hosts[^1].Run.WaitAsync(TimeSpan.FromSeconds(10));
        var outcome = Assert.Single(Outcomes);
        Assert.Equal((id, ActionOutcomeKind.Completed, 1ul), (outcome.Id, outcome.Kind, outcome.Attempt));
        Assert.Equal([ActionCancellationReason.Interrupted], _node.Recorder.Reasons);
        Assert.Empty(_node.Queued);
    }

    [Fact]
    public async Task AFailureWhileTheWorkerStopsFinishesTheAction()
    {
        var stop = StartHost();
        var id = await Enqueue(new WaitingOrder { Mode = "fail-on-cancel" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        stop.Cancel();
        await _hosts[^1].Run.WaitAsync(TimeSpan.FromSeconds(10));
        var outcome = Assert.Single(Outcomes);
        Assert.Equal((id, ActionOutcomeKind.Failed, 1ul, "Rejected"),
            (outcome.Id, outcome.Kind, outcome.Attempt, outcome.Error));
        Assert.Equal([ActionCancellationReason.Interrupted], _node.Recorder.Reasons);
        Assert.Empty(_node.Queued);
    }

    [Fact]
    public async Task AnAttemptInterruptedByAStoppingWorkerRunsAgainWithoutUsingAnAttempt()
    {
        var options = Options();
        options.Planner.MaxAttempts = 1;
        var stop = StartHost(options);
        var id = await Enqueue(new WaitingOrder { Mode = "wait" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        stop.Cancel();
        await _hosts[^1].Run.WaitAsync(TimeSpan.FromSeconds(10));
        var row = Assert.Single(_node.Queued);
        Assert.Equal((false, 1ul, 1ul), (row.IsStarted, row.RunAttempts, row.InterruptedAttempts));
        StartHost(options);
        await UntilAsync(() => _node.Recorder.Started.Count == 2);
        _node.Recorder.Release.SetResult();
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Equal((id, ActionOutcomeKind.Completed, 2ul), (Outcomes[0].Id, Outcomes[0].Kind, Outcomes[0].Attempt));
    }

    [Fact]
    public async Task CancelAfterFinishesTheAttemptAsTimedOut()
    {
        StartHost();
        await Enqueue(new ExportOrder { Parts = 1, Timeout = true });
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Equal((ActionOutcomeKind.Cancelled, "Timeout"), (Outcomes[0].Kind, Outcomes[0].Error));
        Assert.Equal([ActionCancellationReason.Timeout], _node.Recorder.Reasons);
    }

    [Fact]
    public async Task ScheduledTypesStartOncePerPassedOccurrence()
    {
        var options = Options();
        options.ScheduleInterval = TimeSpan.FromMilliseconds(20);
        StartHost(options);
        static string Name() => typeof(HourlyOrder).FullName!;
        await UntilAsync(() => _node.Read<IScheduledActionRuns, ScheduledActionRun>().Length == 1);
        var first = _node.Read<IScheduledActionRuns, ScheduledActionRun>()[0];
        Assert.Equal((Name(), _time.Now.UtcDateTime), (first.Name, first.LastRun)); // recorded, not started
        await Task.Delay(100);
        Assert.Empty(Outcomes);

        _time.Now = _time.Now.AddMinutes(61);
        await UntilAsync(() => Outcomes.Length == 1);
        await Task.Delay(100);
        Assert.Equal(("HourlyOrder", ActionOutcomeKind.Completed), (Outcomes[0].Name, Outcomes[0].Kind));
        Assert.Single(Outcomes);

        _node.Recorder.AllowScheduled = false; // a vetoed occurrence is skipped, not retried
        _time.Now = _time.Now.AddHours(1);
        await UntilAsync(() => _node.Read<IScheduledActionRuns, ScheduledActionRun>()[0].LastRun == _time.Now.UtcDateTime.AddMinutes(-1));
        _node.Recorder.AllowScheduled = true;
        await Task.Delay(100);
        Assert.Single(Outcomes);
    }

    [Fact]
    public async Task SuspendedTypesStartNothingNewUntilResumed()
    {
        StartHost();
        var name = typeof(ProcessOrder).FullName!;
        await _node.Query(a => a.SuspendAsync([name]));
        await Enqueue(new ProcessOrder { Amount = 1 });
        await Enqueue(new RefreshCache { Key = "other types run" });
        await UntilAsync(() => Outcomes.Length == 1);
        await Task.Delay(200);
        Assert.Equal("RefreshCache", Assert.Single(Outcomes).Name);
        await _node.Query(a => a.SuspendAsync([name], suspended: false));
        await UntilAsync(() => Outcomes.Length == 2);
    }

    [Fact]
    public async Task SuspendedTenantsStartNothingNewUntilResumed()
    {
        StartHost();
        await _node.Query(a => a.SuspendTenantsAsync([5]));
        await Enqueue(new ProcessOrder { TenantId = 5, Amount = 1 });
        await Enqueue(new ProcessOrder { TenantId = 6, Amount = 2 });
        await UntilAsync(() => Outcomes.Length == 1);
        await Task.Delay(200);
        Assert.Equal(6ul, Assert.Single(Outcomes).Id.TenantId);
        Assert.Equal([5ul], (await _host!.GetOverviewAsync()).SuspendedTenants);
        await _node.Query(a => a.SuspendTenantsAsync([5], suspended: false));
        await UntilAsync(() => Outcomes.Length == 2);
    }

    [Fact]
    public async Task TheOverviewAndMetricsShowTheCoordinatorsView()
    {
        StartHost();
        var host = _host!;
        var finished = 0L;
        var waiting = new List<int>();
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, host.Metrics.Meter)) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "btdb.backend_actions.attempts.finished") Interlocked.Add(ref finished, value);
        });
        listener.SetMeasurementEventCallback<int>((instrument, value, _, _) =>
        {
            if (instrument.Name == "btdb.backend_actions.waiting") lock (waiting) waiting.Add(value);
        });
        listener.Start();

        await Enqueue(new SlowOrder { Mode = "wait" });
        await Enqueue(new SlowOrder { Mode = "wait" }, priority: ActionPriority.Low); // waits for the limit of 1
        await UntilAsync(() => _node.Recorder.Running == 1);
        await UntilAsync(() => host.Coordinator?.Overview().Priorities.Sum(p => p.Waiting) == 1);
        var overview = await host.GetOverviewAsync();
        var worker = Assert.Single(overview.Workers);
        Assert.Equal(("a", 1000, 100, 1, true), (worker.NodeId, worker.Capacity, worker.Used, worker.Attempts, worker.Ready));
        Assert.Equal([(ActionPriority.Normal, 0, 1), (ActionPriority.Low, 1, 0)],
            overview.Priorities.Select(p => (p.Priority, p.Waiting, p.Running)));
        listener.RecordObservableInstruments();
        lock (waiting) Assert.Equal([1], waiting);

        _node.Recorder.Release.SetResult();
        await UntilAsync(() => Interlocked.Read(ref finished) == 2);
    }

    [Fact]
    public async Task TheCoordinatorGoesOnWhenAConsumeRejectsOneOfItsEvents()
    {
        // Application bugs in OnFinished and in the failure hook make the due event of one delayed input fail
        // deterministically, on every replica.
        _node.Recorder.AfterFinished = (input, _) =>
        {
            if (input is ShiftingItems) throw new InvalidOperationException("A bug in OnFinished.");
        };
        _node.Actions.OnFinishedFailed = (_, _, error) => throw new InvalidOperationException("A bug in the hook.", error);
        StartHost();
        var host = _host!;
        var rejected = 0L;
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, host.Metrics.Meter)) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "btdb.backend_actions.coordinator.rejected_events") Interlocked.Add(ref rejected, value);
        });
        listener.Start();
        var later = _time.Now.UtcDateTime.AddMinutes(1);
        await Enqueue(new ShiftingItems { Key = "throw" }, later); // cannot queue: its GetUniqueKey throws
        await Enqueue(new ProcessOrder { Amount = 1 }, later);
        await UntilAsync(() => host.Coordinator != null);
        var coordinator = host.Coordinator;
        _time.Now = _time.Now.AddMinutes(2);
        // The batch is rejected, so its actions are published one by one, and only the broken one is rejected again.
        await UntilAsync(() => Outcomes.Any(o => o.Name == nameof(ProcessOrder)));
        await UntilAsync(() => Interlocked.Read(ref rejected) >= 2);
        Assert.Equal(nameof(ShiftingItems), Assert.Single(_node.Delayed).Input!.GetType().Name);
        await Enqueue(new ProcessOrder { Amount = 2 });
        await UntilAsync(() => Outcomes.Count(o => o.Name == nameof(ProcessOrder)) == 2);
        Assert.Same(coordinator, host.Coordinator); // the same session went on
    }

    [Fact]
    public async Task TheHostTakesTheReplicationNodesGenerationAndRejectsADifferentOne()
    {
        BackendActionsOptions With(ulong? generation, string? nodeId = "a")
        {
            var options = Options();
            options.ApplicationGeneration = generation;
            options.NodeId = nodeId;
            return options;
        }

        BackendActionsHost Create(BackendActionsOptions options, ulong? fromNode) =>
            new(_node.Actions, _node.Publisher, _cluster.Node("a", fromNode), options);

        Assert.Contains("is 4, but the replication node's is 5",
            Assert.Throws<ArgumentException>(() => Create(With(4), 5)).Message);
        Assert.Contains("ApplicationGeneration", Assert.Throws<ArgumentException>(() => Create(With(null), null)).Message);
        // The test publisher has no EventCluster, so the node ID must be set.
        Assert.Contains("EventPublisherOptions.Cluster", Assert.Throws<ArgumentException>(() => Create(With(5, null), 5)).Message);
        Assert.Equal("a", Create(With(4), 4).NodeId);
        Assert.Equal("a", Create(With(9), null).NodeId); // a node that does not know its generation takes the setting

        StartHost(With(null), generation: 7);
        var host = _host!;
        await UntilAsync(() => host.Coordinator?.Overview().Workers.Count == 1);
        Assert.Equal(7ul, Assert.Single(host.Coordinator!.Overview().Workers).Generation);
    }

    [Fact]
    public async Task ADueBacklogLargerThanOnePassIsQueuedInBoundedPasses()
    {
        StartHost();
        await _node.Query(a => a.SuspendAsync([typeof(ProcessOrder).FullName!])); // queue them, but run none
        var due = _time.Now.UtcDateTime.AddMinutes(1);
        const int count = ActionCoordinator.DueBatch * ActionCoordinator.DueBatchesPerPass + 2_500;
        await _node.Query(a => a.ImportAsync(Enumerable.Range(1, count).Select(i => new ImportedAction
        {
            TenantId = (ulong)i % 7, ActionId = (ulong)i, Input = new ProcessOrder { Amount = i }, Order = (ulong)i,
            QueuedAt = _time.Now.UtcDateTime, StartAt = due
        }).ToList()));
        Assert.Equal(count, _node.Delayed.Length);
        _time.Now = _time.Now.AddMinutes(2);
        await UntilAsync(() => _node.Queued.Length == count);
        Assert.Empty(_node.Delayed);
    }

    [Fact]
    public async Task TheTestContextRecordsWhatAHandlerAsks()
    {
        var context = new Testing.TestActionContext(new(1, 2), attempt: 3);
        var handler = new Recorder.ExportProbe();
        Assert.Equal("all", (await handler.ExecuteAsync(new ExportOrder { Parts = 2 }, context)).Result!.Text);
        Assert.Equal([(1ul, 2ul, 3ul, "part 1"), (1ul, 2ul, 3ul, "part 2")],
            context.Partials.Cast<ExportPart>().Select(p => (p.TenantId, p.ActionId, p.Attempt, p.Text)));
        Assert.Equal(-1000, context.CpuDelta);
        context.Cancel(ActionCancellationReason.Timeout);
        Assert.Equal(ActionCancellationReason.Timeout, context.CancellationReason);
        await Assert.ThrowsAsync<OperationCanceledException>(() => context.SubmitPartialAsync(new ExportPart()).AsTask());
    }

    [Fact]
    public async Task ImportedActionsRunOnceWithTheirNextAttempt()
    {
        await _node.Query(a => a.ImportAsync(ActionImportTest.OldRows()));
        StartHost();
        await UntilAsync(() => Outcomes.Length == 5); // the cancelled one at import, then the four queued
        Assert.Equal([(500ul, ActionOutcomeKind.Completed, 3ul)],
            Outcomes.Where(o => o.Id.ActionId == 500).Select(o => (o.Id.ActionId, o.Kind, o.Attempt)));
        Assert.Equal(4, Outcomes.Count(o => o.Kind == ActionOutcomeKind.Completed));
        Assert.Single(_node.Delayed); // still waiting for its start time
        Assert.Empty(_node.Queued);
    }

    [Fact]
    public async Task ARestartedNodeRunsItsInterruptedAttemptAgain()
    {
        var first = StartHost(Options(TimeSpan.FromMilliseconds(500)));
        var id = await Enqueue(new SlowOrder { Mode = "wait" });
        await UntilAsync(() => _node.Recorder.Running == 1);
        first.Cancel(); // the worker stops without an outcome
        await _hosts[0].Run.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Empty(Outcomes);
        Assert.Equal(1ul, Assert.Single(_node.Queued).RunAttempts);

        _node.Recorder.Release.SetResult();
        StartHost(Options(TimeSpan.FromMilliseconds(500))); // a new coordinator protects the attempt for one lease
        await UntilAsync(() => Outcomes.Length == 1);
        Assert.Equal((id, ActionOutcomeKind.Completed, 2ul, "done 2"),
            (Outcomes[0].Id, Outcomes[0].Kind, Outcomes[0].Attempt, Outcomes[0].Result));
        Assert.Equal([(id, 1ul), (id, 2ul)], _node.Recorder.Started);
    }
}
