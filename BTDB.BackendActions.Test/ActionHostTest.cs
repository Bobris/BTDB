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

    CancellationTokenSource StartHost(BackendActionsOptions? options = null)
    {
        var stop = new CancellationTokenSource();
        _host = new BackendActionsHost(_node.Actions, _node.Publisher, _cluster.Node("a"), options ?? Options());
        _hosts.Add((stop, _host.RunAsync(stop.Token)));
        return stop;
    }

    Task<QualifiedActionId> Enqueue(IActionInput input, DateTime? startAt = null,
        ActionPriority priority = ActionPriority.Normal) =>
        _node.Actions.EnqueueAsync(_node.Publisher, input, new() { StartAt = startAt, Priority = priority });

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
        Assert.True(await _node.Actions.CancelAsync(_node.Publisher, first));
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
        await _node.Actions.SuspendAsync(_node.Publisher, [name]);
        await Enqueue(new ProcessOrder { Amount = 1 });
        await Enqueue(new RefreshCache { Key = "other types run" });
        await UntilAsync(() => Outcomes.Length == 1);
        await Task.Delay(200);
        Assert.Equal("RefreshCache", Assert.Single(Outcomes).Name);
        await _node.Actions.SuspendAsync(_node.Publisher, [name], suspended: false);
        await UntilAsync(() => Outcomes.Length == 2);
    }

    [Fact]
    public async Task SuspendedTenantsStartNothingNewUntilResumed()
    {
        StartHost();
        await _node.Actions.SuspendTenantsAsync(_node.Publisher, [5]);
        await Enqueue(new ProcessOrder { TenantId = 5, Amount = 1 });
        await Enqueue(new ProcessOrder { TenantId = 6, Amount = 2 });
        await UntilAsync(() => Outcomes.Length == 1);
        await Task.Delay(200);
        Assert.Equal(6ul, Assert.Single(Outcomes).Id.TenantId);
        Assert.Equal([5ul], (await _host!.GetOverviewAsync()).SuspendedTenants);
        await _node.Actions.SuspendTenantsAsync(_node.Publisher, [5], suspended: false);
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
    public async Task TheTestContextRecordsWhatAHandlerAsks()
    {
        var context = new Testing.TestActionContext(new(1, 2), attempt: 3);
        var handler = new Recorder.ExportProbe();
        Assert.Equal("all", (await handler.ExecuteAsync(new ExportOrder { Parts = 2 }, context)).Text);
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
        await _node.Actions.ImportAsync(_node.Publisher, ActionImportTest.OldRows());
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
