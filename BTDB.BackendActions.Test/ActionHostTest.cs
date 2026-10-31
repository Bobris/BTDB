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

    CancellationTokenSource StartHost(BackendActionsOptions? options = null)
    {
        var stop = new CancellationTokenSource();
        var host = new BackendActionsHost(_node.Actions, _node.Publisher, _cluster.Node("a"), options ?? Options());
        _hosts.Add((stop, host.RunAsync(stop.Token)));
        return stop;
    }

    Task<QualifiedActionId> Enqueue(IActionInput input, DateTime? startAt = null) =>
        _node.Actions.EnqueueAsync(_node.Publisher, input, new() { StartAt = startAt });

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
