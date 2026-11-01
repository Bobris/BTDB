using Xunit;

namespace BTDB.BackendActions.Test;

/// <summary>Several replicas over shared topics, each with its own database, worker and coordinator.</summary>
public class ActionFailoverTest : IAsyncLifetime
{
    readonly MemoryTopic _data = new("data");
    readonly MemoryTopic _metadata = new("metadata");
    readonly TestTime _time = new();
    readonly FakeCluster _cluster = new() { Leader = "a" };
    readonly Dictionary<string, Node> _nodes = new();
    readonly List<(CancellationTokenSource Stop, Task Run)> _hosts = new();

    public async Task InitializeAsync()
    {
        var a = await Node.StartAsync(_data, _metadata, _time);
        _nodes["a"] = a;
        foreach (var name in new[] { "b", "c" })
            _nodes[name] = await Node.StartAsync(_data, _metadata, _time, files: a.CopyFiles());
    }

    public async Task DisposeAsync()
    {
        foreach (var node in _nodes.Values) node.Recorder.Release.TrySetResult();
        foreach (var (stop, run) in _hosts)
        {
            stop.Cancel();
            await run.WaitAsync(TimeSpan.FromSeconds(10));
        }
        foreach (var node in _nodes.Values) await node.DisposeAsync();
    }

    BackendActionsOptions Options(string node, TimeSpan lease, bool worker, ulong generation, TimeSpan workerTimeout) => new()
    {
        NodeId = node, WorkerCpu = 1000, RunWorker = worker, ApplicationGeneration = generation,
        PollTimeout = TimeSpan.FromMilliseconds(100), RetryDelay = TimeSpan.FromMilliseconds(20),
        PlanInterval = TimeSpan.FromMilliseconds(20), DelayedInterval = TimeSpan.FromMilliseconds(50), TimeProvider = _time,
        Planner = new() { Lease = lease, WorkerTimeout = workerTimeout }
    };

    CancellationTokenSource Host(string node, TimeSpan lease, bool worker = true, ulong generation = 1,
        TimeSpan? workerTimeout = null)
    {
        var stop = new CancellationTokenSource();
        var host = new BackendActionsHost(_nodes[node].Actions, _nodes[node].Publisher, _cluster.Node(node),
            Options(node, lease, worker, generation, workerTimeout ?? TimeSpan.FromSeconds(1)));
        _hosts.Add((stop, host.RunAsync(stop.Token)));
        return stop;
    }

    Task<QualifiedActionId> Enqueue(IActionInput input) => _nodes["a"].Actions.EnqueueAsync(_nodes["a"].Publisher, input);

    static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!predicate()) await Task.Delay(5, timeout.Token);
    }

    int Started => _nodes.Values.Sum(n => n.Recorder.Started.Count);

    void ReleaseAll()
    {
        foreach (var node in _nodes.Values) node.Recorder.Release.TrySetResult();
    }

    // Every replica handles the same outcomes exactly once.
    async Task<Outcome[]> OutcomesAsync(int count)
    {
        await UntilAsync(() => _nodes.Values.All(n => n.Recorder.Outcomes.Count >= count));
        await Task.Delay(100);
        var outcomes = _nodes["a"].Recorder.Outcomes.ToArray();
        Assert.Equal(count, outcomes.Length);
        foreach (var node in _nodes.Values) Assert.Equal(outcomes, node.Recorder.Outcomes);
        return outcomes;
    }

    [Fact]
    public async Task ANewLeaderAdoptsRunningAttemptsInsteadOfRunningThemAgain()
    {
        var lease = TimeSpan.FromMilliseconds(600);
        foreach (var node in _nodes.Keys) Host(node, lease);
        var ids = new List<QualifiedActionId>();
        for (var i = 0; i < 4; i++) ids.Add(await Enqueue(new WaitingOrder { Mode = "wait" }));
        await UntilAsync(() => Started == 4);
        _cluster.Leader = "b";
        await Task.Delay(3 * lease); // past the protection: unadopted attempts would have started again
        Assert.Equal(4, Started);
        ReleaseAll();
        var outcomes = await OutcomesAsync(4);
        Assert.All(outcomes, o => Assert.Equal((ActionOutcomeKind.Completed, 1ul), (o.Kind, o.Attempt)));
        Assert.Equal(ids.Select(i => i.ActionId).Order(), outcomes.Select(o => o.Id.ActionId).Order());
    }

    [Fact]
    public async Task APartitionedWorkersAttemptRunsAgainAndItsLateResultIsIgnored()
    {
        // Longer than the worker timeout: the silent worker is gone before the lease lets anything start again.
        var lease = TimeSpan.FromMilliseconds(1500);
        Host("a", lease, worker: false);
        Host("b", lease);
        var id = await Enqueue(new WaitingOrder { Mode = "wait" });
        await UntilAsync(() => _nodes["b"].Recorder.Started.Count == 1);
        _cluster.Isolate("b", true);
        Host("c", lease);
        await UntilAsync(() => _nodes["c"].Recorder.Started.Count == 1);
        Assert.Equal((id, 2ul), _nodes["c"].Recorder.Started.Single());
        _nodes["b"].Recorder.Release.SetResult(); // attempt 1 finishes first, but it was superseded
        await UntilAsync(() => _nodes["b"].Recorder.Running == 0);
        await Task.Delay(100);
        Assert.Empty(_nodes["a"].Recorder.Outcomes);
        _nodes["c"].Recorder.Release.SetResult();
        var outcome = Assert.Single(await OutcomesAsync(1));
        Assert.Equal((id, ActionOutcomeKind.Completed, 2ul, "done 2"), (outcome.Id, outcome.Kind, outcome.Attempt, outcome.Result));
        _cluster.Isolate("b", false);
    }

    [Fact]
    public async Task ARestartedNodeEndsItsAttemptAtOnceUnderANewCoordinator()
    {
        var lease = TimeSpan.FromSeconds(30);
        Host("a", lease, worker: false);
        var b = Host("b", lease);
        var id = await Enqueue(new WaitingOrder { Mode = "wait" });
        await UntilAsync(() => _nodes["b"].Recorder.Started.Count == 1);
        b.Cancel(); // the process stops without an outcome
        await _hosts[1].Run.WaitAsync(TimeSpan.FromSeconds(10));
        _cluster.Leader = null; // a new coordinator session loads the started row
        _cluster.Leader = "a";
        _nodes["b"].Recorder.Release.SetResult();
        Host("b", lease); // the restarted process on node b
        var outcome = Assert.Single(await OutcomesAsync(1));
        Assert.Equal((id, ActionOutcomeKind.Completed, 2ul), (outcome.Id, outcome.Kind, outcome.Attempt));
    }

    [Fact]
    public async Task AWorkerOfAnOlderGenerationStartsNoNewActions()
    {
        var lease = TimeSpan.FromSeconds(30);
        Host("a", lease, worker: false, generation: 2);
        Host("b", lease, generation: 1); // not upgraded yet
        Host("c", lease, generation: 2);
        ReleaseAll();
        for (var i = 0; i < 3; i++) await Enqueue(new WaitingOrder { Mode = "fast" });
        await OutcomesAsync(3);
        Assert.Empty(_nodes["b"].Recorder.Started);
        Assert.Equal(3, _nodes["c"].Recorder.Started.Count);
    }

    [Fact]
    public async Task AStoppingWorkerHandsItsAttemptsBackAtOnce()
    {
        // Without the stopping worker's last exchange its attempt would wait for the lease or the worker timeout.
        var lease = TimeSpan.FromSeconds(30);
        Host("a", lease, worker: false, workerTimeout: lease);
        var b = Host("b", lease, workerTimeout: lease);
        var id = await Enqueue(new WaitingOrder { Mode = "wait" });
        await UntilAsync(() => _nodes["b"].Recorder.Started.Count == 1);
        Host("c", lease, workerTimeout: lease);
        b.Cancel();
        await _hosts[1].Run.WaitAsync(TimeSpan.FromSeconds(10));
        await UntilAsync(() => _nodes["c"].Recorder.Started.Count == 1);
        Assert.Equal((id, 2ul), _nodes["c"].Recorder.Started.Single());
        _nodes["c"].Recorder.Release.SetResult();
        var outcome = Assert.Single(await OutcomesAsync(1));
        Assert.Equal((ActionOutcomeKind.Completed, 2ul), (outcome.Kind, outcome.Attempt));
    }
}
