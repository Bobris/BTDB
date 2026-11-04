using BTDB.Replication;
using BTDB.Replication.EventSourcing;

namespace BTDB.BackendActions;

/// <summary>
/// Runs backend actions on one node: its worker and, while the node leads, the coordinator. Create it after the
/// publisher started, with the node's <see cref="ReplicationLeadership"/> and <see cref="ReplicationLeaderChannel"/>,
/// and observe <see cref="RunAsync"/> until shutdown; join it before disposing the publisher. The node ID and the
/// application generation come from the publisher's cluster and the replication node unless the options set them.
/// </summary>
public sealed class BackendActionsHost
{
    readonly BackendActions _actions;
    readonly EventPublisher _publisher;
    readonly IBackendActionsCluster _cluster;
    readonly BackendActionsOptions _options;
    ActionCoordinator? _coordinator;

    public BackendActionsHost(BackendActions actions, EventPublisher publisher, ReplicationLeadership leadership,
        ReplicationLeaderChannel channel, BackendActionsOptions options)
        : this(actions, publisher, new ReplicationCluster(leadership, channel), options)
    {
    }

    internal BackendActionsHost(BackendActions actions, EventPublisher publisher, IBackendActionsCluster cluster,
        BackendActionsOptions options)
    {
        _actions = actions ?? throw new ArgumentNullException(nameof(actions));
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _cluster = cluster;
        _options = options ?? throw new ArgumentNullException(nameof(options));
        NodeId = Resolve(options.NodeId, publisher.SelfNodeId, nameof(options.NodeId),
            "configure EventPublisherOptions.Cluster");
        ArgumentException.ThrowIfNullOrWhiteSpace(NodeId, nameof(options.NodeId));
        _generation = Resolve(options.ApplicationGeneration, cluster.ApplicationGeneration,
            nameof(options.ApplicationGeneration), "construct the host after the replication node started");
        ArgumentOutOfRangeException.ThrowIfNegative(options.WorkerCpu);
        if (options.PollTimeout <= TimeSpan.Zero || options.PollTimeout >= options.Planner.WorkerTimeout)
            throw new ArgumentException("The poll timeout must be positive and below the worker timeout.");
        _metrics = new(() => Coordinator?.Overview());
        Worker = new(actions, publisher, cluster, options, NodeId, _generation, _metrics);
    }

    // A value configured twice drifts: one set in the options must agree with the replication node's.
    static string Resolve(string? configured, string? fromNode, string name, string remedy)
    {
        if (configured is null) return fromNode ?? throw Missing(name, remedy);
        if (fromNode is not null && configured != fromNode) throw Different(name, configured, fromNode);
        return configured;
    }

    static ulong Resolve(ulong? configured, ulong? fromNode, string name, string remedy)
    {
        if (configured is not { } value) return fromNode ?? throw Missing(name, remedy);
        if (fromNode is { } node && value != node) throw Different(name, value, node);
        return value;
    }

    static ArgumentException Missing(string name, string remedy) =>
        new($"Set BackendActionsOptions.{name} or {remedy}.");

    static ArgumentException Different(string name, object configured, object fromNode) =>
        new($"BackendActionsOptions.{name} is {configured}, but the replication node's is {fromNode}.");

    readonly ulong _generation;

    /// <summary>This node's ID, as its worker reports it.</summary>
    public string NodeId { get; }

    readonly BackendActionsMetrics _metrics;
    internal BackendActionsMetrics Metrics => _metrics;

    internal ActionWorker Worker { get; }
    internal ActionCoordinator? Coordinator => Volatile.Read(ref _coordinator);

    /// <summary>Serves the coordinator's exchange, runs the worker and coordinates during every leader session until
    /// <paramref name="stopping"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken stopping)
    {
        using var metrics = _metrics;
        using var serving = _cluster.Serve(ExchangeJson.Channel, (request, cancellation) =>
            Coordinator is { } coordinator ? coordinator.ExchangeAsync(request, cancellation)
                : throw new IOException("This node does not coordinate backend actions."));
        using var overview = _cluster.Serve(ExchangeJson.OverviewChannel, (_, _) =>
            ValueTask.FromResult<ReadOnlyMemory<byte>>(Coordinator is { } coordinator
                ? ExchangeJson.Encode(coordinator.Overview())
                : throw new IOException("This node does not coordinate backend actions.")));
        var worker = _options.RunWorker ? Worker.RunAsync(stopping) : Task.CompletedTask;
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                try
                {
                    await _cluster.RunWhileLeaderAsync(CoordinateAsync, stopping).ConfigureAwait(false);
                }
                catch (Exception) when (!stopping.IsCancellationRequested)
                {
                    // A failed session ends only its coordinator; the next session starts a new one.
                    await Task.Delay(_options.RetryDelay, _options.TimeProvider, stopping).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
        finally
        {
            try { await worker.ConfigureAwait(false); }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
        }
    }

    /// <summary>The active coordinator's workers and queue, from any node; IOException while no coordinator runs or
    /// the leader is unreachable.</summary>
    public async Task<ActionsOverview> GetOverviewAsync(CancellationToken cancellation = default)
    {
        var reply = await _cluster.SendAsync(ExchangeJson.OverviewChannel, ReadOnlyMemory<byte>.Empty, cancellation)
            .ConfigureAwait(false);
        try { return ExchangeJson.DecodeOverview(reply); }
        catch (System.Text.Json.JsonException error) { throw new IOException("Invalid backend action overview.", error); }
    }

    async Task CoordinateAsync(CancellationToken leader)
    {
        var coordinator = new ActionCoordinator(_actions, _publisher, _options, _generation, _metrics);
        Volatile.Write(ref _coordinator, coordinator);
        try
        {
            await coordinator.RunAsync(leader).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.CompareExchange(ref _coordinator, null, coordinator);
        }
    }
}
