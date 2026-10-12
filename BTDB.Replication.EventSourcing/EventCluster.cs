using System.Text.Json;

namespace BTDB.Replication.EventSourcing;

public sealed record EventClusterOptions
{
    /// <summary>How long registration waits for the leader to confirm the announcement before startup continues
    /// with a settling view.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public Func<Guid> NewSessionId { get; init; } = Guid.NewGuid;
}

/// <summary>Sampled membership of consumers that finished startup replay. Not a leadership or fencing grant. Settling
/// means the view may be incomplete: the replication leader changed recently, or this node has not reached it.</summary>
public sealed record EventClusterSnapshot(string SelfNodeId, string? LeaderNodeId,
    IReadOnlyList<string> LiveNodeIds, bool Settling);

/// <summary>Consumer membership through <see cref="ReplicationPresence"/>: every node announces itself to the selected
/// replication leader, which aggregates the view in memory. Nothing is written to shared storage, and node clocks need
/// not agree. Use one instance per publisher, the node's <see cref="ReplicationPresence"/> and the same node ID as its
/// <c>LeaderCandidate.NodeId</c>.</summary>
public sealed class EventCluster
{
    sealed record Member(string NodeId, Guid SessionId);

    readonly ReplicationPresence _presence;
    readonly EventClusterOptions _options;
    readonly Guid _session;
    string? _announced;
    int _attached;

    public EventCluster(string selfNodeId, ReplicationPresence presence, EventClusterOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(selfNodeId);
        SelfNodeId = selfNodeId;
        _presence = presence ?? throw new ArgumentNullException(nameof(presence));
        _options = options ?? new();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.RequestTimeout.Ticks);
        ArgumentNullException.ThrowIfNull(_options.NewSessionId);
        _session = _options.NewSessionId();
        if (_session == Guid.Empty) throw new ArgumentException("Session IDs must be nonempty.", nameof(options));
    }

    public string SelfNodeId { get; }

    /// <summary>No I/O: the latest view received from the replication leader. LeaderNodeId is the selected replication
    /// leader, nullable before selection; it can name a leader that is still activating or no longer reachable.</summary>
    public EventClusterSnapshot Current => Snapshot(_presence.Current);

    /// <summary>The view after this node's next presence exchange with the leader.</summary>
    public async ValueTask<EventClusterSnapshot> RefreshAsync(CancellationToken cancellation = default) =>
        Snapshot(await _presence.RefreshAsync(cancellation).ConfigureAwait(false));

    EventClusterSnapshot Snapshot(ReplicationPresenceSnapshot presence)
    {
        var live = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var value in presence.Members.Values)
            if (Decode(value) is { } member) live.Add(member.NodeId);
        return new(SelfNodeId, presence.LeaderNodeId, Array.AsReadOnly(live.ToArray()), presence.Settling);
    }

    internal void Attach()
    {
        if (Interlocked.Exchange(ref _attached, 1) != 0)
            throw new InvalidOperationException("Use a separate EventCluster for each publisher session.");
    }

    /// <summary>Announce this consumer once its replay finished. A live consumer of the same node ID announced from
    /// another node fails startup; a settling view may not show it yet.</summary>
    internal async Task RegisterAsync(CancellationToken cancellation)
    {
        var value = JsonSerializer.Serialize(new Member(SelfNodeId, _session));
        if (!_presence.TryClaim(value))
            throw new InvalidOperationException("This node's presence is already announced by another consumer.");
        _announced = value;
        ReplicationPresenceSnapshot view;
        using (var timeout = new CancellationTokenSource(_options.RequestTimeout))
        using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timeout.Token))
        {
            try { view = await _presence.RefreshAsync(linked.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { view = _presence.Current; }
        }
        foreach (var other in view.Members.Values)
            if (Decode(other) is { } member && member.NodeId == SelfNodeId && member.SessionId != _session)
            {
                await StopAsync().ConfigureAwait(false);
                throw new InvalidOperationException($"Node '{SelfNodeId}' already has a live consumer session.");
            }
    }

    /// <summary>Withdraw this consumer and wait (at most RequestTimeout) until the leader received the withdrawal; the
    /// leader drops a crashed node after the presence timeout instead.</summary>
    internal async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _announced, null) is not { } value) return;
        _presence.Withdraw(value);
        if (!_presence.Reachable) return; // nobody to tell; the leader expires the node
        using var timeout = new CancellationTokenSource(_options.RequestTimeout);
        try { await _presence.RefreshAsync(timeout.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) { } // best effort: the leader expires the node
    }

    static Member? Decode(string value)
    {
        try
        {
            return JsonSerializer.Deserialize<Member>(value) is { NodeId: { Length: > 0 }, SessionId: var session } member &&
                   session != Guid.Empty ? member : null;
        }
        catch (JsonException) { return null; }
    }
}
