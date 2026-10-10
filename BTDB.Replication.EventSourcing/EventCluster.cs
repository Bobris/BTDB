using System.Text.Json;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.EventSourcing;

public sealed record EventClusterOptions
{
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan NodeTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
    public Func<Guid> NewSessionId { get; init; } = Guid.NewGuid;
}

/// <summary>Sampled membership of consumers that finished startup replay. Not a leadership or fencing grant.</summary>
public sealed record EventClusterSnapshot(string SelfNodeId, string? LeaderNodeId,
    IReadOnlyList<string> LiveNodeIds, DateTimeOffset SampledAt);

/// <summary>Shared-storage heartbeats, independent of database writes and the two Layer2 topics.
/// Use one instance per publisher, a shared membership storage prefix and unique running node IDs.
/// UTC clocks must be synchronized within the application's membership timeout budget.</summary>
public sealed class EventCluster
{
    sealed record Member(string NodeId, Guid SessionId, DateTimeOffset ExpiresAt);
    sealed record View(string? Leader, Member[] Members, DateTimeOffset SampledAt);

    readonly IEventLogStorage _storage;
    readonly IReplicationLeaderStorage _leaders;
    readonly string _clusterId, _prefix, _key;
    readonly EventClusterOptions _options;
    readonly Guid _session;
    readonly SemaphoreSlim _refreshing = new(1);
    View _view = new(null, [], DateTimeOffset.MinValue);
    int _attached;
    bool _registered;

    public EventCluster(string selfNodeId, string clusterId, IEventLogStorage membershipStorage,
        IReplicationLeaderStorage leaderStorage, EventClusterOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(selfNodeId);
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        SelfNodeId = selfNodeId;
        _clusterId = clusterId;
        _storage = membershipStorage ?? throw new ArgumentNullException(nameof(membershipStorage));
        _leaders = leaderStorage ?? throw new ArgumentNullException(nameof(leaderStorage));
        _options = options ?? new();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.HeartbeatInterval.Ticks);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.RequestTimeout.Ticks);
        if (_options.NodeTimeout <= _options.HeartbeatInterval)
            throw new ArgumentOutOfRangeException(nameof(options), "Node timeout must exceed the heartbeat interval.");
        ArgumentNullException.ThrowIfNull(_options.TimeProvider);
        ArgumentNullException.ThrowIfNull(_options.NewSessionId);
        _session = _options.NewSessionId();
        if (_session == Guid.Empty) throw new ArgumentException("Session IDs must be nonempty.", nameof(options));
        _prefix = $"event-consumers/{Uri.EscapeDataString(clusterId)}/";
        _key = _prefix + Uri.EscapeDataString(selfNodeId);
    }

    public string SelfNodeId { get; }
    /// <summary>No I/O; expired members are removed even between heartbeat refreshes. Leader is last observed,
    /// nullable before selection; it can name a node still activating or no longer reachable.</summary>
    public EventClusterSnapshot Current
    {
        get
        {
            var view = Volatile.Read(ref _view);
            var now = _options.TimeProvider.GetUtcNow();
            return new(SelfNodeId, view.Leader,
                Array.AsReadOnly(view.Members.Where(m => m.ExpiresAt > now).Select(m => m.NodeId).ToArray()), view.SampledAt);
        }
    }

    /// <summary>Refresh from shared storage. The result is a sampled view, not an atomic cluster census.</summary>
    public async ValueTask<EventClusterSnapshot> RefreshAsync(CancellationToken cancellation = default)
    {
        using var timeout = new CancellationTokenSource(_options.RequestTimeout, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timeout.Token);
        await _refreshing.WaitAsync(linked.Token).ConfigureAwait(false);
        try
        {
            var leader = await _leaders.ReadAsync(linked.Token).ConfigureAwait(false);
            var json = LeaderJson.Parse(leader.Json);
            string? leaderId = null;
            if (json.Count > 0)
            {
                if (LeaderJson.OptionalString(json, "clusterId") != _clusterId || LeaderJson.OptionalInt32(json, "format") != 1)
                    throw new InvalidDataException("Leader document belongs to another cluster or format.");
                leaderId = LeaderJson.OptionalString(json, "nodeId");
            }
            var members = new List<Member>();
            await foreach (var info in _storage.ListAsync(_prefix, linked.Token).ConfigureAwait(false))
            {
                var blob = await _storage.ReadAsync(info.Key, linked.Token).ConfigureAwait(false);
                if (blob == null) continue;
                var member = Decode(blob);
                if (info.Key != _prefix + Uri.EscapeDataString(member.NodeId))
                    throw new InvalidDataException("Membership key does not match its node ID.");
                members.Add(member);
            }
            Volatile.Write(ref _view, new(leaderId, members.OrderBy(m => m.NodeId, StringComparer.Ordinal).ToArray(),
                _options.TimeProvider.GetUtcNow()));
            return Current;
        }
        finally { _refreshing.Release(); }
    }

    internal void Attach()
    {
        if (Interlocked.Exchange(ref _attached, 1) != 0)
            throw new InvalidOperationException("Use a separate EventCluster for each publisher session.");
    }

    internal async Task BeatAsync(CancellationToken cancellation)
    {
        await HeartbeatAsync(cancellation).ConfigureAwait(false);
        await RefreshAsync(cancellation).ConfigureAwait(false);
    }

    internal async Task RunAsync(CancellationToken cancellation)
    {
        while (true)
        {
            await Task.Delay(_options.HeartbeatInterval, _options.TimeProvider, cancellation).ConfigureAwait(false);
            await BeatAsync(cancellation).ConfigureAwait(false);
        }
    }

    async ValueTask HeartbeatAsync(CancellationToken cancellation)
    {
        using var timeout = new CancellationTokenSource(_options.RequestTimeout, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timeout.Token);
        // A session ID plus conditional version prevents an old stop/renew from overwriting its replacement.
        // The duplicate-ID and replacement-session tests exercise this race.
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new Member(SelfNodeId, _session,
            _options.TimeProvider.GetUtcNow() + _options.NodeTimeout));
        var current = await _storage.ReadAsync(_key, linked.Token).ConfigureAwait(false);
        while (true)
        {
            if (current != null)
            {
                var member = Decode(current);
                if (member.SessionId != _session && member.ExpiresAt > _options.TimeProvider.GetUtcNow())
                    throw new InvalidOperationException($"Node '{SelfNodeId}' already has a live consumer session.");
            }
            var result = await _storage.WriteAsync(_key, current?.Version, bytes, null, null, linked.Token).ConfigureAwait(false);
            if (result.Outcome == EventLogWriteOutcome.Applied) break;
            current = await _storage.ReadAsync(_key, linked.Token).ConfigureAwait(false);
            if (current != null && current.Content.Span.SequenceEqual(bytes)) break;
            linked.Token.ThrowIfCancellationRequested();
        }
        _registered = true;
    }

    internal async Task StopAsync()
    {
        if (!_registered) return;
        using var timeout = new CancellationTokenSource(_options.RequestTimeout, _options.TimeProvider);
        try
        {
            var current = await _storage.ReadAsync(_key, timeout.Token).ConfigureAwait(false);
            if (current == null || Decode(current).SessionId != _session) return;
            var bytes = JsonSerializer.SerializeToUtf8Bytes(new Member(SelfNodeId, _session, DateTimeOffset.MinValue));
            await _storage.WriteAsync(_key, current.Version, bytes, null, null, timeout.Token).ConfigureAwait(false);
        }
        // Best-effort unregistration: a crash, lost response or unavailable storage instead expires by timeout.
        catch (Exception) { }
        finally
        {
            var view = Volatile.Read(ref _view);
            Volatile.Write(ref _view, view with { Members = view.Members.Where(m => m.NodeId != SelfNodeId).ToArray() });
        }
    }

    static Member Decode(EventLogBlob blob)
    {
        var member = JsonSerializer.Deserialize<Member>(blob.Content.Span);
        return member is { SessionId: var session } && session != Guid.Empty && !string.IsNullOrWhiteSpace(member.NodeId)
            ? member : throw new InvalidDataException("Invalid consumer membership record.");
    }
}
