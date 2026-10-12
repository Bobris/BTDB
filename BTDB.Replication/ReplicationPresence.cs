using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>
/// The selected replication leader's view of node presence: one value per node endpoint. Members lists every node
/// that announced a value, including the leader itself. Settling is true while the view may be incomplete or stale: the
/// leader session started less than one presence timeout ago, so members may not have announced themselves to it yet,
/// or this node has not exchanged presence with a leader within that timeout.
/// </summary>
public sealed record ReplicationPresenceSnapshot(string? LeaderNodeId, bool Settling,
    IReadOnlyDictionary<string, string> Members);

/// <summary>A follower's announcement; KnownVersion is the view version it already holds from this leader session.</summary>
internal sealed record ReplicationPresenceRequest(string Endpoint, string? Value, ulong KnownVersion);

/// <summary>The leader's answer: Members is null when the requester already holds this version.</summary>
internal sealed record ReplicationPresenceView(ulong Version, bool Settling, IReadOnlyDictionary<string, string>? Members);

/// <summary>
/// Node presence without shared storage. Followers announce their value to the selected replication leader at
/// <see cref="ReplicationNodeOptions.PresenceInterval"/> (and promptly after <see cref="Announce"/>), and the leader
/// answers with its aggregated view. The leader keeps the view in memory and expires a node that stops announcing after
/// the presence timeout, measured on its own monotonic clock, so node clocks need not agree. Lease fencing makes at
/// most one leader serve presence; a new leader starts empty and reports <see cref="ReplicationPresenceSnapshot.Settling"/>
/// until every live node had time to announce itself. Presence is discovery, not fencing.
/// </summary>
public sealed class ReplicationPresence
{
    /// <summary>Longest announced value, in characters.</summary>
    public const int MaximumValueLength = 4096;

    static readonly IReadOnlyDictionary<string, string> NoMembers = new Dictionary<string, string>();

    readonly object _lock = new();
    IReplicationScheduler? _scheduler;
    TimeSpan _timeout;
    string? _value;
    bool _dirty;
    Board? _board;
    string? _leaderNodeId;
    ulong _knownVersion;
    IReadOnlyDictionary<string, string> _members = NoMembers;
    bool _viewSettling = true;
    TimeSpan? _receivedAt;
    ulong _taken, _answered;
    TaskCompletionSource _exchanged = NewSignal();

    static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Set (or with null withdraw) this node's value. Followers send it with their next replication step.</summary>
    public void Announce(string? value)
    {
        if (value is { Length: > MaximumValueLength }) throw new ArgumentOutOfRangeException(nameof(value));
        lock (_lock)
        {
            if (_value == value) return;
            _value = value;
            _dirty = true;
            _board?.SelfChanged();
        }
    }

    /// <summary>Announce only when this node has no value yet.</summary>
    internal bool TryClaim(string value)
    {
        lock (_lock)
        {
            if (_value != null) return false;
            Announce(value);
            return true;
        }
    }

    /// <summary>Withdraw only this exact value, never a replacement announced since.</summary>
    internal void Withdraw(string value)
    {
        lock (_lock)
            if (_value == value) Announce(null);
    }

    /// <summary>This node leads, or exchanged with a leader within the presence timeout.</summary>
    internal bool Reachable
    {
        get
        {
            lock (_lock)
                return _board != null || _scheduler is { } scheduler && _receivedAt is { } received &&
                    scheduler.Elapsed - received <= _timeout;
        }
    }

    /// <summary>No I/O: the latest view this node holds.</summary>
    public ReplicationPresenceSnapshot Current
    {
        get
        {
            lock (_lock)
            {
                if (_scheduler is not { } scheduler) return new(null, true, NoMembers);
                var now = scheduler.Elapsed;
                if (_board is { } board) return new(board.NodeId, board.Settling(now), board.Members(now));
                var stale = _receivedAt is not { } received || now - received > _timeout;
                return new(_leaderNodeId, _viewSettling || stale, _members);
            }
        }
    }

    /// <summary>The view after an exchange with the leader that started after this call, so it carries every earlier
    /// <see cref="Announce"/>; at once on the leader itself. While no leader answers, this waits until cancellation.</summary>
    public async Task<ReplicationPresenceSnapshot> RefreshAsync(CancellationToken cancellation = default)
    {
        ulong target;
        lock (_lock)
        {
            _dirty = true;
            target = _taken + 1;
        }
        while (true)
        {
            Task exchanged;
            lock (_lock)
            {
                if (_board != null || _answered >= target) break;
                exchanged = _exchanged.Task;
            }
            await exchanged.WaitAsync(cancellation).ConfigureAwait(false);
        }
        return Current;
    }

    internal void Bind(IReplicationScheduler scheduler, TimeSpan timeout)
    {
        lock (_lock)
        {
            if (_scheduler != null && !ReferenceEquals(_scheduler, scheduler))
                throw new InvalidOperationException("Presence belongs to another replication node.");
            _scheduler = scheduler;
            _timeout = timeout;
        }
    }

    /// <summary>Whether a follower should exchange now: its value changed, a refresh waits, or the interval passed.</summary>
    internal bool TakeDue(TimeSpan interval, ref TimeSpan next)
    {
        lock (_lock)
        {
            var now = _scheduler!.Elapsed;
            if (!_dirty && now < next) return false;
            _dirty = false;
            next = now + interval;
            return true;
        }
    }

    /// <summary>The announcement to send and its sequence number, which <see cref="Received"/> reports back.</summary>
    internal (ReplicationPresenceRequest Request, ulong Sequence) Request(string endpoint)
    {
        lock (_lock) return (new(endpoint, _value, _knownVersion), ++_taken);
    }

    /// <summary>A follower connected to a leader session: its view versions start over.</summary>
    internal void Connected(string? leaderNodeId)
    {
        lock (_lock)
        {
            _leaderNodeId = leaderNodeId;
            _knownVersion = 0;
            _dirty = true;
        }
    }

    internal void Received(ReplicationPresenceView view, ulong sequence)
    {
        TaskCompletionSource exchanged;
        lock (_lock)
        {
            if (_board != null) return;
            _answered = Math.Max(_answered, sequence);
            if (view.Members is { } members)
            {
                _members = members;
                _knownVersion = view.Version;
            }
            else if (view.Version != _knownVersion) throw new IOException("Presence view version is unknown.");
            _viewSettling = view.Settling;
            _receivedAt = _scheduler!.Elapsed;
            exchanged = _exchanged;
            _exchanged = NewSignal();
        }
        exchanged.TrySetResult();
    }

    /// <summary>This node serves a leader session; the returned board aggregates its followers' announcements.</summary>
    internal Board StartLeading(string endpoint, string? nodeId)
    {
        TaskCompletionSource exchanged;
        Board board;
        lock (_lock)
        {
            board = new(this, endpoint, nodeId, _scheduler!.Elapsed);
            _board = board;
            _leaderNodeId = nodeId;
            exchanged = _exchanged;
            _exchanged = NewSignal();
        }
        exchanged.TrySetResult();
        return board;
    }

    internal void StopLeading(Board board)
    {
        lock (_lock)
        {
            if (_board != board) return;
            _board = null;
            board.Closed = true;
            _members = NoMembers;
            _receivedAt = null;
            _knownVersion = 0;
        }
    }

    internal sealed class Board(ReplicationPresence owner, string endpoint, string? nodeId, TimeSpan started)
    {
        readonly Dictionary<string, (string Value, TimeSpan Seen)> _entries = new(StringComparer.Ordinal);
        ulong _version = 1;
        IReadOnlyDictionary<string, string>? _members;
        internal bool Closed;

        public string? NodeId => nodeId;

        // Callers hold owner._lock.
        internal bool Settling(TimeSpan now) => now - started < owner._timeout;

        internal void SelfChanged()
        {
            _version++;
            _members = null;
        }

        internal IReadOnlyDictionary<string, string> Members(TimeSpan now)
        {
            Expire(now);
            if (_members != null) return _members;
            var members = new SortedDictionary<string, string>(StringComparer.Ordinal);
            foreach (var (key, entry) in _entries) members[key] = entry.Value;
            if (owner._value is { } self) members[endpoint] = self;
            return _members = members.AsReadOnly();
        }

        void Expire(TimeSpan now)
        {
            List<string>? expired = null;
            foreach (var (key, entry) in _entries)
                if (now - entry.Seen > owner._timeout) (expired ??= []).Add(key);
            if (expired == null) return;
            foreach (var key in expired) _entries.Remove(key);
            SelfChanged();
        }

        public ReplicationPresenceView Exchange(ReplicationPresenceRequest request)
        {
            if (string.IsNullOrEmpty(request.Endpoint) || request.Endpoint == endpoint ||
                request.Value is { Length: > MaximumValueLength })
                throw new ArgumentException("Invalid presence announcement.");
            lock (owner._lock)
            {
                if (Closed) throw new IOException("Leader session no longer serves presence.");
                var now = owner._scheduler!.Elapsed;
                if (request.Value is { } value)
                {
                    var changed = !_entries.TryGetValue(request.Endpoint, out var previous) || previous.Value != value;
                    _entries[request.Endpoint] = (value, now);
                    if (changed) SelfChanged();
                }
                else if (_entries.Remove(request.Endpoint)) SelfChanged();
                var members = Members(now);
                return new(_version, Settling(now), request.KnownVersion == _version ? null : members);
            }
        }
    }
}
