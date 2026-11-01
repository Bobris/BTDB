using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.EventSourcing;

/// <summary>Skips an event whose consumption never completes: a process crash (such as a stack overflow), or a fatal
/// exception that repeats. After every start, the first <see cref="EventPublisherOptions.MaximumBatchSize"/> events
/// record each attempt in <see cref="AttemptStorage"/> under this node's key. An event that already started
/// <see cref="Attempts"/> times without completing is skipped once the active leader recorded that decision in
/// leader.json (<c>applicationData.btdbEventSkips</c>); a follower waits for it. Every publisher reads the decisions
/// once at start, so all nodes skip the same event. After the attempt window, consumption continues only once the
/// window is canonical, so each crash-restart cycle restores further until it reaches the failing event.</summary>
public sealed class EventPoisonOptions
{
    /// <param name="applicationData">This node's leader.json application data.</param>
    /// <param name="status">This node's replication status.</param>
    /// <param name="database">The replicated database name the publisher consumes into.</param>
    /// <param name="attemptStorage">Shared storage with a prefix isolated to this application history.</param>
    /// <param name="nodeId">Unique per running node, such as the host's replication node ID.</param>
    public EventPoisonOptions(ReplicationApplicationData applicationData, ReplicationStatus status, string database,
        IEventLogStorage attemptStorage, string nodeId)
        : this(new ApplicationDataSkipDecisions(applicationData ?? throw new ArgumentNullException(nameof(applicationData))),
            WaitForCanonical(status, database), attemptStorage, nodeId)
    {
    }

    internal EventPoisonOptions(IEventSkipDecisions decisions,
        Func<LeaderTrlProgress, CancellationToken, ValueTask> waitForCanonicalAsync, IEventLogStorage attemptStorage,
        string nodeId)
    {
        Decisions = decisions;
        WaitForCanonicalAsync = waitForCanonicalAsync;
        AttemptStorage = attemptStorage ?? throw new ArgumentNullException(nameof(attemptStorage));
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        NodeId = nodeId;
    }

    internal IEventSkipDecisions Decisions { get; }
    internal Func<LeaderTrlProgress, CancellationToken, ValueTask> WaitForCanonicalAsync { get; }
    public IEventLogStorage AttemptStorage { get; }
    public string NodeId { get; }
    public int Attempts { get; init; } = 2;

    /// <summary>Delay between attempts to record or find a skip decision while this node is not the active leader.</summary>
    public TimeSpan DecisionRetryInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>A node that cannot record a decision stops after this long. If the leader consumed the event, the
    /// restart restores canonical history beyond it; otherwise the node waits again after restart.</summary>
    public TimeSpan DecisionTimeout { get; init; } = TimeSpan.FromMinutes(5);

    static Func<LeaderTrlProgress, CancellationToken, ValueTask> WaitForCanonical(ReplicationStatus status, string database)
    {
        ArgumentNullException.ThrowIfNull(status);
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        return (progress, cancellation) => new(status.WaitForCanonicalAsync(database, progress.Position, cancellation));
    }
}

/// <summary>The event was skipped by a leader.json decision; its consumption changed nothing.</summary>
public sealed class EventSkippedException(ulong eventId, string reason)
    : Exception($"Event {eventId} was skipped: {reason}")
{
    public ulong EventId { get; } = eventId;
    public string Reason { get; } = reason;
}

internal sealed record EventSkipSnapshot(object Version, JsonNode? Value);

/// <summary>Conditional access to leader.json applicationData; writes succeed only on the active leader.</summary>
internal interface IEventSkipDecisions
{
    ValueTask<EventSkipSnapshot> ReadAsync(CancellationToken cancellation);
    ValueTask<LeaderWriteOutcome> TryWriteAsync(EventSkipSnapshot expected, JsonNode? value, CancellationToken cancellation);
}

sealed class ApplicationDataSkipDecisions(ReplicationApplicationData data) : IEventSkipDecisions
{
    public async ValueTask<EventSkipSnapshot> ReadAsync(CancellationToken cancellation)
    {
        var snapshot = await data.ReadAsync(cancellation).ConfigureAwait(false);
        return new(snapshot, snapshot.Value);
    }

    public ValueTask<LeaderWriteOutcome> TryWriteAsync(EventSkipSnapshot expected, JsonNode? value,
        CancellationToken cancellation) =>
        data.TryWriteAsync((ReplicationApplicationDataSnapshot)expected.Version, value, cancellation);
}

/// <summary>Skip decisions in <c>applicationData.btdbEventSkips</c>, an object from event ID to reason. Other
/// application data is preserved. Decisions at or below a canonical event ID are pruned: every restore starts beyond
/// them. A node that read its decisions before such a prune crashes on the event again and then restores beyond it.</summary>
internal static class EventSkips
{
    internal const string Key = "btdbEventSkips";

    internal static Dictionary<ulong, string> Parse(JsonNode? applicationData)
    {
        var skips = new Dictionary<ulong, string>();
        if (applicationData is not JsonObject data || data[Key] is not { } node) return skips;
        if (node is not JsonObject entries) throw new InvalidDataException("Invalid event skip decisions.");
        foreach (var (key, value) in entries)
        {
            if (!ulong.TryParse(key, NumberStyles.None, CultureInfo.InvariantCulture, out var eventId) || eventId == 0 ||
                value is not JsonValue reason || !reason.TryGetValue<string>(out var text))
                throw new InvalidDataException("Invalid event skip decision.");
            skips[eventId] = text;
        }
        return skips;
    }

    /// <summary>The application data with <paramref name="add"/> recorded and decisions at or below
    /// <paramref name="canonicalEventId"/> removed; null when nothing changes.</summary>
    internal static JsonNode? Update(JsonNode? applicationData, (ulong EventId, string Reason)? add, ulong canonicalEventId)
    {
        if (applicationData != null && applicationData is not JsonObject)
            throw new InvalidDataException("leader.json applicationData must be an object to hold event skip decisions.");
        var skips = Parse(applicationData);
        var changed = skips.Keys.Any(id => id <= canonicalEventId);
        if (add is { } decision && !skips.ContainsKey(decision.EventId))
        {
            skips[decision.EventId] = decision.Reason;
            changed = true;
        }
        if (!changed) return null;
        var result = (JsonObject?)applicationData?.DeepClone() ?? new JsonObject();
        var entries = new JsonObject();
        foreach (var (eventId, reason) in skips.OrderBy(s => s.Key))
            if (eventId > canonicalEventId) entries[eventId.ToString(CultureInfo.InvariantCulture)] = reason;
        if (entries.Count == 0) result.Remove(Key);
        else result[Key] = entries;
        return result;
    }

    /// <summary>One conditional write of the change, reconciling an ambiguous outcome by retrying the same intent.
    /// True when the change landed or was not needed; false when rejected (stale version or not the leader).</summary>
    internal static async ValueTask<bool> TryUpdateAsync(IEventSkipDecisions decisions, (ulong EventId, string Reason)? add,
        ulong canonicalEventId, TimeSpan retryInterval, TimeProvider time, CancellationToken cancellation)
    {
        var snapshot = await decisions.ReadAsync(cancellation).ConfigureAwait(false);
        var value = Update(snapshot.Value, add, canonicalEventId);
        if (value == null) return true;
        while (true)
        {
            var outcome = await decisions.TryWriteAsync(snapshot, value, cancellation).ConfigureAwait(false);
            if (outcome != LeaderWriteOutcome.Ambiguous) return outcome == LeaderWriteOutcome.Applied;
            await Task.Delay(retryInterval, time, cancellation).ConfigureAwait(false);
        }
    }
}

/// <summary>This node's last started attempt: the data offset and how many times it started without completing.</summary>
internal sealed class EventAttemptJournal(IEventLogStorage storage, string key, int limit)
{
    internal sealed record Attempt(ulong Offset, int Count);

    Attempt? _last;
    string? _version;

    internal async Task LoadAsync(CancellationToken cancellation)
    {
        var blob = await storage.ReadAsync(key, cancellation).ConfigureAwait(false);
        _version = blob?.Version;
        _last = blob == null ? null : Decode(blob);
    }

    /// <summary>Records the attempt before consumption; true when the event exhausted its attempts instead.</summary>
    internal async ValueTask<bool> BeforeAttemptAsync(ulong offset, CancellationToken cancellation)
    {
        // An earlier offset completed before the recorded crash: keep that crash's count.
        if (_last is { } last && offset < last.Offset) return false;
        if (_last is { } same && offset == same.Offset)
        {
            if (same.Count >= limit) return true;
            await WriteAsync(new(offset, same.Count + 1), cancellation).ConfigureAwait(false);
            return false;
        }
        await WriteAsync(new(offset, 1), cancellation).ConfigureAwait(false);
        return false;
    }

    async Task WriteAsync(Attempt attempt, CancellationToken cancellation)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(attempt, EventAttemptJson.Default.Attempt);
        while (true)
        {
            var result = await storage.WriteAsync(key, _version, bytes, null, null, cancellation).ConfigureAwait(false);
            if (result.Outcome == EventLogWriteOutcome.Applied)
            {
                _version = result.Version;
                break;
            }
            // Rejected or ambiguous: adopt the stored version, and finish if our write landed.
            var current = await storage.ReadAsync(key, cancellation).ConfigureAwait(false);
            _version = current?.Version;
            if (current != null && current.Content.Span.SequenceEqual(bytes)) break;
            cancellation.ThrowIfCancellationRequested();
        }
        _last = attempt;
    }

    static Attempt Decode(EventLogBlob blob) =>
        JsonSerializer.Deserialize(blob.Content.Span, EventAttemptJson.Default.Attempt) is { Count: > 0 } attempt ? attempt
            : throw new InvalidDataException("Invalid event attempt record.");
}

/// <summary>Source-generated, so the journal also works under NativeAOT; the format is the default one.</summary>
[JsonSerializable(typeof(EventAttemptJournal.Attempt))]
internal sealed partial class EventAttemptJson : JsonSerializerContext;
