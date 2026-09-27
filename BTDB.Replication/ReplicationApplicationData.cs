using System;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>A version-bound copy of the application's JSON, without leader credentials. Value may be edited
/// before TryWriteAsync; doing so never mutates the retained compare-and-swap base. Do not mutate it concurrently.</summary>
public sealed class ReplicationApplicationDataSnapshot
{
    internal ReplicationApplicationData Owner { get; }
    internal LeaderRecord Record { get; }
    public string Version => Record.Token;
    public JsonNode? Value { get; }

    internal ReplicationApplicationDataSnapshot(ReplicationApplicationData owner, LeaderRecord record, JsonNode? value)
    {
        Owner = owner;
        Record = record;
        Value = value;
    }

    public override string ToString() => nameof(ReplicationApplicationDataSnapshot);
}

/// <summary>Opaque application state in leader.json. All nodes may read; only the local active leader may write.
/// No event timeout, skip, cleanup or transaction policy is inferred from the content.</summary>
public sealed class ReplicationApplicationData
{
    readonly ILeaderRecordStorage _storage;
    readonly string _clusterId;
    readonly LeaseSessionController _leases;
    readonly Func<SelectedLeadership?> _selected;

    internal ReplicationApplicationData(ILeaderRecordStorage storage, string clusterId, LeaseSessionController leases,
        Func<SelectedLeadership?> selected)
    {
        _storage = storage;
        _clusterId = clusterId;
        _leases = leases;
        _selected = selected;
    }

    public async ValueTask<ReplicationApplicationDataSnapshot> ReadAsync(CancellationToken cancellation = default)
    {
        var record = await _storage.ReadAsync(cancellation).ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        var json = Parse(record);
        return new(this, record, json["applicationData"]?.DeepClone());
    }

    /// <summary>Replace applicationData (null clears it) using the snapshot's exact version and current lease.
    /// Preserves all other fields and advances revision without changing term/session or adopting databases again.
    /// Rejected means this conditional attempt was rejected or this node cannot write. Ambiguous means its effect
    /// is unresolved; an unchanged read does not prove failure. Retrying the same snapshot/value is safe, but never
    /// rebase an unresolved operation onto a fresh snapshot blindly. Cancellation or an exception after dispatch
    /// can also leave an applied write. Applied describes the storage effect, not continued leadership authority.</summary>
    public async ValueTask<LeaderWriteOutcome> TryWriteAsync(ReplicationApplicationDataSnapshot expected,
        JsonNode? value, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(expected);
        if (!ReferenceEquals(expected.Owner, this))
            throw new ArgumentException("Use a snapshot read through this application-data service.", nameof(expected));
        cancellation.ThrowIfCancellationRequested();
        var selected = _selected();
        if (selected == null || !ReferenceEquals(_leases.Current, selected.Authority)) return LeaderWriteOutcome.Rejected;
        var intent = Parse(expected.Record);
        if (LeaderJson.OptionalUInt64(intent, "term") != selected.Term ||
            LeaderJson.OptionalString(intent, "sessionId") != selected.SessionId) return LeaderWriteOutcome.Rejected;
        intent["applicationData"] = value?.DeepClone();
        intent["revision"] = checked(LeaderJson.OptionalUInt64(intent, "revision") + 1);
        var json = intent.ToJsonString();
        string handle;
        // A delayed caller cannot start a write after handoff, fencing, shutdown or session replacement.
        if (!ReferenceEquals(_selected(), selected)) return LeaderWriteOutcome.Rejected;
        try { handle = _leases.GetHandle(selected.Authority); }
        catch (InvalidOperationException) { return LeaderWriteOutcome.Rejected; }
        var outcome = await _storage.WriteAsync(handle, expected.Version, json, cancellation).ConfigureAwait(false);
        if (outcome == LeaderWriteOutcome.Applied) return outcome;
        // Exact intent also reconciles a retry whose first successful response was lost. A changed value alone
        // cannot prove which operation landed; compare the full document, including revision and session.
        try
        {
            var observed = await _storage.ReadAsync(cancellation).ConfigureAwait(false);
            cancellation.ThrowIfCancellationRequested();
            if (JsonNode.DeepEquals(Parse(observed), intent)) return LeaderWriteOutcome.Applied;
        }
        catch (IOException) { }
        return outcome;
    }

    JsonObject Parse(LeaderRecord record)
    {
        var json = LeaderJson.Parse(record.Json);
        if (LeaderJson.OptionalInt32(json, "format") != 1 || LeaderJson.OptionalString(json, "clusterId") != _clusterId)
            throw new InvalidDataException("Leader record belongs to a different cluster or format.");
        return json;
    }
}
