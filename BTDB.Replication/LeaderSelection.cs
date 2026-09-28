using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>Host-supplied candidate for one acquisition. Create fresh SessionId and ApiKey values on every call.
/// Do not mutate DatabaseNames after returning this record or log its credential property.</summary>
public sealed record LeaderCandidate(string ClusterId, string NodeId, string SessionId,
    ulong ApplicationGeneration, string[] DatabaseNames, string PeerEndpoint, string ApiKey)
{
    public override string ToString() => $"{ClusterId}/{NodeId}/{SessionId} (generation {ApplicationGeneration})";
}

internal sealed record SelectedLeadership(LeaseAuthority Authority, ulong Term, string SessionId, IReadOnlyList<string> DatabaseNames);

/// <summary>One selection attempt under a confirmed lease. Retains the exact JSON across uncertain responses.
/// The supplied session identity and API key must be fresh for this attempt. Unknown fields and opaque application data survive
/// selection; no additional operation document or durable receipt is created.</summary>
internal sealed class LeaderSelection(IReplicationLeaderStorage storage, LeaseSessionController leases,
    LeaseAuthority authority, LeaderCandidate candidate)
{
    LeaderRecord? _previous;
    JsonObject? _intent;
    string[]? _names;
    string? _json;

    public async ValueTask<SelectedLeadership?> SelectAsync(CancellationToken cancellation = default)
    {
        RequireAuthority();
        var observed = await storage.ReadAsync(cancellation).ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        RequireAuthority();
        var current = LeaderJson.Parse(observed.Json);
        if (_intent != null)
        {
            if (JsonNode.DeepEquals(current, _intent)) return Selected();
            if (observed.Token != _previous!.Token) return Conflict();
        }
        else
        {
            if (LeaderJson.OptionalString(current, "clusterId") != candidate.ClusterId ||
                LeaderJson.OptionalInt32(current, "format") != 1)
                throw new InvalidDataException("Leader record belongs to a different cluster or format.");
            var generation = LeaderJson.OptionalUInt64(current, "applicationGeneration");
            if (generation > candidate.ApplicationGeneration) return Conflict();
            // A set, as in follower discovery: nodes of one generation may list the same names in another order.
            var selected = DatabaseNames();
            if (generation == candidate.ApplicationGeneration &&
                LeaderJson.OptionalNames(current, "databaseNames") is { } names &&
                !names.ToHashSet(StringComparer.Ordinal).SetEquals(selected))
                throw new InvalidDataException("The same application generation must select the same database set.");
            if (LeaderJson.OptionalString(current, "sessionId") == candidate.SessionId)
                throw new InvalidDataException("A fresh lease requires a fresh leadership session identity.");
            _intent = current.DeepClone().AsObject();
            _intent["term"] = checked(LeaderJson.OptionalUInt64(current, "term") + 1);
            _intent["revision"] = checked(LeaderJson.OptionalUInt64(current, "revision") + 1);
            _intent["nodeId"] = candidate.NodeId;
            _intent["sessionId"] = candidate.SessionId;
            _intent["applicationGeneration"] = candidate.ApplicationGeneration;
            _intent["databaseNames"] = new JsonArray([.. selected.Select(name => JsonValue.Create(name))]);
            _intent["peerEndpoint"] = candidate.PeerEndpoint;
            _intent["apiKey"] = candidate.ApiKey;
            _names = selected;
            _previous = observed;
            _json = _intent.ToJsonString();
        }
        var result = await storage.WriteAsync(leases.GetHandle(authority), _previous!.Token, _json!, cancellation)
            .ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        RequireAuthority();
        if (result == LeaderWriteOutcome.Applied) return Selected();
        // Even a rejection can follow a lost successful response. Only exact content proves this selection.
        observed = await storage.ReadAsync(cancellation).ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        RequireAuthority();
        if (JsonNode.DeepEquals(LeaderJson.Parse(observed.Json), _intent)) return Selected();
        if (observed.Token != _previous.Token || result == LeaderWriteOutcome.Rejected) return Conflict();
        return null;
    }

    string[] DatabaseNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in candidate.DatabaseNames)
            if (string.IsNullOrEmpty(name) || !names.Add(name))
                throw new ArgumentException("Database names must be nonempty and unique.");
        return [.. candidate.DatabaseNames];
    }

    SelectedLeadership Selected() => new(authority, _intent!["term"]!.GetValue<ulong>(), candidate.SessionId, _names!);
    SelectedLeadership? Conflict() { authority.Fence(); return null; }
    void RequireAuthority()
    {
        if (!authority.IsValid) throw new InvalidOperationException("Selection requires live lease authority.");
    }
}
