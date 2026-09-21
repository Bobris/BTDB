using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

internal sealed record LeaderRecord(string Token, string Json);
internal enum LeaderWriteOutcome { Applied, Rejected, Ambiguous }

internal interface ILeaderRecordStorage
{
    ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation);
    ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string expectedToken, string json,
        CancellationToken cancellation);
}

internal sealed record LeaderCandidate(string ClusterId, string NodeId, string SessionId,
    ulong ApplicationGeneration, string[] DatabaseNames, string PeerEndpoint, string ApiKey);

internal sealed record SelectedLeadership(LeaseAuthority Authority, ulong Term, string SessionId, IReadOnlyList<string> DatabaseNames);

/// <summary>One selection attempt under a confirmed lease. Retains the exact JSON across uncertain responses.
/// The supplied session identity and API key must be fresh for this attempt. Unknown fields and skip entries survive
/// selection; no additional operation document or durable receipt is created.</summary>
internal sealed class LeaderSelection(ILeaderRecordStorage storage, LeaseSessionController leases,
    LeaseAuthority authority, LeaderCandidate candidate)
{
    LeaderRecord? _previous;
    JsonObject? _intent;
    string? _json;

    public async ValueTask<SelectedLeadership?> SelectAsync(CancellationToken cancellation = default)
    {
        RequireAuthority();
        var observed = await storage.ReadAsync(cancellation).ConfigureAwait(false);
        cancellation.ThrowIfCancellationRequested();
        RequireAuthority();
        var current = JsonNode.Parse(observed.Json)?.AsObject() ?? throw new InvalidDataException("Missing leader JSON.");
        if (_intent != null)
        {
            if (JsonNode.DeepEquals(current, _intent)) return Selected();
            if (observed.Token != _previous!.Token) return Conflict();
        }
        else
        {
            if (current["clusterId"]?.GetValue<string>() != candidate.ClusterId ||
                current["format"]?.GetValue<int>() != 1)
                throw new InvalidDataException("Leader record belongs to a different cluster or format.");
            var generation = current["applicationGeneration"]?.GetValue<ulong>() ?? 0;
            if (generation > candidate.ApplicationGeneration) return Conflict();
            if (generation == candidate.ApplicationGeneration && current["databaseNames"] is { } names &&
                !JsonNode.DeepEquals(names, DatabaseNames()))
                throw new InvalidDataException("The same application generation must select the same database set.");
            if (current["sessionId"]?.GetValue<string>() == candidate.SessionId)
                throw new InvalidDataException("A fresh lease requires a fresh leadership session identity.");
            _intent = current.DeepClone().AsObject();
            _intent["term"] = checked((current["term"]?.GetValue<ulong>() ?? 0) + 1);
            _intent["revision"] = checked((current["revision"]?.GetValue<ulong>() ?? 0) + 1);
            _intent["nodeId"] = candidate.NodeId;
            _intent["sessionId"] = candidate.SessionId;
            _intent["applicationGeneration"] = candidate.ApplicationGeneration;
            _intent["databaseNames"] = DatabaseNames();
            _intent["peerEndpoint"] = candidate.PeerEndpoint;
            _intent["apiKey"] = candidate.ApiKey;
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
        if (JsonNode.DeepEquals(JsonNode.Parse(observed.Json), _intent)) return Selected();
        if (observed.Token != _previous.Token || result == LeaderWriteOutcome.Rejected) return Conflict();
        return null;
    }

    JsonArray DatabaseNames()
    {
        var result = new JsonArray();
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in candidate.DatabaseNames)
        {
            if (string.IsNullOrEmpty(name) || !names.Add(name))
                throw new ArgumentException("Database names must be nonempty and unique.");
            result.Add(name);
        }
        return result;
    }

    SelectedLeadership Selected() => new(authority, _intent!["term"]!.GetValue<ulong>(), candidate.SessionId,
        _intent["databaseNames"]!.AsArray().Select(n => n!.GetValue<string>()).ToArray());
    SelectedLeadership? Conflict() { authority.Fence(); return null; }
    void RequireAuthority()
    {
        if (!authority.IsValid) throw new InvalidOperationException("Selection requires live lease authority.");
    }
}
