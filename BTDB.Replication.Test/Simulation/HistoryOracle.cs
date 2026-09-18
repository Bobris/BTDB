using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;

namespace BTDB.Replication.Test.Simulation;

// Synthetic observations used to test the oracle itself. These are NOT a TRL codec, persistent sidecar,
// leader record proposal or a second publication document. M1/M2 will supply observations from native files.
internal sealed record ModelAuthority(ulong Term, string Session);
internal sealed record ModelRange(string Key, int Offset, int Length, string Sha256);
internal sealed record ModelTransaction(ulong Sequence, ulong EventId, bool Application, bool Complete, ModelRange[] Ranges);
internal sealed record ModelHistory(ulong GenesisCursor, ulong PublishedCursor, ModelTransaction[] Transactions);

internal sealed class HistoryOracle(SimulatedBlobStore storage)
{
    public const string LeaderKey = "model/leader";
    public const string HistoryPrefix = "model/trl/";
    public static byte[] Encode<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value);
    public static string Hash(ReadOnlySpan<byte> bytes) => Convert.ToHexString(SHA256.HashData(bytes));

    static T Decode<T>(byte[] bytes) => JsonSerializer.Deserialize<T>(bytes)
        ?? throw new InvalidOperationException("I5 missing model observation.");

    public void Check()
    {
        // Replay observable requests/effects, not node role flags or publisher watermarks. A request dispatched
        // under the old selected term can still land after selection changes; M1 adds per-TRL adoption fencing.
        var objects = new Dictionary<string, BlobSnapshot>();
        ModelAuthority? authority = null;
        foreach (var observation in storage.Journal)
        {
            var request = observation.Request;
            if (observation.IsDispatch)
            {
                if (request.Key.StartsWith(HistoryPrefix, StringComparison.Ordinal) &&
                    (authority is null || request.Actor != authority.Session))
                    throw new InvalidOperationException("I1 canonical dispatch without selected authority.");
                continue;
            }
            if (!observation.Applied) continue;
            if (request.Key == LeaderKey && observation.Result is { } selected)
            {
                var next = Decode<ModelAuthority>(selected.Content);
                if (authority is not null && next.Term <= authority.Term)
                    throw new InvalidOperationException("I1 leadership term did not increase.");
                authority = next;
            }
            if (request.Key.StartsWith(HistoryPrefix, StringComparison.Ordinal))
            {
                if (observation.Result is null)
                    throw new InvalidOperationException("I5 selected history was deleted.");
                if (objects.TryGetValue(request.Key, out var prior))
                {
                    var before = Decode<ModelHistory>(prior.Content);
                    var after = Decode<ModelHistory>(observation.Result.Content);
                    if (before.GenesisCursor != after.GenesisCursor ||
                        after.Transactions.Length < before.Transactions.Length ||
                        !before.Transactions.Select(Encode).Zip(after.Transactions.Select(Encode),
                            (left, right) => left.AsSpan().SequenceEqual(right)).All(equal => equal))
                        throw new InvalidOperationException("I5 published transaction prefix changed.");
                }
            }
            if (observation.Result is { } result) objects[request.Key] = result;
            else objects.Remove(request.Key);

            // Check each intermediate remote state, including an effect whose response was lost.
            foreach (var (key, blob) in objects)
                if (key.StartsWith(HistoryPrefix, StringComparison.Ordinal))
                    CheckHistory(Decode<ModelHistory>(blob.Content), objects);
        }
    }

    static void CheckHistory(ModelHistory history, Dictionary<string, BlobSnapshot> objects)
    {
        var cursor = history.GenesisCursor;
        ulong sequence = 0;
        foreach (var transaction in history.Transactions)
        {
            if (!transaction.Complete || transaction.Sequence != ++sequence)
                throw new InvalidOperationException("I2/I5 incomplete transaction or canonical sequence gap.");
            if (transaction.Application ? transaction.EventId <= cursor : transaction.EventId != cursor)
                throw new InvalidOperationException("I2 invalid application/schema cursor.");
            cursor = transaction.EventId;
            if (transaction.Ranges.Length == 0)
                throw new InvalidOperationException("I5 transaction has no recovery bytes.");
            foreach (var range in transaction.Ranges)
            {
                if (!objects.TryGetValue(range.Key, out var file) || range.Offset < 0 || range.Length <= 0 ||
                    (long)range.Offset + range.Length > file.Content.Length ||
                    Hash(file.Content.AsSpan(range.Offset, range.Length)) != range.Sha256)
                    throw new InvalidOperationException("I5 missing or corrupt recovery range.");
            }
        }
        if (history.PublishedCursor != cursor)
            throw new InvalidOperationException("I2/I5 claimed cursor exceeds complete history.");
    }
}
