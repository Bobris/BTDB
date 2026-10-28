using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.Http.Test;

/// Valid peer messages with random bytes changed, inserted or removed, or cut short, decode or fail with
/// InvalidDataException, as both wire formats promise for malformed input; nothing else escapes, and decoding
/// allocates for the message, not for lengths it declares.
public class PeerWireFuzzTest
{
    static PeerRequest Base(PeerOperation operation) => new("cluster", 7, "session", "https://node", operation);

    static readonly ReplicationPeerPollRequest[] Requested = [new("a", new(3, 10)), new("b")];

    static IEnumerable<(string Name, byte[] Bytes, Action<byte[]> Decode)> Messages()
    {
        foreach (var request in new[]
                 {
                     Base(PeerOperation.Connect),
                     Base(PeerOperation.Poll) with
                     {
                         Challenge = -3, DurationTicks = 100, InlineBudget = 1024, Databases = [new("a", new(5, 77)), new("b")]
                     },
                     Base(PeerOperation.Read) with { Database = "a", FileId = 3, Offset = 1ul << 40, Count = 4096 },
                     Base(PeerOperation.Handoff) with { Handoff = new(9, "10000000-0000-0000-0000-000000000009") },
                     Base(PeerOperation.Presence) with { Presence = new("https://follower", "value", 12) }
                 })
            yield return ($"request {request.Operation}", ReplicationPeerWire.EncodeRequest(request),
                bytes => ReplicationPeerWire.DecodeRequest(bytes));
        var poll = new ReplicationPeerPoll(5, true,
        [
            new("a", new(8, 4, 20), new(3, 12), [new(3, 10, new byte[] { 1, 2, 3 }), new(4, 0, new byte[] { 4, 5 })],
                new(4, 1)),
            new("b", null)
        ]);
        yield return ("poll", ReplicationPeerWire.EncodePoll(poll), bytes => ReplicationPeerWire.DecodePoll(bytes, Requested));
        yield return ("presence", ReplicationPeerWire.EncodePresence(new(3, true,
                new Dictionary<string, string> { ["https://a"] = "A", ["https://b"] = "" })),
            bytes => ReplicationPeerWire.DecodePresence(bytes));
        yield return ("submit", EventLogWire.Encode(new EventLogSubmitRequest("events", 5, 3, 2, true, 7,
            [new byte[] { 1, 2 }, Array.Empty<byte>()])), bytes => EventLogWire.DecodeSubmit(bytes));
        yield return ("submit response", EventLogWire.Encode(new EventLogSubmitResponse(EventLogSubmitStatus.Committed,
            [4, 5], 6, "https://owner")), bytes => EventLogWire.DecodeSubmitResponse(bytes));
        yield return ("topic", EventLogWire.EncodeTopic("events", 9), bytes => EventLogWire.DecodeTopic(bytes));
        yield return ("bounds", EventLogWire.Encode(new EventLogBoundsResponse(true, new(2, 9), "https://owner")),
            bytes => EventLogWire.DecodeBounds(bytes));
        yield return ("live", EventLogWire.Encode(new EventLogLiveMessage(EventLogLiveKind.Batch, 4,
            [new byte[] { 9 }], "v1", null))[4..], bytes => EventLogWire.DecodeLive(bytes));
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 40).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void MalformedMessagesFailWithInvalidData(int seed)
    {
        var random = new Random(seed);
        foreach (var (name, original, decode) in Messages())
        {
            decode(original);
            for (var i = 0; i < 300; i++)
            {
                var bytes = original.ToList();
                for (var changes = random.Next(1, 4); changes > 0; changes--)
                {
                    var at = random.Next(bytes.Count + 1);
                    switch (random.Next(4))
                    {
                        case 0 when at < bytes.Count:
                            bytes[at] = (byte)random.Next(256);
                            break;
                        case 1 when at < bytes.Count:
                            bytes[at] = (byte)(random.Next(2) == 0 ? 0xFF : 0x80 | random.Next(128));
                            break;
                        case 2:
                            bytes.Insert(at, (byte)random.Next(256));
                            break;
                        default:
                            bytes.RemoveRange(at, Math.Min(random.Next(1, 4), bytes.Count - at));
                            break;
                    }
                }

                var mutated = bytes.ToArray();
                var before = GC.GetAllocatedBytesForCurrentThread();
                try
                {
                    decode(mutated);
                }
                catch (InvalidDataException)
                {
                }
                catch (Exception error)
                {
                    Assert.Fail($"Seed {seed}, {name}, {Convert.ToHexString(mutated)}: {error.GetType().Name}: {error.Message}");
                }

                Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 1 << 20);
            }
        }
    }
}
