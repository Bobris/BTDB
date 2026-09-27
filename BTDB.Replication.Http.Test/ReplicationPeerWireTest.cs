using System.IO;
using Xunit;

namespace BTDB.Replication.Http.Test;

public class ReplicationPeerWireTest
{
    static PeerRequest Base(PeerOperation operation) => new("cluster", 7, "session", "https://node", operation);

    [Fact]
    public void EveryRequestOperationRoundTrips()
    {
        var poll = Base(PeerOperation.Poll) with
        {
            Challenge = -3, DurationTicks = 100, InlineBudget = 1024,
            Databases = [new("a", new(5, 77)), new("b")]
        };
        var decoded = ReplicationPeerWire.DecodeRequest(ReplicationPeerWire.EncodeRequest(poll));
        Assert.Equal(poll with { Databases = null }, decoded with { Databases = null });
        Assert.Equal(poll.Databases, decoded.Databases);
        foreach (var request in new[]
                 {
                     Base(PeerOperation.Connect),
                     Base(PeerOperation.Read) with { Database = "a", FileId = 3, Offset = 1ul << 40, Count = 4096 },
                     Base(PeerOperation.Handoff) with { Handoff = new(9, "10000000-0000-0000-0000-000000000009") }
                 })
            Assert.Equal(request, ReplicationPeerWire.DecodeRequest(ReplicationPeerWire.EncodeRequest(request)));
    }

    [Fact]
    public void PollResponseRoundTripsAndSlicesInlineBytesWithoutCopying()
    {
        ReplicationPeerPollRequest[] requested = [new("a", new(3, 10)), new("b")];
        var poll = new ReplicationPeerPoll(5, true,
        [
            new("a", new(8, 4, 20), new(3, 12), [new(3, 10, new byte[] { 1, 2, 3 }), new(4, 0, new byte[] { 4, 5 })]),
            new("b", null)
        ]);
        var bytes = ReplicationPeerWire.EncodePoll(poll);
        var decoded = ReplicationPeerWire.DecodePoll(bytes, requested);
        decoded.Validate(5, requested, 5);
        Assert.Equal((5L, true), (decoded.Challenge, decoded.Granted));
        Assert.Equal(poll.Databases[1], decoded.Databases[1]);
        var a = decoded.Databases[0];
        Assert.Equal((poll.Databases[0].Progress, poll.Databases[0].Published), (a.Progress, a.Published));
        Assert.Equal(new byte[] { 4, 5 }, a.Chunks![1].Bytes.ToArray());
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(a.Chunks[0].Bytes, out var segment));
        Assert.Same(bytes, segment.Array);
        Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodePoll(bytes[..^1], requested));
        Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodePoll(bytes, [new("a")]));
    }
}
