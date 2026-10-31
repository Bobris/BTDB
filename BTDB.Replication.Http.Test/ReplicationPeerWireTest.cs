using System;
using System.Collections.Generic;
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
                     Base(PeerOperation.Handoff) with { Handoff = new(9, "10000000-0000-0000-0000-000000000009") },
                     Base(PeerOperation.Presence) with { Presence = new("https://follower", "value", 12) },
                     Base(PeerOperation.Presence) with { Presence = new("https://follower", null, 0) }
                 })
            Assert.Equal(request, ReplicationPeerWire.DecodeRequest(ReplicationPeerWire.EncodeRequest(request)));
    }

    /// A string field declaring far more characters than the message holds is truncated input: decoding fails with
    /// InvalidDataException without allocating the declared length.
    [Theory]
    [InlineData(1000)]
    [InlineData(1 << 28)]
    [InlineData(int.MaxValue)]
    public void StringLongerThanTheMessageFailsWithoutAllocatingIt(int declared)
    {
        var writer = new BTDB.StreamLayer.MemWriter();
        writer.WriteUInt8(1);
        writer.WriteUInt8((byte)PeerOperation.Connect);
        writer.WriteVUInt64((ulong)declared + 1);
        writer.WriteBlock("abc"u8);
        var bytes = writer.GetSpan().ToArray();
        var before = System.GC.GetAllocatedBytesForCurrentThread();
        Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodeRequest(bytes));
        Assert.InRange(System.GC.GetAllocatedBytesForCurrentThread() - before, 0, 64 * 1024);
    }

    [Fact]
    public void PresenceViewRoundTripsAndRejectsOversizedValues()
    {
        var members = new Dictionary<string, string> { ["https://a"] = "A", ["https://b"] = "" };
        var view = ReplicationPeerWire.DecodePresence(ReplicationPeerWire.EncodePresence(new(3, true, members)));
        Assert.Equal((3ul, true), (view.Version, view.Settling));
        Assert.Equal(members, view.Members);
        Assert.Null(ReplicationPeerWire.DecodePresence(ReplicationPeerWire.EncodePresence(new(4, false, null))).Members);
        var oversized = Base(PeerOperation.Presence) with
        {
            Presence = new("https://follower", new string('x', ReplicationPresence.MaximumValueLength + 1), 0)
        };
        Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodeRequest(ReplicationPeerWire.EncodeRequest(oversized)));
    }

    [Fact]
    public void ApplicationRequestRoundTripsAndRejectsInvalidChannelsAndSizes()
    {
        var request = Base(PeerOperation.Application) with { Channel = "actions", Payload = new byte[] { 1, 2, 3 } };
        var decoded = ReplicationPeerWire.DecodeRequest(ReplicationPeerWire.EncodeRequest(request));
        Assert.Equal("actions", decoded.Channel);
        Assert.Equal(new byte[] { 1, 2, 3 }, decoded.Payload.ToArray());
        Assert.Equal(request with { Payload = default }, decoded with { Payload = default });
        Assert.True(ReplicationPeerWire.DecodeRequest(ReplicationPeerWire.EncodeRequest(request with { Payload = default }))
            .Payload.IsEmpty);
        foreach (var invalid in new[]
                 {
                     request with { Channel = "" },
                     request with { Channel = new string('c', ReplicationLeaderChannel.MaximumNameLength + 1) },
                     request with { Payload = new byte[ReplicationLeaderChannel.MaximumMessageBytes + 1] }
                 })
            Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodeRequest(ReplicationPeerWire.EncodeRequest(invalid)));
        var truncated = ReplicationPeerWire.EncodeRequest(request);
        Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodeRequest(truncated.AsMemory(0, truncated.Length - 1)));
    }

    [Fact]
    public void PollResponseRoundTripsAndSlicesInlineBytesWithoutCopying()
    {
        ReplicationPeerPollRequest[] requested = [new("a", new(3, 10)), new("b")];
        var poll = new ReplicationPeerPoll(5, true,
        [
            new("a", new(8, 4, 20), new(3, 12), [new(3, 10, new byte[] { 1, 2, 3 }), new(4, 0, new byte[] { 4, 5 })],
                new(4, 1)),
            new("b", null)
        ]);
        var bytes = ReplicationPeerWire.EncodePoll(poll);
        var decoded = ReplicationPeerWire.DecodePoll(bytes, requested);
        decoded.Validate(5, requested, 5);
        Assert.Equal((5L, true), (decoded.Challenge, decoded.Granted));
        Assert.Equal(poll.Databases[1], decoded.Databases[1]);
        var a = decoded.Databases[0];
        Assert.Equal((poll.Databases[0].Progress, poll.Databases[0].Published, poll.Databases[0].Schema),
            (a.Progress, a.Published, a.Schema));
        Assert.Equal(new byte[] { 4, 5 }, a.Chunks![1].Bytes.ToArray());
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(a.Chunks[0].Bytes, out var segment));
        Assert.Same(bytes, segment.Array);
        Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodePoll(bytes[..^1], requested));
        Assert.Throws<InvalidDataException>(() => ReplicationPeerWire.DecodePoll(bytes, [new("a")]));
    }
}
