using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.Test.EventLog;

public class EventLogFormatTest
{
    static byte[] Split(ulong id, ulong first, IEnumerable<byte[][]> frames, bool seal)
    {
        var bytes = new List<byte>(EventLogFormat.CreateSplit("t", id, first));
        var next = first;
        foreach (var records in frames)
        {
            var memory = records.Select(r => (ReadOnlyMemory<byte>)r).ToArray();
            var frame = new byte[EventLogFormat.FrameLength(1, records.Length, records.Sum(r => r.Length))];
            EventLogFormat.WriteFrame(frame, next, memory, [new(7, next + 100, (uint)records.Length)]);
            bytes.AddRange(frame);
            next += (ulong)records.Length;
        }
        if (seal)
        {
            var sealBytes = new byte[EventLogFormat.SealLength];
            EventLogFormat.WriteSeal(sealBytes, next, id + 1);
            bytes.AddRange(sealBytes);
        }
        return bytes.ToArray();
    }

    [Fact]
    public void SplitRoundTripsFramesRecordsTransfersAndSeal()
    {
        var content = Split(3, 10, [[[1], [2, 3]], [[], [4, 5, 6]]], true);
        var parsed = EventLogFormat.Parse(content, "t");
        Assert.Equal(0, parsed.Level);
        Assert.Equal(3ul, parsed.SplitId);
        Assert.Equal(10ul, parsed.FirstOffset);
        Assert.Equal(14ul, parsed.NextOffset);
        Assert.Equal(new EventLogSeal(14, 4), parsed.Seal);
        var records = parsed.Frames.SelectMany(f => EventLogFormat.Records(content, f)).ToArray();
        Assert.Equal([10ul, 11, 12, 13], records.Select(r => r.Offset));
        Assert.Equal(new byte[] { 2, 3 }, records[1].Payload.ToArray());
        Assert.Empty(records[2].Payload.ToArray());
        Assert.Equal([(7ul, 110ul), (7ul, 111ul)], EventLogFormat.Transfers(parsed.Frames[0]));
    }

    [Fact]
    public void EmptySplitIsValidButNeverSealed()
    {
        Assert.Equal(5ul, EventLogFormat.Parse(Split(1, 5, [], false), "t").NextOffset);
        Assert.Throws<EventLogCorruptedException>(() => EventLogFormat.Parse(Split(1, 5, [], true), "t"));
    }

    [Fact]
    public void AnyFlippedByteOrTruncationIsCorruption()
    {
        var content = Split(1, 0, [[[1, 2, 3]], [[4]]], true);
        for (var i = 0; i < content.Length; i++)
        {
            var copy = content.ToArray();
            copy[i] ^= 0x40;
            Assert.ThrowsAny<Exception>(() => EventLogFormat.Parse(copy, "t"));
        }
        for (var length = 1; length < content.Length; length++)
        {
            var header = EventLogFormat.SplitHeaderLength("t");
            if (length == header) continue; // exactly the empty split
            if (length > header && EndsAtFrameBoundary(content, length)) continue;
            Assert.ThrowsAny<Exception>(() => EventLogFormat.Parse(content.AsSpan(0, length), "t"));
        }
        Assert.Throws<EventLogCorruptedException>(() => EventLogFormat.Parse(content, "u"));
    }

    static bool EndsAtFrameBoundary(byte[] content, int length) =>
        EventLogFormat.Parse(content, "t").Frames.Any(f => f.Position + f.Length == length);

    [Fact]
    public void MergedObjectKeepsFramesByteForByteAndIndexesEveryRecord()
    {
        var first = Split(1, 0, [[[1], [2]]], true);
        var second = Split(2, 2, [[[3, 3]], [[4], [5]]], true);
        var sources = new[] { first, second }.Select(c => ((ReadOnlyMemory<byte>)c, EventLogFormat.Parse(c, "t"))).ToArray();
        var merged = EventLogFormat.CreateMerged("t", 1, 1, 2, sources);
        Assert.Equal(merged, EventLogFormat.CreateMerged("t", 1, 1, 2, sources));
        var parsed = EventLogFormat.Parse(merged, "t");
        Assert.Equal(1, parsed.Level);
        Assert.Equal((1ul, 2ul, 0ul, 5ul), (parsed.SplitId, parsed.LastSplitId, parsed.FirstOffset, parsed.NextOffset));
        Assert.True(parsed.IsSealed);
        var expected = sources.SelectMany(s => s.Item2.Frames.SelectMany(f => EventLogFormat.Records(s.Item1, f)))
            .Select(r => (r.Offset, r.Payload.ToArray())).ToArray();
        var actual = parsed.Frames.SelectMany(f => EventLogFormat.Records(merged, f))
            .Select(r => (r.Offset, r.Payload.ToArray())).ToArray();
        Assert.Equal(expected.Length, actual.Length);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Offset, actual[i].Offset);
            Assert.Equal(expected[i].Item2, actual[i].Item2);
        }
        var copy = merged.ToArray();
        copy[parsed.HeaderLength - 6] ^= 1; // a record start table entry
        Assert.Throws<EventLogCorruptedException>(() => EventLogFormat.Parse(copy, "t"));
    }

    [Theory]
    [InlineData("a", true)]
    [InlineData("events-data.v1_x", true)]
    [InlineData("", false)]
    [InlineData("A", false)]
    [InlineData("a/b", false)]
    [InlineData("..", false)]
    public void TopicNamesAreRestricted(string name, bool valid) => Assert.Equal(valid, EventLogFormat.IsValidTopic(name));

    [Fact]
    public void KeysRoundTripAndSortByNumber()
    {
        Assert.True(EventLogFormat.TryParseKey("t", EventLogFormat.SplitKey("t", 42), out var level, out var id));
        Assert.Equal((0, 42ul), (level, id));
        Assert.True(EventLogFormat.TryParseKey("t", EventLogFormat.MergedKey("t", 2, 12345), out level, out id));
        Assert.Equal((2, 12345ul), (level, id));
        Assert.False(EventLogFormat.TryParseKey("t", "t/l1/x.elog", out _, out _));
        Assert.False(EventLogFormat.TryParseKey("t", "tt/" + new string('0', 20) + ".elog", out _, out _));
        Assert.True(string.CompareOrdinal(EventLogFormat.MergedKey("t", 1, 9), EventLogFormat.MergedKey("t", 1, 10)) < 0);
    }
}
