using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using BTDB.Buffer;

namespace BTDB.Replication.EventLog;

/// <summary>Publisher session and consecutive sequence numbers of the records a frame carries, in record order.</summary>
internal readonly record struct EventLogTransferRun(ulong Session, ulong FirstSequence, uint Count);

internal sealed record EventLogFrame(int Position, int Length, ulong FirstOffset, uint RecordCount,
    EventLogTransferRun[] Runs, int RecordsPosition)
{
    public ulong NextOffset => FirstOffset + RecordCount;
}

internal sealed record EventLogSeal(ulong NextOffset, ulong SuccessorSplitId);

/// <summary>A parsed split or merged object. Level 0 is an ordinary split.</summary>
internal sealed class EventLogObject
{
    public required byte Level { get; init; }
    public required ulong SplitId { get; init; }
    public required ulong LastSplitId { get; init; }
    public required ulong FirstOffset { get; init; }
    public required ulong NextOffset { get; init; }
    public required IReadOnlyList<EventLogFrame> Frames { get; init; }
    public EventLogSeal? Seal { get; init; }
    public required int HeaderLength { get; init; }
    public required int Length { get; init; }
    public ulong RecordCount => NextOffset - FirstOffset;
    public bool IsSealed => Seal != null || Level > 0;
}

/// <summary>
/// Binary layout, little-endian. Every object starts with "BTEL", format version and kind, the topic name, its first
/// split ID and first record offset.
/// A split is the header, a checksum, then commit frames and optionally a terminal seal. A merged object additionally
/// carries its level, last split ID, record count, frame and record start tables in the header, then the frames
/// byte-for-byte and an end marker. A frame is 'F', body length, first offset, record count, transfer runs, records
/// (length + bytes) and a Fletcher-32 checksum over marker, length and body. The seal is 'S', next offset, successor
/// split ID and a checksum.
/// </summary>
internal static class EventLogFormat
{
    static ReadOnlySpan<byte> Magic => "BTEL"u8;
    const byte FormatVersion = 1;
    const byte KindSplit = 1;
    const byte KindMerged = 2;
    const byte FrameMarker = (byte)'F';
    const byte SealMarker = (byte)'S';
    const byte EndMarker = (byte)'E';
    const byte EntryWidth = 4;
    internal const int SealLength = 1 + 8 + 8 + 4;
    internal const int EndLength = 1 + 8 + 4;
    const int RunLength = 8 + 8 + 4;
    internal const int MaxTopicLength = 100;
    internal const int MaxObjectLength = int.MaxValue - 64;

    public static bool IsValidTopic(string name)
    {
        if (name.Length is 0 or > MaxTopicLength) return false;
        foreach (var c in name)
            if (!(c is >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_' or '.')) return false;
        return name != "." && name != "..";
    }

    public static void ValidateTopic(string name)
    {
        if (!IsValidTopic(name))
            throw new ArgumentException("Topic names are 1–100 characters from [a-z0-9-_.].", nameof(name));
    }

    public static string SplitKey(string topic, ulong splitId) =>
        string.Create(CultureInfo.InvariantCulture, $"{topic}/{splitId:D20}.elog");

    public static string MergedPrefix(string topic, int level) =>
        string.Create(CultureInfo.InvariantCulture, $"{topic}/l{level}/");

    public static string MergedKey(string topic, int level, ulong firstOffset) =>
        string.Create(CultureInfo.InvariantCulture, $"{MergedPrefix(topic, level)}{firstOffset:D20}.elog");

    /// <summary>Parse "{topic}/{id}.elog" or "{topic}/l{level}/{offset}.elog"; level 0 returns the split ID.</summary>
    public static bool TryParseKey(string topic, string key, out int level, out ulong number)
    {
        level = 0;
        number = 0;
        if (!key.StartsWith(topic, StringComparison.Ordinal) || key.Length < topic.Length + 26 ||
            key[topic.Length] != '/' || !key.EndsWith(".elog", StringComparison.Ordinal)) return false;
        var rest = key.AsSpan(topic.Length + 1, key.Length - topic.Length - 6);
        if (rest.Length > 20)
        {
            if (rest.Length != 23 || rest[0] != 'l' || rest[2] != '/' || rest[1] is < '1' or > '9') return false;
            level = rest[1] - '0';
            rest = rest[3..];
        }
        if (rest.Length != 20) return false;
        foreach (var c in rest)
            if (c is < '0' or > '9') return false;
        return ulong.TryParse(rest, NumberStyles.None, CultureInfo.InvariantCulture, out number);
    }

    static int TopicBytes(string topic) => Encoding.UTF8.GetByteCount(topic);

    public static int SplitHeaderLength(string topic) => 4 + 1 + 1 + 2 + TopicBytes(topic) + 8 + 8 + 4;

    public static int FrameLength(int runCount, int recordCount, long payloadBytes) =>
        checked((int)(1 + 4 + 8 + 4 + 4 + (long)runCount * RunLength + (long)recordCount * 4 + payloadBytes + 4));

    static int WriteCommonHeader(Span<byte> destination, byte kind, string topic, ulong splitId, ulong firstOffset)
    {
        Magic.CopyTo(destination);
        destination[4] = FormatVersion;
        destination[5] = kind;
        var topicLength = Encoding.UTF8.GetBytes(topic, destination[8..]);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[6..], (ushort)topicLength);
        var position = 8 + topicLength;
        BinaryPrimitives.WriteUInt64LittleEndian(destination[position..], splitId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[(position + 8)..], firstOffset);
        return position + 16;
    }

    static int WriteChecksum(Span<byte> destination, int length)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(destination[length..], Checksum.CalcFletcher32(destination[..length]));
        return length + 4;
    }

    public static int WriteSplitHeader(Span<byte> destination, string topic, ulong splitId, ulong firstOffset) =>
        WriteChecksum(destination, WriteCommonHeader(destination, KindSplit, topic, splitId, firstOffset));

    public static int WriteFrame(Span<byte> destination, ulong firstOffset, IReadOnlyList<ReadOnlyMemory<byte>> records,
        IReadOnlyList<EventLogTransferRun> runs)
    {
        long payload = 0;
        foreach (var record in records) payload += record.Length;
        var length = FrameLength(runs.Count, records.Count, payload);
        var frame = destination[..length];
        frame[0] = FrameMarker;
        BinaryPrimitives.WriteUInt32LittleEndian(frame[1..], (uint)(length - 9));
        BinaryPrimitives.WriteUInt64LittleEndian(frame[5..], firstOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[13..], (uint)records.Count);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[17..], (uint)runs.Count);
        var position = 21;
        foreach (var run in runs)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(frame[position..], run.Session);
            BinaryPrimitives.WriteUInt64LittleEndian(frame[(position + 8)..], run.FirstSequence);
            BinaryPrimitives.WriteUInt32LittleEndian(frame[(position + 16)..], run.Count);
            position += RunLength;
        }
        foreach (var record in records)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(frame[position..], (uint)record.Length);
            record.Span.CopyTo(frame[(position + 4)..]);
            position += 4 + record.Length;
        }
        return WriteChecksum(frame, position);
    }

    public static int WriteSeal(Span<byte> destination, ulong nextOffset, ulong successorSplitId)
    {
        destination[0] = SealMarker;
        BinaryPrimitives.WriteUInt64LittleEndian(destination[1..], nextOffset);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[9..], successorSplitId);
        return WriteChecksum(destination, 17);
    }

    public static byte[] CreateSplit(string topic, ulong splitId, ulong firstOffset)
    {
        var bytes = new byte[SplitHeaderLength(topic)];
        WriteSplitHeader(bytes, topic, splitId, firstOffset);
        return bytes;
    }

    /// <summary>Build a merged object from the frames of consecutive objects covering splits
    /// <paramref name="firstSplitId"/>..<paramref name="lastSplitId"/>. The result depends only on the records.</summary>
    public static byte[] CreateMerged(string topic, int level, ulong firstSplitId, ulong lastSplitId,
        IReadOnlyList<(ReadOnlyMemory<byte> Content, EventLogObject Object)> sources)
    {
        if (sources.Count == 0) throw new ArgumentException("A merge needs sources.", nameof(sources));
        var firstOffset = sources[0].Object.FirstOffset;
        long frameBytes = 0;
        var frameCount = 0;
        ulong recordCount = 0;
        var expected = firstOffset;
        foreach (var (_, source) in sources)
        {
            if (source.FirstOffset != expected) throw new ArgumentException("Merge sources must be consecutive.");
            foreach (var frame in source.Frames)
            {
                frameBytes += frame.Length;
                frameCount++;
            }
            recordCount += source.RecordCount;
            expected = source.NextOffset;
        }
        var headerLength = checked(8 + TopicBytes(topic) + 16 + 1 + 8 + 8 + 4 + 1 +
                                   (frameCount + (long)recordCount) * EntryWidth + 4);
        var total = headerLength + frameBytes + EndLength;
        if (total > MaxObjectLength) throw new ArgumentException("The merged object is too large.");
        var bytes = new byte[total];
        var span = bytes.AsSpan();
        var position = WriteCommonHeader(span, KindMerged, topic, firstSplitId, firstOffset);
        span[position] = (byte)level;
        BinaryPrimitives.WriteUInt64LittleEndian(span[(position + 1)..], lastSplitId);
        BinaryPrimitives.WriteUInt64LittleEndian(span[(position + 9)..], recordCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span[(position + 17)..], (uint)frameCount);
        span[position + 21] = EntryWidth;
        var frameTable = position + 22;
        var recordTable = frameTable + frameCount * EntryWidth;
        var target = (int)headerLength;
        var frameIndex = 0;
        var recordIndex = 0;
        foreach (var (content, source) in sources)
        {
            foreach (var frame in source.Frames)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(span[(frameTable + frameIndex++ * EntryWidth)..], (uint)target);
                var frameSpan = content.Span.Slice(frame.Position, frame.Length);
                var recordPosition = frame.RecordsPosition - frame.Position;
                for (var i = 0; i < frame.RecordCount; i++)
                {
                    BinaryPrimitives.WriteUInt32LittleEndian(span[(recordTable + recordIndex++ * EntryWidth)..],
                        (uint)(target + recordPosition));
                    recordPosition += 4 + (int)BinaryPrimitives.ReadUInt32LittleEndian(frameSpan[recordPosition..]);
                }
                frameSpan.CopyTo(span[target..]);
                target += frame.Length;
            }
        }
        WriteChecksum(span, (int)headerLength - 4);
        span[target] = EndMarker;
        BinaryPrimitives.WriteUInt64LittleEndian(span[(target + 1)..], expected);
        WriteChecksum(span[target..], 9);
        return bytes;
    }

    static EventLogCorruptedException Corrupt(string what) => new($"Event log object is corrupted: {what}.");

    static void VerifyChecksum(ReadOnlySpan<byte> content, int length, string what)
    {
        if (content.Length < length + 4 || Checksum.CalcFletcher32(content[..length]) !=
            BinaryPrimitives.ReadUInt32LittleEndian(content[length..])) throw Corrupt(what);
    }

    /// <summary>Validate and parse complete object content. Anything that does not match is corruption.</summary>
    public static EventLogObject Parse(ReadOnlySpan<byte> content, string topic)
    {
        if (content.Length < 8 || !content[..4].SequenceEqual(Magic) || content[4] != FormatVersion)
            throw Corrupt("header");
        var kind = content[5];
        var topicLength = BinaryPrimitives.ReadUInt16LittleEndian(content[6..]);
        var position = 8 + topicLength;
        if (content.Length < position + 16 || Encoding.UTF8.GetString(content.Slice(8, topicLength)) != topic)
            throw Corrupt("topic");
        var splitId = BinaryPrimitives.ReadUInt64LittleEndian(content[position..]);
        var firstOffset = BinaryPrimitives.ReadUInt64LittleEndian(content[(position + 8)..]);
        position += 16;
        byte level = 0;
        var lastSplitId = splitId;
        ulong recordCount = 0;
        var frameCount = 0;
        int frameTable = 0, recordTable = 0;
        if (kind == KindMerged)
        {
            if (content.Length < position + 22) throw Corrupt("merged header");
            level = content[position];
            lastSplitId = BinaryPrimitives.ReadUInt64LittleEndian(content[(position + 1)..]);
            recordCount = BinaryPrimitives.ReadUInt64LittleEndian(content[(position + 9)..]);
            frameCount = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(content[(position + 17)..]));
            if (level == 0 || content[position + 21] != EntryWidth || lastSplitId < splitId) throw Corrupt("merged header");
            frameTable = position + 22;
            recordTable = frameTable + frameCount * EntryWidth;
            var tables = (frameCount + (long)recordCount) * EntryWidth;
            if (content.Length < frameTable + tables + 4) throw Corrupt("merged header");
            position = frameTable + (int)tables;
        }
        else if (kind != KindSplit) throw Corrupt("kind");
        VerifyChecksum(content, position, "header checksum");
        var headerLength = position + 4;
        position = headerLength;
        var frames = new List<EventLogFrame>();
        var next = firstOffset;
        EventLogSeal? seal = null;
        var sawEnd = false;
        var recordIndex = 0;
        while (position < content.Length)
        {
            var marker = content[position];
            if (seal != null || sawEnd) throw Corrupt("data after the end");
            if (marker == FrameMarker)
            {
                if (content.Length < position + 25) throw Corrupt("frame");
                var length = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(content[(position + 1)..]) + 9);
                if (length < 25 || content.Length - position < length) throw Corrupt("frame length");
                var frame = content.Slice(position, length);
                VerifyChecksum(frame, length - 4, "frame checksum");
                var frameOffset = BinaryPrimitives.ReadUInt64LittleEndian(frame[5..]);
                var records = BinaryPrimitives.ReadUInt32LittleEndian(frame[13..]);
                var runCount = BinaryPrimitives.ReadUInt32LittleEndian(frame[17..]);
                if (frameOffset != next || records == 0 || runCount > records) throw Corrupt("frame offsets");
                var runs = new EventLogTransferRun[runCount];
                var local = 21;
                ulong runRecords = 0;
                for (var i = 0; i < runCount; i++)
                {
                    if (local + RunLength > length - 4) throw Corrupt("frame runs");
                    runs[i] = new(BinaryPrimitives.ReadUInt64LittleEndian(frame[local..]),
                        BinaryPrimitives.ReadUInt64LittleEndian(frame[(local + 8)..]),
                        BinaryPrimitives.ReadUInt32LittleEndian(frame[(local + 16)..]));
                    runRecords += runs[i].Count;
                    local += RunLength;
                }
                if (runRecords != records) throw Corrupt("frame runs");
                var recordsPosition = local;
                for (var i = 0; i < records; i++)
                {
                    if (local + 4 > length - 4) throw Corrupt("record");
                    if (kind == KindMerged && ReadEntry(content, recordTable, recordIndex) != position + local)
                        throw Corrupt("record table");
                    recordIndex++;
                    var recordLength = BinaryPrimitives.ReadUInt32LittleEndian(frame[local..]);
                    if (recordLength > (uint)(length - 4 - local - 4)) throw Corrupt("record length");
                    local += 4 + (int)recordLength;
                }
                if (local != length - 4) throw Corrupt("frame layout");
                if (kind == KindMerged && (frames.Count >= frameCount || ReadEntry(content, frameTable, frames.Count) != position))
                    throw Corrupt("frame table");
                frames.Add(new(position, length, frameOffset, records, runs, position + recordsPosition));
                next += records;
                position += length;
            }
            else if (marker == SealMarker && kind == KindSplit)
            {
                if (content.Length - position != SealLength) throw Corrupt("seal");
                VerifyChecksum(content[position..], 17, "seal checksum");
                seal = new(BinaryPrimitives.ReadUInt64LittleEndian(content[(position + 1)..]),
                    BinaryPrimitives.ReadUInt64LittleEndian(content[(position + 9)..]));
                if (seal.NextOffset != next || seal.SuccessorSplitId != splitId + 1 || frames.Count == 0)
                    throw Corrupt("seal");
                position += SealLength;
            }
            else if (marker == EndMarker && kind == KindMerged)
            {
                if (content.Length - position != EndLength) throw Corrupt("end marker");
                VerifyChecksum(content[position..], 9, "end checksum");
                if (BinaryPrimitives.ReadUInt64LittleEndian(content[(position + 1)..]) != next) throw Corrupt("end marker");
                sawEnd = true;
                position += EndLength;
            }
            else throw Corrupt("unknown marker");
        }
        if (kind == KindMerged && (!sawEnd || frames.Count != frameCount || next - firstOffset != recordCount))
            throw Corrupt("merged content");
        return new()
        {
            Level = level, SplitId = splitId, LastSplitId = lastSplitId, FirstOffset = firstOffset, NextOffset = next,
            Frames = frames, Seal = seal, HeaderLength = headerLength, Length = content.Length
        };
    }

    /// <summary>Validate a level-0 split header prefix of <see cref="SplitHeaderLength"/> bytes.</summary>
    public static (ulong SplitId, ulong FirstOffset) ParseSplitHeader(ReadOnlySpan<byte> header, string topic)
    {
        var length = SplitHeaderLength(topic);
        if (header.Length < length || !header[..4].SequenceEqual(Magic) || header[4] != FormatVersion ||
            header[5] != KindSplit) throw Corrupt("split header");
        var position = 8 + BinaryPrimitives.ReadUInt16LittleEndian(header[6..]);
        if (position + 20 != length || Encoding.UTF8.GetString(header[8..position]) != topic) throw Corrupt("topic");
        VerifyChecksum(header, position + 16, "header checksum");
        return (BinaryPrimitives.ReadUInt64LittleEndian(header[position..]),
            BinaryPrimitives.ReadUInt64LittleEndian(header[(position + 8)..]));
    }

    static int ReadEntry(ReadOnlySpan<byte> content, int table, int index) =>
        checked((int)BinaryPrimitives.ReadUInt32LittleEndian(content[(table + index * EntryWidth)..]));

    /// <summary>Records of one parsed frame with their offsets, as slices of <paramref name="content"/>.</summary>
    public static IEnumerable<EventLogRecord> Records(ReadOnlyMemory<byte> content, EventLogFrame frame)
    {
        var position = frame.RecordsPosition;
        for (var i = 0u; i < frame.RecordCount; i++)
        {
            var length = (int)BinaryPrimitives.ReadUInt32LittleEndian(content.Span[position..]);
            yield return new(frame.FirstOffset + i, content.Slice(position + 4, length));
            position += 4 + length;
        }
    }

    /// <summary>The transfer (session, sequence) of each record of a frame, in record order.</summary>
    public static IEnumerable<(ulong Session, ulong Sequence)> Transfers(EventLogFrame frame)
    {
        foreach (var run in frame.Runs)
            for (var i = 0u; i < run.Count; i++)
                yield return (run.Session, run.FirstSequence + i);
    }
}
