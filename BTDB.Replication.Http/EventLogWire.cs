using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.Http;

/// <summary>Versioned little-endian event-log peer messages. Decoding validates every length before allocating.</summary>
internal static class EventLogWire
{
    const byte Version = 1;

    sealed class Writer
    {
        byte[] _buffer = new byte[256];
        int _length;

        Span<byte> Reserve(int count)
        {
            if (_length + count > _buffer.Length) Array.Resize(ref _buffer, Math.Max(_buffer.Length * 2, _length + count));
            var span = _buffer.AsSpan(_length, count);
            _length += count;
            return span;
        }

        public Writer U8(byte value) { Reserve(1)[0] = value; return this; }
        public Writer U32(uint value) { BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), value); return this; }
        public Writer U64(ulong value) { BinaryPrimitives.WriteUInt64LittleEndian(Reserve(8), value); return this; }

        public Writer Text(string? value)
        {
            if (value == null) return U32(uint.MaxValue);
            var bytes = Encoding.UTF8.GetBytes(value);
            U32((uint)bytes.Length);
            bytes.CopyTo(Reserve(bytes.Length));
            return this;
        }

        public Writer Records(IReadOnlyList<ReadOnlyMemory<byte>> records)
        {
            U32((uint)records.Count);
            foreach (var record in records)
            {
                U32((uint)record.Length);
                record.Span.CopyTo(Reserve(record.Length));
            }
            return this;
        }

        public byte[] ToArray() => _buffer.AsSpan(0, _length).ToArray();
    }

    ref struct Reader(ReadOnlySpan<byte> bytes)
    {
        readonly ReadOnlySpan<byte> _bytes = bytes;
        int _position;

        ReadOnlySpan<byte> Take(int count)
        {
            if (count < 0 || _bytes.Length - _position < count) throw new InvalidDataException("Truncated event log message.");
            var span = _bytes.Slice(_position, count);
            _position += count;
            return span;
        }

        public byte U8() => Take(1)[0];
        public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));
        public ulong U64() => BinaryPrimitives.ReadUInt64LittleEndian(Take(8));

        public string? Text()
        {
            var length = U32();
            if (length == uint.MaxValue) return null;
            if (length > 4096) throw new InvalidDataException("Event log text is too long.");
            return Encoding.UTF8.GetString(Take((int)length));
        }

        public ReadOnlyMemory<byte>[] Records()
        {
            var count = U32();
            if (count > (uint)(_bytes.Length - _position) / 4) throw new InvalidDataException("Invalid record count.");
            var records = new ReadOnlyMemory<byte>[count];
            for (var i = 0; i < records.Length; i++)
            {
                var length = U32();
                if (length > (uint)(_bytes.Length - _position)) throw new InvalidDataException("Invalid record length.");
                records[i] = Take((int)length).ToArray();
            }
            return records;
        }

        public void Version()
        {
            if (U8() != EventLogWire.Version) throw new InvalidDataException("Unsupported event log protocol version.");
        }

        public void End()
        {
            if (_position != _bytes.Length) throw new InvalidDataException("Trailing bytes in event log message.");
        }
    }

    public static byte[] Encode(EventLogSubmitRequest request) => new Writer().U8(Version).Text(request.Topic)
        .U64(request.Session).U64(request.FirstSequence).U64(request.OldestUnresolved)
        .U8(request.PreviouslyDispatched ? (byte)1 : (byte)0).U64(request.ResumeFrom).Records(request.Records).ToArray();

    public static EventLogSubmitRequest DecodeSubmit(ReadOnlySpan<byte> bytes)
    {
        var reader = new Reader(bytes);
        reader.Version();
        var topic = reader.Text() ?? throw new InvalidDataException("Missing topic.");
        var session = reader.U64();
        var first = reader.U64();
        var oldest = reader.U64();
        var dispatched = reader.U8() switch { 0 => false, 1 => true, _ => throw new InvalidDataException("Invalid flag.") };
        var resumeFrom = reader.U64();
        var records = reader.Records();
        reader.End();
        if (records.Length == 0 || first == 0 || oldest == 0 || oldest > first)
            throw new InvalidDataException("Invalid submission sequences.");
        return new(topic, session, first, oldest, dispatched, resumeFrom, records);
    }

    public static byte[] Encode(EventLogSubmitResponse response)
    {
        var writer = new Writer().U8(Version).U8((byte)response.Status).U64(response.DurableNext)
            .Text(response.OwnerEndpoint).U32((uint)response.Offsets.Count);
        foreach (var offset in response.Offsets) writer.U64(offset);
        return writer.ToArray();
    }

    public static EventLogSubmitResponse DecodeSubmitResponse(ReadOnlySpan<byte> bytes)
    {
        var reader = new Reader(bytes);
        reader.Version();
        var status = reader.U8();
        if (status > (byte)EventLogSubmitStatus.SequenceGap) throw new InvalidDataException("Invalid status.");
        var next = reader.U64();
        var owner = reader.Text();
        var count = reader.U32();
        if (count > (uint)bytes.Length / 8) throw new InvalidDataException("Invalid offset count.");
        var offsets = new ulong[count];
        for (var i = 0; i < offsets.Length; i++) offsets[i] = reader.U64();
        reader.End();
        return new((EventLogSubmitStatus)status, offsets, next, owner);
    }

    public static byte[] EncodeTopic(string topic, ulong from = 0) => new Writer().U8(Version).Text(topic).U64(from).ToArray();

    public static (string Topic, ulong From) DecodeTopic(ReadOnlySpan<byte> bytes)
    {
        var reader = new Reader(bytes);
        reader.Version();
        var topic = reader.Text() ?? throw new InvalidDataException("Missing topic.");
        var from = reader.U64();
        reader.End();
        return (topic, from);
    }

    public static byte[] Encode(EventLogBoundsResponse response) => new Writer().U8(Version)
        .U8(response.IsOwner ? (byte)1 : (byte)0).U64(response.Bounds.First).U64(response.Bounds.Next)
        .Text(response.OwnerEndpoint).ToArray();

    public static EventLogBoundsResponse DecodeBounds(ReadOnlySpan<byte> bytes)
    {
        var reader = new Reader(bytes);
        reader.Version();
        var owner = reader.U8() switch { 0 => false, 1 => true, _ => throw new InvalidDataException("Invalid flag.") };
        var first = reader.U64();
        var next = reader.U64();
        var endpoint = reader.Text();
        reader.End();
        if (first > next) throw new InvalidDataException("Invalid bounds.");
        return new(owner, new(first, next), endpoint);
    }

    /// <summary>One length-prefixed live message; the prefix counts the bytes after it.</summary>
    public static byte[] Encode(EventLogLiveMessage message)
    {
        var body = new Writer().U8(Version).U8((byte)message.Kind).U64(message.Offset).Text(message.TailVersion)
            .Text(message.OwnerEndpoint).Records(message.Records).ToArray();
        var framed = new byte[body.Length + 4];
        BinaryPrimitives.WriteUInt32LittleEndian(framed, (uint)body.Length);
        body.CopyTo(framed, 4);
        return framed;
    }

    public static EventLogLiveMessage DecodeLive(ReadOnlySpan<byte> body)
    {
        var reader = new Reader(body);
        reader.Version();
        var kind = reader.U8();
        if (kind > (byte)EventLogLiveKind.NotOwner) throw new InvalidDataException("Invalid live message kind.");
        var offset = reader.U64();
        var version = reader.Text();
        var owner = reader.Text();
        var records = reader.Records();
        reader.End();
        if (kind == (byte)EventLogLiveKind.Batch && records.Length == 0) throw new InvalidDataException("Empty batch.");
        return new((EventLogLiveKind)kind, offset, records, version, owner);
    }
}
