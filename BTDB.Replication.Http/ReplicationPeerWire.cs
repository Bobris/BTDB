using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication.Http;

internal enum PeerOperation : byte { Connect = 1, Poll = 2, Read = 3, Handoff = 4 }

/// <summary>Database addresses a range read; Databases lists every database of one poll (empty for a heartbeat).</summary>
internal sealed record PeerRequest(string ClusterId, ulong Term, string SessionId, string Endpoint, PeerOperation Operation,
    string? Database = null, long Challenge = 0, long DurationTicks = 0, uint FileId = 0, ulong Offset = 0,
    int Count = 0, PreparedHandoff? Handoff = null, ReplicationPeerPollRequest[]? Databases = null, int InlineBudget = 0);

/// <summary>
/// Compact binary peer messages: a version byte, then fixed fields in VUInt/string encoding. Requests carry the
/// selected session identity (the API key travels in the Authorization header). A poll response omits database
/// names, which follow the request order, and appends each inline TRL chunk's raw bytes after its header, so the
/// client slices them without copying. Malformed input throws InvalidDataException.
/// </summary>
internal static class ReplicationPeerWire
{
    const byte Version = 1;
    // Far above the handful of databases a node coordinates; bounds allocation from a hostile message.
    internal const int MaximumPolledDatabases = 256;
    const byte HasProgress = 1, HasPublished = 2, HasSchema = 4;

    public static byte[] EncodeRequest(PeerRequest request)
    {
        var writer = new MemWriter();
        writer.WriteUInt8(Version);
        writer.WriteUInt8((byte)request.Operation);
        writer.WriteString(request.ClusterId);
        writer.WriteVUInt64(request.Term);
        writer.WriteString(request.SessionId);
        writer.WriteString(request.Endpoint);
        switch (request.Operation)
        {
            case PeerOperation.Poll:
                writer.WriteVInt64(request.Challenge);
                writer.WriteVInt64(request.DurationTicks);
                writer.WriteVUInt32((uint)request.InlineBudget);
                var databases = request.Databases ?? [];
                writer.WriteVUInt32((uint)databases.Length);
                foreach (var database in databases)
                {
                    writer.WriteString(database.Database);
                    writer.WriteVUInt32(database.From.FileId);
                    writer.WriteVUInt32(database.From.Offset);
                }
                break;
            case PeerOperation.Read:
                writer.WriteString(request.Database);
                writer.WriteVUInt32(request.FileId);
                writer.WriteVUInt64(request.Offset);
                writer.WriteVUInt32((uint)request.Count);
                break;
            case PeerOperation.Handoff:
                writer.WriteVUInt64(request.Handoff!.ApplicationGeneration);
                writer.WriteString(request.Handoff.TransferId);
                break;
        }
        return writer.GetSpan().ToArray();
    }

    public static PeerRequest DecodeRequest(ReadOnlyMemory<byte> bytes) => Decode(bytes, static (ref MemReader reader) =>
    {
        var operation = (PeerOperation)reader.ReadUInt8();
        var request = new PeerRequest(RequiredString(ref reader), reader.ReadVUInt64(), RequiredString(ref reader),
            RequiredString(ref reader), operation);
        switch (operation)
        {
            case PeerOperation.Connect:
                return request;
            case PeerOperation.Poll:
                var challenge = reader.ReadVInt64();
                var duration = reader.ReadVInt64();
                var budget = reader.ReadVUInt32();
                var count = reader.ReadVUInt32();
                if (budget > ReplicationPeerPoll.MaximumInlineBytes || count > MaximumPolledDatabases)
                    throw new InvalidDataException("Peer poll exceeds its limits.");
                var databases = new ReplicationPeerPollRequest[count];
                for (var i = 0; i < databases.Length; i++)
                    databases[i] = new(RequiredString(ref reader), new(reader.ReadVUInt32(), reader.ReadVUInt32()));
                return request with { Challenge = challenge, DurationTicks = duration, InlineBudget = (int)budget, Databases = databases };
            case PeerOperation.Read:
                var database = RequiredString(ref reader);
                var fileId = reader.ReadVUInt32();
                var offset = reader.ReadVUInt64();
                var length = reader.ReadVUInt32();
                if (length > int.MaxValue) throw new InvalidDataException("Invalid TRL range.");
                return request with { Database = database, FileId = fileId, Offset = offset, Count = (int)length };
            case PeerOperation.Handoff:
                return request with { Handoff = new(reader.ReadVUInt64(), RequiredString(ref reader)) };
            default:
                throw new InvalidDataException("Unknown peer operation.");
        }
    });

    /// <summary>The poll response as segments: small encoded headers interleaved with each inline chunk's own bytes,
    /// so a server writes TRL bytes to the response without copying them into one buffer.</summary>
    public static List<ReadOnlyMemory<byte>> EncodePollSegments(ReplicationPeerPoll poll)
    {
        var segments = new List<ReadOnlyMemory<byte>>();
        var writer = new MemWriter();
        writer.WriteUInt8(Version);
        writer.WriteVInt64(poll.Challenge);
        writer.WriteBool(poll.Granted);
        writer.WriteVUInt32((uint)poll.Databases.Count);
        foreach (var database in poll.Databases)
        {
            writer.WriteUInt8((byte)((database.Progress != null ? HasProgress : 0) | (database.Published != null ? HasPublished : 0) |
                (database.Schema != null ? HasSchema : 0)));
            if (database.Progress is { } progress)
            {
                writer.WriteVUInt64(progress.EventId);
                writer.WriteVUInt32(progress.TrlFileId);
                writer.WriteVUInt32(progress.TrlPosition);
            }
            if (database.Published is { } published)
            {
                writer.WriteVUInt32(published.FileId);
                writer.WriteVUInt32(published.Offset);
            }
            if (database.Schema is { } schema)
            {
                writer.WriteVUInt32(schema.FileId);
                writer.WriteVUInt32(schema.Offset);
            }
            var chunks = database.Chunks ?? [];
            writer.WriteVUInt32((uint)chunks.Count);
            foreach (var chunk in chunks)
            {
                writer.WriteVUInt32(chunk.FileId);
                writer.WriteVUInt32(chunk.Offset);
                writer.WriteVUInt32((uint)chunk.Bytes.Length);
                segments.Add(writer.GetSpanAndReset().ToArray());
                segments.Add(chunk.Bytes);
            }
        }
        segments.Add(writer.GetSpanAndReset().ToArray());
        return segments;
    }

    public static byte[] EncodePoll(ReplicationPeerPoll poll)
    {
        var segments = EncodePollSegments(poll);
        var result = new byte[segments.Sum(segment => segment.Length)];
        var offset = 0;
        foreach (var segment in segments)
        {
            segment.Span.CopyTo(result.AsSpan(offset));
            offset += segment.Length;
        }
        return result;
    }

    /// <summary>Names come from the request; inline chunk bytes are slices of bytes, not copies.</summary>
    public static ReplicationPeerPoll DecodePoll(byte[] bytes, IReadOnlyList<ReplicationPeerPollRequest> requested) =>
        Decode(bytes, (ref MemReader reader) =>
        {
            var challenge = reader.ReadVInt64();
            var granted = reader.ReadBool();
            var count = reader.ReadVUInt32();
            if (count != requested.Count) throw new InvalidDataException("Peer poll answers another request.");
            var databases = new ReplicationPeerDatabaseProgress[count];
            for (var i = 0; i < databases.Length; i++)
            {
                var flags = reader.ReadUInt8();
                if ((flags & ~(HasProgress | HasPublished | HasSchema)) != 0) throw new InvalidDataException("Invalid peer progress flags.");
                LeaderTrlProgress? progress = (flags & HasProgress) != 0
                    ? new LeaderTrlProgress(reader.ReadVUInt64(), reader.ReadVUInt32(), reader.ReadVUInt32()) : null;
                TransactionLogPosition? published = (flags & HasPublished) != 0
                    ? new TransactionLogPosition(reader.ReadVUInt32(), reader.ReadVUInt32()) : null;
                TransactionLogPosition? schema = (flags & HasSchema) != 0
                    ? new TransactionLogPosition(reader.ReadVUInt32(), reader.ReadVUInt32()) : null;
                var chunkCount = reader.ReadVUInt32();
                List<ReplicationPeerTrlChunk>? chunks = null;
                for (var c = 0u; c < chunkCount; c++)
                {
                    var fileId = reader.ReadVUInt32();
                    var offset = reader.ReadVUInt32();
                    var length = reader.ReadVUInt32();
                    var start = reader.GetCurrentPosition();
                    if (length > (ulong)bytes.Length - (ulong)start) throw new InvalidDataException("Truncated inline TRL bytes.");
                    reader.SkipBlock(length);
                    (chunks ??= new()).Add(new(fileId, offset, bytes.AsMemory((int)start, (int)length)));
                }
                databases[i] = new(requested[i].Database, progress, published, chunks, schema);
            }
            return new ReplicationPeerPoll(challenge, granted, databases);
        });

    delegate T Reader<out T>(ref MemReader reader);

    static T Decode<T>(ReadOnlyMemory<byte> bytes, Reader<T> read)
    {
        try
        {
            using var controller = new ReadOnlyMemoryMemReader(bytes);
            var reader = new MemReader(controller);
            if (reader.ReadUInt8() != Version) throw new InvalidDataException("Unsupported peer protocol version.");
            var result = read(ref reader);
            if (!reader.Eof) throw new InvalidDataException("Trailing bytes in peer message.");
            return result;
        }
        catch (EndOfStreamException error) { throw new InvalidDataException("Truncated peer message.", error); }
        catch (OverflowException error) { throw new InvalidDataException("Invalid peer message.", error); }
    }

    static string RequiredString(ref MemReader reader) =>
        reader.ReadString() ?? throw new InvalidDataException("Missing peer message field.");
}
