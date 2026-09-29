using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>Consecutive records of one publisher session, starting at <see cref="FirstSequence"/>. Sequences below
/// <see cref="OldestUnresolved"/> have receipts. When <see cref="PreviouslyDispatched"/> is set, some records may already
/// be committed at offsets from <see cref="ResumeFrom"/>, and the owner finds them instead of appending them again.</summary>
public sealed record EventLogSubmitRequest(string Topic, ulong Session, ulong FirstSequence, ulong OldestUnresolved,
    bool PreviouslyDispatched, ulong ResumeFrom, IReadOnlyList<ReadOnlyMemory<byte>> Records);

public enum EventLogSubmitStatus
{
    /// <summary>Every record is durable; <see cref="EventLogSubmitResponse.Offsets"/> lists their offsets.</summary>
    Committed,
    /// <summary>The receiver does not own the topic; <see cref="EventLogSubmitResponse.OwnerEndpoint"/> may name the owner.</summary>
    NotOwner,
    /// <summary>A predecessor sequence is missing; resend from the oldest unresolved record.</summary>
    SequenceGap,
}

public sealed record EventLogSubmitResponse(EventLogSubmitStatus Status, IReadOnlyList<ulong> Offsets, ulong DurableNext,
    string? OwnerEndpoint);

public sealed record EventLogBoundsResponse(bool IsOwner, EventLogBounds Bounds, string? OwnerEndpoint);

public enum EventLogLiveKind
{
    /// <summary>A committed batch starting at <see cref="EventLogLiveMessage.Offset"/>.</summary>
    Batch,
    /// <summary>The owner validated its tail; <see cref="EventLogLiveMessage.Offset"/> is its durable next offset.</summary>
    Heartbeat,
    /// <summary>The requested offset is not in the owner's cache: read storage up to <see cref="EventLogLiveMessage.Offset"/>
    /// and subscribe again.</summary>
    CatchUp,
    /// <summary>The sender does not own the topic (any more); <see cref="EventLogLiveMessage.OwnerEndpoint"/> may name the owner.</summary>
    NotOwner,
}

public sealed record EventLogLiveMessage(EventLogLiveKind Kind, ulong Offset, IReadOnlyList<ReadOnlyMemory<byte>> Records,
    string? TailVersion, string? OwnerEndpoint)
{
    public static EventLogLiveMessage NotOwner(string? owner) => new(EventLogLiveKind.NotOwner, 0, [], null, owner);
}

/// <summary>Serves this node's owned topics to peers; the HTTP host exposes it.</summary>
public interface IEventLogPeerHandler
{
    ValueTask<EventLogSubmitResponse> SubmitAsync(EventLogSubmitRequest request, CancellationToken cancellation);
    ValueTask<EventLogBoundsResponse> GetBoundsAsync(string topic, CancellationToken cancellation);
    IAsyncEnumerable<EventLogLiveMessage> SubscribeAsync(string topic, ulong from, CancellationToken cancellation);
}

/// <summary>Reaches another node's <see cref="IEventLogPeerHandler"/>. Any exception means the peer is unreachable for
/// this call; a dispatched submission may still have taken effect.</summary>
public interface IEventLogPeerTransport
{
    ValueTask<EventLogSubmitResponse> SubmitAsync(string endpoint, EventLogSubmitRequest request,
        CancellationToken cancellation);
    ValueTask<EventLogBoundsResponse> GetBoundsAsync(string endpoint, string topic, CancellationToken cancellation);
    IAsyncEnumerable<EventLogLiveMessage> SubscribeAsync(string endpoint, string topic, ulong from,
        CancellationToken cancellation);
}
