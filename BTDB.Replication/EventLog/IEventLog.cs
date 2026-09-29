using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>An embedded ordered log of opaque records, grouped into independent named topics.
/// See BTDB.Replication/EventLogImplementationPlan.md for the contract.</summary>
public interface IEventLog
{
    /// <summary>Any valid name addresses a topic; a topic without records is empty. Names are 1–100 characters
    /// from [a-z0-9-_.].</summary>
    IEventTopic GetTopic(string name);
}

public interface IEventTopic
{
    string Name { get; }

    /// <summary>Durably append one record and return its offset. The record is copied before the call returns, so
    /// the caller may reuse its buffer at once. Publications that one process starts on one topic, each after the
    /// previous call returned, commit in call order. Cancellation or failure after dispatch means an unknown outcome,
    /// never proof that the record was not stored; a committed record is stored exactly once.</summary>
    ValueTask<ulong> PublishAsync(ReadOnlyMemory<byte> record, CancellationToken cancellation = default);

    /// <summary>First retained offset and durable next offset. Next covers every receipt completed before the call
    /// started.</summary>
    ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default);

    /// <summary>Records with consecutive offsets from <paramref name="start"/>; <paramref name="end"/> is exclusive and
    /// null follows live records until cancellation. A start below First or above the durable next offset fails with
    /// <see cref="EventLogOffsetNotAvailableException"/>. A payload is valid until the enumerator advances.</summary>
    IAsyncEnumerable<EventLogRecord> ReadAsync(ulong start, ulong? end = null, CancellationToken cancellation = default);
}

public readonly record struct EventLogBounds(ulong First, ulong Next);

public readonly record struct EventLogRecord(ulong Offset, ReadOnlyMemory<byte> Payload);

public sealed class EventLogOffsetNotAvailableException(string message) : Exception(message);

/// <summary>Committed topic content that does not match the format: never reinterpreted or truncated.</summary>
public sealed class EventLogCorruptedException(string message) : Exception(message);

/// <summary>A record above the configured maximum record size, rejected before acceptance.</summary>
public sealed class EventLogRecordTooLargeException(string message) : Exception(message);
