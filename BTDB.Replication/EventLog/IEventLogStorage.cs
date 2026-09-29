using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>The owner session recorded in a split's blob metadata; Endpoint routes publications to it.</summary>
public sealed record EventLogOwner(string Session, string Endpoint);

/// <summary>One consistent object version. Versions are opaque tokens (ETags), not ordered terms.</summary>
public sealed record EventLogBlob(string Version, ReadOnlyMemory<byte> Content, EventLogOwner? Owner);

public sealed record EventLogBlobProperties(string Version, long Length, EventLogOwner? Owner);

public sealed record EventLogObjectInfo(string Key, string Version, long Length, DateTimeOffset? DeleteAfter);

public enum EventLogWriteOutcome { Applied, Rejected, Ambiguous }

public readonly record struct EventLogWriteResult(EventLogWriteOutcome Outcome, string? Version = null)
{
    public static EventLogWriteResult Rejected => new(EventLogWriteOutcome.Rejected);
    public static EventLogWriteResult Ambiguous => new(EventLogWriteOutcome.Ambiguous);
}

/// <summary>
/// Object storage for event-log topics; keys are relative to the adapter's prefix. Conditional writes are atomic:
/// content and owner metadata are installed together, and a write replaces all metadata. Adapters never retry a
/// conditional mutation transparently: a timeout, transport error or lost response is <see cref="EventLogWriteOutcome.Ambiguous"/>
/// because it may still land. Reads throw on transport errors and return null for missing objects.
/// </summary>
public interface IEventLogStorage
{
    ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation);

    ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation);

    /// <summary>Read a range of exactly this version; a changed or missing version throws
    /// <see cref="EventLogVersionChangedException"/>.</summary>
    ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination,
        CancellationToken cancellation);

    /// <summary>Replace the object when its version equals <paramref name="expectedVersion"/>, or create it when
    /// <paramref name="expectedVersion"/> is null and it does not exist. Sha256, when set, is stored with the write.</summary>
    ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
        EventLogOwner? owner, string? sha256, CancellationToken cancellation);

    /// <summary>Content-preserving conditional owner change.</summary>
    ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner,
        CancellationToken cancellation);

    IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken cancellation);

    /// <summary>Persist a deletion deadline without extending an existing one; return the new version, or null when
    /// the object is missing or its version changed.</summary>
    ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay, CancellationToken cancellation);

    /// <summary>Delete only when the persisted deadline has elapsed and the version still matches.</summary>
    ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation);
}

public sealed class EventLogVersionChangedException(string key) : Exception($"Object '{key}' changed or disappeared.");
