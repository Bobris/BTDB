using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>One consistent version of a versioned blob. Versions are opaque tokens, not ordered terms.</summary>
public sealed record VersionedBlobContent(string Version, ReadOnlyMemory<byte> Content);

public enum VersionedBlobWriteOutcome { Applied, Rejected, Ambiguous }

/// <summary>Version is the new version of an applied write and null otherwise.</summary>
public readonly record struct VersionedBlobWriteResult(VersionedBlobWriteOutcome Outcome, string? Version = null)
{
    public static VersionedBlobWriteResult Rejected => new(VersionedBlobWriteOutcome.Rejected);
    public static VersionedBlobWriteResult Ambiguous => new(VersionedBlobWriteOutcome.Ambiguous);
}

/// <summary>
/// A single blob read whole and replaced whole under a version condition, for small shared state such as Layer2 event
/// metadata. A write installs the complete content atomically. Implementations never retry a conditional write
/// transparently: a timeout, transport error or lost response is <see cref="VersionedBlobWriteOutcome.Ambiguous"/>
/// because it may still land; reconcile by reading.
/// </summary>
public interface IVersionedBlob
{
    /// <summary>The current version, or null when the blob does not exist. Throws on transport errors.</summary>
    ValueTask<VersionedBlobContent?> ReadAsync(CancellationToken cancellation);

    /// <summary>Replace the blob when its version equals <paramref name="expectedVersion"/>, or create it when
    /// <paramref name="expectedVersion"/> is null and it does not exist.</summary>
    ValueTask<VersionedBlobWriteResult> WriteAsync(string? expectedVersion, ReadOnlyMemory<byte> content,
        CancellationToken cancellation);
}
