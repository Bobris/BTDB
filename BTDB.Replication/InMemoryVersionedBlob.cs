using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>A fault injected into the next write of an <see cref="InMemoryVersionedBlob"/>.</summary>
internal enum VersionedBlobFault
{
    None,
    /// <summary>Reports Rejected without applying, like a lost race.</summary>
    Reject,
    /// <summary>Reports Ambiguous without applying, like a request that never reached the provider.</summary>
    AmbiguousNotApplied,
    /// <summary>Applies the write, then reports Ambiguous, like a lost response.</summary>
    AmbiguousApplied
}

/// <summary>A versioned blob in memory, for tests of a single process: share one instance between the nodes that share
/// it. Versions never repeat. All operations complete synchronously; content is copied in and out.</summary>
public sealed class InMemoryVersionedBlob : IVersionedBlob
{
    readonly Lock _lock = new();
    byte[]? _content;
    string? _version;
    long _versions;

    /// <summary>Applies to the next write only, then resets to <see cref="VersionedBlobFault.None"/>.</summary>
    internal VersionedBlobFault NextWriteFault { get; set; }

    public int Reads { get; private set; }
    public int Writes { get; private set; }

    public ValueTask<VersionedBlobContent?> ReadAsync(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Reads++;
            return ValueTask.FromResult(_version == null ? null
                : new VersionedBlobContent(_version, _content!.AsSpan().ToArray()));
        }
    }

    public ValueTask<VersionedBlobWriteResult> WriteAsync(string? expectedVersion, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Writes++;
            var fault = NextWriteFault;
            NextWriteFault = VersionedBlobFault.None;
            switch (fault)
            {
                case VersionedBlobFault.Reject: return ValueTask.FromResult(VersionedBlobWriteResult.Rejected);
                case VersionedBlobFault.AmbiguousNotApplied: return ValueTask.FromResult(VersionedBlobWriteResult.Ambiguous);
            }
            if (expectedVersion != _version) return ValueTask.FromResult(VersionedBlobWriteResult.Rejected);
            _content = content.ToArray();
            _version = (++_versions).ToString("x", CultureInfo.InvariantCulture);
            return ValueTask.FromResult(fault == VersionedBlobFault.AmbiguousApplied
                ? VersionedBlobWriteResult.Ambiguous
                : new VersionedBlobWriteResult(VersionedBlobWriteOutcome.Applied, _version));
        }
    }
}
