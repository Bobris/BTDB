using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.LSM.Storage;

public enum LsmWriteOutcome
{
    /// The object was created by this request.
    Applied,

    /// The key already exists; for the writer this means it was fenced.
    Rejected,

    /// The request may or may not have taken effect; read the object back before continuing.
    Ambiguous
}

public readonly record struct LsmObjectInfo(string Key, long Length);

/// Object storage holding immutable BTDB.LSM objects (ImplementationPlan.md, section 5). Every object is created once
/// and never modified; only deletion removes it.
public interface ILsmStorage
{
    /// Creates an immutable object if the key does not exist yet.
    ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation);

    /// Reads a whole small object; returns null if it does not exist.
    ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation);

    /// Reads exactly destination.Length bytes starting at offset; throws FileNotFoundException if the object does not
    /// exist and EndOfStreamException if it ends before the range does.
    ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation);

    /// Synchronous ranged read for cursor cache misses, with the same exceptions; the cursor API is synchronous.
    void ReadRange(string key, long offset, Span<byte> destination);

    /// Lists objects whose key starts with prefix, in ascending key order.
    IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation);

    ValueTask DeleteAsync(string key, CancellationToken cancellation);

    /// Creates or replaces a small mutable object; only the reader registry uses it, each reader for its own key.
    ValueTask PutAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation);

    /// Acquires the exclusive lease on key (creating the object if needed) for duration; false while another holder
    /// owns it. Acquiring again with the same leaseId extends it.
    ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation);

    /// Extends a held lease; false when the lease was lost.
    ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation);

    ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation);
}
