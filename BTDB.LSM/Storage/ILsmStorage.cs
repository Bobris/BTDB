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

    /// Reads exactly destination.Length bytes starting at offset.
    ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation);

    /// Synchronous ranged read for cursor cache misses; the cursor API is synchronous.
    void ReadRange(string key, long offset, Span<byte> destination);

    /// Lists objects whose key starts with prefix, in ascending key order.
    IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation);

    ValueTask DeleteAsync(string key, CancellationToken cancellation);
}
