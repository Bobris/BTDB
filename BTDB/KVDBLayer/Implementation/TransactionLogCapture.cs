using System;

namespace BTDB.KVDBLayer;

/// <summary>A native TRL byte position, ordered by file ID and then offset.</summary>
public readonly record struct TransactionLogPosition(uint FileId, uint Offset) : IComparable<TransactionLogPosition>
{
    public int CompareTo(TransactionLogPosition other)
    {
        var result = FileId.CompareTo(other.FileId);
        return result != 0 ? result : Offset.CompareTo(other.Offset);
    }

    public static bool operator <(TransactionLogPosition left, TransactionLogPosition right) => left.CompareTo(right) < 0;
    public static bool operator >(TransactionLogPosition left, TransactionLogPosition right) => left.CompareTo(right) > 0;
    public static bool operator <=(TransactionLogPosition left, TransactionLogPosition right) => left.CompareTo(right) <= 0;
    public static bool operator >=(TransactionLogPosition left, TransactionLogPosition right) => left.CompareTo(right) >= 0;
}

/// <summary>
/// Latest complete local TRL prefix and the prefix acknowledged by its consumer. Constant memory; no index files.
/// Acknowledge only after publication or byte comparison finishes. The acknowledged tail remains available for append.
/// </summary>
public sealed class TransactionLogCapture
{
    readonly object _lock = new();
    TransactionLogPosition _completed;
    TransactionLogPosition _acknowledged;
    TransactionLogPosition _nonApplicationCommitted;

    public TransactionLogPosition Completed { get { lock (_lock) return _completed; } }
    public TransactionLogPosition Acknowledged { get { lock (_lock) return _acknowledged; } }

    /// <summary>End of the latest committed transaction that left CommitUlong unchanged, observed while replaying the
    /// opened TRL or committing locally; default until one is observed. Rollbacks never count. It is recorded before
    /// Completed covers the commit, so reading it after a completed cut covers every such commit through that cut.</summary>
    public TransactionLogPosition NonApplicationCommitted { get { lock (_lock) return _nonApplicationCommitted; } }
    internal uint OldestRequiredFileId { get { lock (_lock) return _acknowledged.FileId; } }

    internal void Initialize(IFileCollectionWithFileInfos files)
    {
        foreach (var (id, type) in files.FileTypes)
            if (type == KVFileType.TransactionLog &&
                (_acknowledged.FileId == 0 || id < _acknowledged.FileId))
                _acknowledged = new(id, 0);
    }

    internal void RetainFrom(uint fileId)
    {
        lock (_lock)
            if (_acknowledged.FileId == 0) _acknowledged = new(fileId, 0);
    }

    internal void Complete(uint fileId, uint offset)
    {
        lock (_lock) _completed = new(fileId, offset);
    }

    // Replaying a virtual batch observes the same commits again; keep the latest position.
    internal void CommitNonApplication(uint fileId, uint offset)
    {
        lock (_lock)
            if (new TransactionLogPosition(fileId, offset) > _nonApplicationCommitted)
                _nonApplicationCommitted = new(fileId, offset);
    }

    public void Acknowledge(TransactionLogPosition position)
    {
        lock (_lock)
        {
            if (position < _acknowledged || position > _completed)
                throw new ArgumentOutOfRangeException(nameof(position));
            _acknowledged = position;
        }
    }
}
