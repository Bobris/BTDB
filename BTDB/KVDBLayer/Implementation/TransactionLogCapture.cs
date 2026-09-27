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

    public TransactionLogPosition Completed { get { lock (_lock) return _completed; } }
    public TransactionLogPosition Acknowledged { get { lock (_lock) return _acknowledged; } }
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
