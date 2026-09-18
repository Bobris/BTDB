using System;

namespace BTDB.KVDBLayer;

public readonly record struct TransactionLogPosition(uint FileId, uint Offset);

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

    internal void Initialize(FileCollectionWithFileInfos files)
    {
        foreach (var (id, info) in files.FileInfos)
            if (info.FileType == KVFileType.TransactionLog &&
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
            static ulong Order(TransactionLogPosition p) => ((ulong)p.FileId << 32) | p.Offset;
            if (Order(position) < Order(_acknowledged) || Order(position) > Order(_completed))
                throw new ArgumentOutOfRangeException(nameof(position));
            _acknowledged = position;
        }
    }
}
