using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer.ReadOnly;

sealed class ReadOnlyKeyValueDBTransaction : IKeyValueDBTransaction
{
    internal readonly ReadOnlyKeyValueDB Db;
    bool _disposed;

    public ReadOnlyKeyValueDBTransaction(ReadOnlyKeyValueDB db)
    {
        Db = db;
    }

    public void Dispose()
    {
        _disposed = true;
    }

    internal void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        Db.ThrowIfDisposed();
    }

    public IKeyValueDB Owner => Db;
    public DateTime CreatedTime { get; } = DateTime.UtcNow;
    public string? DescriptionForLeaks { get; set; }

    public IKeyValueDBCursor CreateCursor()
    {
        ThrowIfDisposed();
        return new ReadOnlyKeyValueDBCursor(this);
    }

    public long GetKeyValueCount() => Db.KeyValueCount;

    public bool IsWriting() => false;

    public bool IsReadOnly() => true;

    public bool IsDisposed() => _disposed;

    public ulong GetCommitUlong() => Db.CommitUlong;

    public void SetCommitUlong(ulong value)
    {
        if (value != Db.CommitUlong) throw ReadOnlyKeyValueDB.ReadOnlyException();
    }

    public uint GetUlongCount() => (uint)Db.Ulongs.Length;

    public ulong GetUlong(uint idx) => idx < Db.Ulongs.Length ? Db.Ulongs[idx] : 0;

    public void SetUlong(uint idx, ulong value)
    {
        if (GetUlong(idx) != value) throw ReadOnlyKeyValueDB.ReadOnlyException();
    }

    public void NextCommitTemporaryCloseTransactionLog()
    {
    }

    public void Commit()
    {
    }

    public long GetTransactionNumber() => 0;

    public bool RollbackAdvised { get; set; }

    public Dictionary<(uint Depth, uint Children), uint> CalcBTreeStats()
    {
        ThrowIfDisposed();
        var stats = new Dictionary<(uint Depth, uint Children), uint>();
        if (Db.RootOffset != 0) AddStats(stats, ReadOnlyKeyValueDBCursor.Node.Read(Db, Db.RootOffset), 0);
        return stats;
    }

    void AddStats(Dictionary<(uint Depth, uint Children), uint> stats, in ReadOnlyKeyValueDBCursor.Node node,
        uint depth)
    {
        var key = (depth, (uint)node.ChildCount);
        stats[key] = stats.GetValueOrDefault(key) + 1;
        if (node.IsLeaf) return;
        for (var i = 0; i < node.ChildCount; i++)
        {
            AddStats(stats, ReadOnlyKeyValueDBCursor.Node.ReadChild(Db, node, i, depth + 1), depth + 1);
        }
    }

    public IKeyValueDBCursor? FirstCursor { get; set; }
    public IKeyValueDBCursor? LastCursor { get; set; }
}
