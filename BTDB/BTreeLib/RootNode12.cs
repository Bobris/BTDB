using System;
using System.Threading;
using BTDB.Collections;
using BTDB.KVDBLayer.BTree;

namespace BTDB.BTreeLib;

class RootNode12 : IRootNode
{
    internal RootNode12(BTreeImpl12 impl)
    {
        Impl = impl;
        Root = IntPtr.Zero;
        Writable = true;
        _referenceCount = 1;
    }

    int _referenceCount;
    internal IntPtr Root;
    internal readonly BTreeImpl12 Impl;
    internal bool Writable;

    public IRootNode? Next { get; set; }

    public ulong CommitUlong { get; set; }
    public long TransactionId { get; set; }
    public uint TrLogFileId { get; set; }
    public uint TrLogOffset { get; set; }
    public string? DescriptionForLeaks { get; set; }

    public void Dispose()
    {
        Impl.Dereference(Root);
    }

    public IRootNode Snapshot()
    {
        var snapshot = new RootNode12(Impl)
        {
            Writable = false,
            Root = Root,
            CommitUlong = CommitUlong,
            TransactionId = TransactionId,
            TrLogFileId = TrLogFileId,
            TrLogOffset = TrLogOffset,
            _ulongs = (ulong[])_ulongs?.Clone()
        };
        if (Writable)
            TransactionId++;
        NodeUtils12.Reference(Root);
        return snapshot;
    }

    public IRootNode CreateWritableTransaction()
    {
        if (Writable) throw new InvalidOperationException("Only readonly root node could be CreateWritableTransaction");
        var node = new RootNode12(Impl)
        {
            Writable = true,
            Root = Root,
            CommitUlong = CommitUlong,
            TransactionId = TransactionId + 1,
            TrLogFileId = TrLogFileId,
            TrLogOffset = TrLogOffset,
            _ulongs = (ulong[])_ulongs?.Clone()
        };
        NodeUtils12.Reference(Root);
        return node;
    }

    public void Commit()
    {
        Writable = false;
    }

    public ICursor CreateCursor()
    {
        return new Cursor12(this);
    }

    public long GetCount()
    {
        if (Root == IntPtr.Zero) return 0;
        ref var header = ref NodeUtils12.Ptr2NodeHeader(Root);
        return (long)header.RecursiveChildCount;
    }

    public void RevertTo(IRootNode snapshot)
    {
        if (!Writable)
            throw new InvalidOperationException("Only writable root node could be reverted");
        var oldRoot = Root;
        Root = ((RootNode12)snapshot).Root;
        _ulongs = (ulong[])((RootNode12)snapshot)._ulongs?.Clone();
        CommitUlong = ((RootNode12)snapshot).CommitUlong;
        TransactionId = ((RootNode12)snapshot).TransactionId;
        TrLogFileId = ((RootNode12)snapshot).TrLogFileId;
        TrLogOffset = ((RootNode12)snapshot).TrLogOffset;
        if (oldRoot != Root)
        {
            NodeUtils12.Reference(Root);
            Impl.Dereference(oldRoot);
        }
    }

    ulong[]? _ulongs;

    public ulong GetUlong(uint idx)
    {
        if (_ulongs == null) return 0;
        if (idx >= _ulongs.Length) return 0;
        return _ulongs[idx];
    }

    public void SetUlong(uint idx, ulong value)
    {
        if (_ulongs == null || idx >= _ulongs.Length)
            Array.Resize(ref _ulongs, (int)(idx + 1));
        _ulongs[idx] = value;
    }

    public uint GetUlongCount()
    {
        return _ulongs == null ? 0U : (uint)_ulongs.Length;
    }

    public ulong[]? UlongsArray => _ulongs;

    public bool Reference()
    {
        while (true)
        {
            var original = Thread.VolatileRead(ref _referenceCount);
            if (original == 0)
                return true;
            if (Interlocked.CompareExchange(ref _referenceCount, original + 1, original) == original)
            {
                return false;
            }
        }
    }

    public bool Dereference()
    {
        return Interlocked.Decrement(ref _referenceCount) == 0;
    }

    public void ValuesIterate(ValuesIterateAction visit)
    {
        if (Root == IntPtr.Zero)
            return;
        BTreeImpl12.ValuesIterate(Root, visit);
    }

    public void KeyValueIterate(ref KeyValueIterateCtx keyValueIterateCtx, KeyValueIterateCallback callback)
    {
        if (Root == IntPtr.Zero)
            return;
        BTreeImpl12.KeyValueIterate(Root, ref keyValueIterateCtx, callback);
    }

    public void CalcBTreeStats(RefDictionary<(uint Depth, uint Children), uint> stats, uint depth)
    {
        if (Root == IntPtr.Zero)
            return;
        BTreeImpl12.CalcBTreeStats(Root, stats, 0u);
    }

    public bool ShouldBeDisposed => _referenceCount == 0;
}
