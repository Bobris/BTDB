using System;
using System.Threading;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.BTree;
using BTDB.StreamLayer;

namespace BTDB.BTreeLib;

public interface IRootNode : IRootNodeInternal, IDisposable
{
    // Return true if it should be disposed
    bool Reference();
    bool Dereference();
    bool ShouldBeDisposed { get; }

    IRootNode? Next { get; set; }

    ulong CommitUlong { get; set; }
    long TransactionId { get; set; }
    uint TrLogFileId { get; set; }
    uint TrLogOffset { get; set; }
    string? DescriptionForLeaks { get; set; }

    IRootNode Snapshot();
    IRootNode CreateWritableTransaction();
    void Commit();
    void RevertTo(IRootNode snapshot);
    ICursor CreateCursor();
    long GetCount();
    ulong GetUlong(uint idx);
    void SetUlong(uint idx, ulong value);
    uint GetUlongCount();
    ulong[]? UlongsArray { get; }

    void ValuesIterate(ValuesIterateAction visit);
    void KeyValueIterate(ref KeyValueIterateCtx keyValueIterateCtx, KeyValueIterateCallback callback);
    void CalcBTreeStats(RefDictionary<(uint Depth, uint Children), uint> stats, uint depth);
}

public ref struct KeyValueIterateCtx
{
    public CancellationToken CancellationToken;
    public Span<byte> PreviousPrefix;
    public Span<byte> PreviousSuffix;
    public Span<byte> CurrentPrefix;
    public Span<byte> CurrentSuffix;
    public Span<byte> CurrentValue;
    public SpanWriter Writer;
    public uint PreviousCurrentCommonLength;

    public void CalcCommonLength()
    {
        PreviousCurrentCommonLength = (uint)
            TreeNodeUtils.FindFirstDifference(PreviousPrefix, PreviousSuffix, CurrentPrefix, CurrentSuffix);
    }

    public void CalcCommonLengthWithIdenticalPrefixes()
    {
        PreviousCurrentCommonLength =
            (uint)(CurrentPrefix.Length + TreeNodeUtils.FindFirstDifference(PreviousSuffix, CurrentSuffix));
    }
}

public delegate void KeyValueIterateCallback(ref KeyValueIterateCtx keyValueIterateCtx);
