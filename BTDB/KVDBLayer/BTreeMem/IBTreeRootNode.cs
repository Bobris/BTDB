using System;
using BTDB.Collections;

namespace BTDB.KVDBLayer.BTreeMem;

interface IBTreeRootNode : IBTreeNode
{
    long TransactionId { get; }
    string? DescriptionForLeaks { get; set; }
    ulong CommitUlong { get; set; }
    uint GetUlongCount();
    ulong GetUlong(uint idx);
    void SetUlong(uint idx, ulong value);
    IBTreeRootNode NewTransactionRoot();
    void EraseRange(long firstKeyIndex, long lastKeyIndex);
    FindResult FindKey(ref StructList<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key, uint prefixLen);
    bool FindNextKey(ref StructList<NodeIdxPair> stack);
    bool FindPreviousKey(ref StructList<NodeIdxPair> stack);
    void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator);

    void FastIterate(ref StructList<NodeIdxPair> stack, ref long keyIndex, ref Span<byte> buffer,
        CursorIterateCallback callback);
}
