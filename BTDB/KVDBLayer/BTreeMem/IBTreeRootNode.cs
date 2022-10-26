using System;
using System.Collections.Generic;
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
    FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key, uint prefixLen);
    bool FindNextKey(List<NodeIdxPair> stack);
    bool FindPreviousKey(List<NodeIdxPair> stack);
    void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator);
}
