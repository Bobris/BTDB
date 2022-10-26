using System;
using System.Collections.Generic;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.BTree;

delegate BTreeLeafMember BuildTreeCallback(ref SpanReader reader);
interface IBTreeRootNode : IBTreeNode, IRootNodeInternal
{
    long TransactionId { get; }
    string? DescriptionForLeaks { get; set; }
    uint TrLogFileId { get; set; }
    uint TrLogOffset { get; set; }
    int UseCount { get; set; }
    ulong CommitUlong { get; set; }
    ulong[]? UlongsArray { get; set; }
    ulong GetUlong(uint idx);
    void SetUlong(uint idx, ulong value);
    IBTreeRootNode NewTransactionRoot();
    IBTreeRootNode CloneRoot();
    FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key, uint prefixLen);
    void EraseOne(long keyIndex);
    void EraseRange(long firstKeyIndex, long lastKeyIndex);
    bool FindNextKey(List<NodeIdxPair> stack);
    bool FindPreviousKey(List<NodeIdxPair> stack);
    void BuildTree(long keyCount, ref SpanReader reader, BuildTreeCallback memberGenerator);
    new void ReplaceValues(ReplaceValuesCtx ctx);
}
