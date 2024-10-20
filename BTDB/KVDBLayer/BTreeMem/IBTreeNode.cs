using System;
using System.Collections.Generic;
using BTDB.Collections;

namespace BTDB.KVDBLayer.BTreeMem;

interface IBTreeNode
{
    void CreateOrUpdate(ref CreateOrUpdateCtx ctx);
    void UpdateKeySuffix(ref UpdateKeySuffixCtx ctx);
    FindResult FindKey(ref StructList<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key);
    long CalcKeyCount();
    byte[] GetLeftMostKey();
    void FillStackByIndex(ref StructList<NodeIdxPair> stack, long keyIndex);
    long FindLastWithPrefix(in ReadOnlySpan<byte> prefix);
    bool NextIdxValid(int idx);
    void FillStackByLeftMost(ref StructList<NodeIdxPair> stack, int i);
    void FillStackByRightMost(ref StructList<NodeIdxPair> stack, int i);
    int GetLastChildrenIdx();
    IBTreeNode EraseRange(long transactionId, long firstKeyIndex, long lastKeyIndex);
    IBTreeNode EraseOne(long transactionId, long keyIndex);
    void CalcBTreeStats(RefDictionary<(uint Depth, uint Children), uint> stats, uint depth);
}
