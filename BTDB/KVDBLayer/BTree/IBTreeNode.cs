using System;
using System.Collections.Generic;
using BTDB.Collections;

namespace BTDB.KVDBLayer.BTree;

public delegate void ValuesIterateAction(uint valueFileId, uint valueOfs, int valueSize);

interface IBTreeNode
{
    void CreateOrUpdate(ref CreateOrUpdateCtx ctx);
    FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key);
    long CalcKeyCount();
    byte[] GetLeftMostKey();
    void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex);
    long FindLastWithPrefix(in ReadOnlySpan<byte> prefix);
    bool NextIdxValid(int idx);
    void FillStackByLeftMost(List<NodeIdxPair> stack, int i);
    void FillStackByRightMost(List<NodeIdxPair> stack, int i);
    int GetLastChildrenIdx();
    IBTreeNode EraseRange(long transactionId, long firstKeyIndex, long lastKeyIndex);
    IBTreeNode EraseOne(long transactionId, long keyIndex);
    void Iterate(ValuesIterateAction action);
    IBTreeNode ReplaceValues(ReplaceValuesCtx ctx);
    void CalcBTreeStats(RefDictionary<(uint Depth, uint Children),uint> stats, uint depth);
}
