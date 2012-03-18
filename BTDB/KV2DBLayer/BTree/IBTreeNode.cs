using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.KV2DBLayer.BTree
{
    internal delegate void BTreeIterateAction(ref BTreeLeafMember member);

    internal interface IBTreeNode
    {
        void CreateOrUpdate(CreateOrUpdateCtx ctx);
        FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, byte[] prefix, ByteBuffer key);
        long CalcKeyCount();
        byte[] GetLeftMostKey();
        void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex);
        long FindLastWithPrefix(byte[] prefix);
        bool NextIdxValid(int idx);
        void FillStackByLeftMost(List<NodeIdxPair> stack, int i);
        void FillStackByRightMost(List<NodeIdxPair> stack, int i);
        int GetLastChildrenIdx();
        IBTreeNode EraseRange(long transactionId, long firstKeyIndex, long lastKeyIndex);
        void Iterate(BTreeIterateAction action);
    }
}