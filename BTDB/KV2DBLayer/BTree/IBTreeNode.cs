using System.Collections.Generic;
using BTDB.Buffer;

namespace BTDB.KV2DBLayer.BTree
{
    internal interface IBTreeNode
    {
        void CreateOrUpdate(CreateOrUpdateCtx ctx);
        FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, byte[] prefix, ByteBuffer key);
        long CalcKeyCount();
        byte[] GetLeftMostKey();
        void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex);
    }
}