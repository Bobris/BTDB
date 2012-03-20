using System;
using System.Collections.Generic;

namespace BTDB.KV2DBLayer.BTree
{
    internal interface IBTreeRootNode : IBTreeNode
    {
        long TransactionId { get; }
        uint TrLogFileId { get; set; }
        uint TrLogOffset { get; set; }
        IBTreeRootNode NewTransactionRoot();
        void EraseRange(long firstKeyIndex, long lastKeyIndex);
        bool FindNextKey(List<NodeIdxPair> stack);
        bool FindPreviousKey(List<NodeIdxPair> stack);
        void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator);
        void RemappingIterate(BTreeRemappingIterateAction action);
    }
}