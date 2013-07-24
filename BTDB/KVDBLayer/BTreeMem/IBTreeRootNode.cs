using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer.BTreeMem
{
    internal interface IBTreeRootNode : IBTreeNode
    {
        long TransactionId { get; }
        IBTreeRootNode NewTransactionRoot();
        void EraseRange(long firstKeyIndex, long lastKeyIndex);
        bool FindNextKey(List<NodeIdxPair> stack);
        bool FindPreviousKey(List<NodeIdxPair> stack);
        void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator);
    }
}