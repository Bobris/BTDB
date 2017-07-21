using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer.BTreeMem
{
    interface IBTreeRootNode : IBTreeNode
    {
        long TransactionId { get; }
        string DescriptionForLeaks { get; set; }
        ulong CommitUlong { get; set; }
        uint GetUlongCount();
        ulong GetUlong(uint idx);
        void SetUlong(uint idx, ulong value);
        IBTreeRootNode NewTransactionRoot();
        void EraseRange(long firstKeyIndex, long lastKeyIndex);
        bool FindNextKey(List<NodeIdxPair> stack);
        bool FindPreviousKey(List<NodeIdxPair> stack);
        void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator);
    }
}