using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer.BTree
{
    interface IBTreeRootNode : IBTreeNode
    {
        long TransactionId { get; }
        uint TrLogFileId { get; set; }
        uint TrLogOffset { get; set; }
        int UseCount { get; set; }
        ulong CommitUlong { get; set; }
        IBTreeRootNode NewTransactionRoot();
        IBTreeRootNode CloneRoot();
        void EraseRange(long firstKeyIndex, long lastKeyIndex);
        bool FindNextKey(List<NodeIdxPair> stack);
        bool FindPreviousKey(List<NodeIdxPair> stack);
        void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator);
        void RemappingIterate(BTreeRemappingIterateAction action);
    }
}