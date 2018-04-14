using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer.BTree
{
    interface IBTreeRootNode : IBTreeNode
    {
        long TransactionId { get; }
        string DescriptionForLeaks { get; set; }
        uint TrLogFileId { get; set; }
        uint TrLogOffset { get; set; }
        int UseCount { get; set; }
        ulong CommitUlong { get; set; }
        ulong[] UlongsArray { get; set; }
        ulong GetUlong(uint idx);
        void SetUlong(uint idx, ulong value);
        IBTreeRootNode NewTransactionRoot();
        IBTreeRootNode CloneRoot();
        void EraseRange(long firstKeyIndex, long lastKeyIndex);
        bool FindNextKey(List<NodeIdxPair> stack);
        bool FindPreviousKey(List<NodeIdxPair> stack);
        void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator);
        void RemappingIterate(BTreeRemappingIterateAction action);
        new void ReplaceValues(ReplaceValuesCtx ctx);
    }
}