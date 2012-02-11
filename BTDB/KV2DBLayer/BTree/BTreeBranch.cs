using System;

namespace BTDB.KV2DBLayer.BTree
{
    internal class BTreeBranch : IBTreeNode
    {
        long _transactionId;
        byte[][] _keys;
        IBTreeNode[] _children;
        long[] _pairCounts;

        internal static IBTreeNode CreateFromTwo(IBTreeNode node1, IBTreeNode node2)
        {

            throw new NotImplementedException();
        }

        public void CreateOrUpdate(CreateOrUpdateCtx ctx)
        {
            throw new NotImplementedException();
        }

        public long CalcKeyCount()
        {
            throw new NotImplementedException();
        }
    }
}