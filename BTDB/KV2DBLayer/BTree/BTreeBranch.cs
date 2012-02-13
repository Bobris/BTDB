using System;
using BTDB.Buffer;

namespace BTDB.KV2DBLayer.BTree
{
    internal class BTreeBranch : IBTreeNode
    {
        internal long TransactionId;
        byte[][] _keys;
        IBTreeNode[] _children;
        long[] _pairCounts;

        internal BTreeBranch(long transactionId, IBTreeNode node1, IBTreeNode node2)
        {
            TransactionId = transactionId;
            _children = new[] { node1, node2 };
            _keys = new[] { node2.GetLeftMostKey() };
            var leftCount = node1.CalcKeyCount();
            var rightCount = node2.CalcKeyCount();
            _pairCounts = new[] {leftCount, leftCount + rightCount};
        }

        int Find(byte[] prefix, ByteBuffer key)
        {
            var left = 0;
            var right = _keys.Length;
            while (left < right)
            {
                var middle = (left + right) / 2;
                var currentKey = _keys[middle];
                var result = BitArrayManipulation.CompareByteArray(prefix, 0, prefix.Length,
                                                                   currentKey, 0, Math.Min(currentKey.Length, prefix.Length));
                if (result == 0)
                {
                    result = BitArrayManipulation.CompareByteArray(key.Buffer, key.Offset, key.Length,
                                                                   currentKey, prefix.Length, currentKey.Length - prefix.Length);
                }
                if (result <= 0)
                {
                    right = middle;
                }
                else
                {
                    left = middle + 1;
                }
            }
            return left;
        }

        public void CreateOrUpdate(CreateOrUpdateCtx ctx)
        {
            var index = Find(ctx.KeyPrefix, ctx.Key);
            ctx.Stack.Add(new NodeIdxPair { Node = this, Idx = index });
            ctx.Depth++;
            _children[index].CreateOrUpdate(ctx);
            ctx.Depth--;

            throw new NotImplementedException();
        }

        public long CalcKeyCount()
        {
            return _pairCounts[_pairCounts.Length - 1];
        }

        public byte[] GetLeftMostKey()
        {
            return _children[0].GetLeftMostKey();
        }
    }
}