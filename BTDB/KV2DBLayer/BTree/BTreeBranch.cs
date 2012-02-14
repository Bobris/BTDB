using System;
using System.Diagnostics;
using BTDB.Buffer;

namespace BTDB.KV2DBLayer.BTree
{
    internal class BTreeBranch : IBTreeNode
    {
        internal long TransactionId;
        byte[][] _keys;
        IBTreeNode[] _children;
        long[] _pairCounts;
        const int MaxChildren = 10;

        internal BTreeBranch(long transactionId, IBTreeNode node1, IBTreeNode node2)
        {
            TransactionId = transactionId;
            _children = new[] { node1, node2 };
            _keys = new[] { node2.GetLeftMostKey() };
            var leftCount = node1.CalcKeyCount();
            var rightCount = node2.CalcKeyCount();
            _pairCounts = new[] { leftCount, leftCount + rightCount };
        }

        BTreeBranch(long transactionId, byte[][] newKeys, IBTreeNode[] newChildren, long[] newPairCounts)
        {
            TransactionId = transactionId;
            _keys = newKeys;
            _children = newChildren;
            _pairCounts = newPairCounts;
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
            var newBranch = this;
            if (ctx.Split)
            {
                ctx.Split = false;
                var newKeys = new byte[_children.Length][];
                var newChildren = new IBTreeNode[_children.Length + 1];
                var newPairCounts = new long[_children.Length + 1];
                Array.Copy(_keys, 0, newKeys, 0, index);
                newKeys[index] = ctx.Node2.GetLeftMostKey();
                Array.Copy(_keys, index, newKeys, index + 1, _keys.Length - index);
                Array.Copy(_children, 0, newChildren, 0, index);
                newChildren[index] = ctx.Node1;
                newChildren[index + 1] = ctx.Node2;
                Array.Copy(_children, index + 1, newChildren, index + 2, _children.Length - index - 1);
                Array.Copy(_pairCounts, newPairCounts, index);
                var previousPairCount = index > 0 ? newPairCounts[index - 1] : 0;
                for (var i = index; i < newPairCounts.Length; i++)
                {
                    previousPairCount += newChildren[i].CalcKeyCount();
                    _pairCounts[i] = previousPairCount;
                }
                ctx.Node1 = null;
                ctx.Node2 = null;
                if (_children.Length < MaxChildren)
                {
                    if (TransactionId != ctx.TransactionId)
                    {
                        newBranch = new BTreeBranch(ctx.TransactionId, newKeys, newChildren, newPairCounts);
                        ctx.Node1 = newBranch;
                        ctx.Update = true;
                    }
                    else
                    {
                        _keys = newKeys;
                        _children = newChildren;
                        _pairCounts = newPairCounts;
                    }
                    if (ctx.SplitInRight) index++;
                    ctx.Stack[ctx.Depth] = new NodeIdxPair { Node = newBranch, Idx = index };
                    return;
                }
                if (ctx.SplitInRight) index++;
                ctx.Split = true;

                var keyCountLeft = (newChildren.Length + 1) / 2;
                var keyCountRight = newChildren.Length + 1 - keyCountLeft;

                var splitKeys = new byte[keyCountLeft - 1][];
                var splitChildren = new IBTreeNode[keyCountLeft];
                var splitPairCounts = new long[keyCountLeft];
                Array.Copy(newKeys, splitKeys, splitKeys.Length);
                Array.Copy(newChildren, splitChildren, splitChildren.Length);
                Array.Copy(newPairCounts, splitPairCounts, splitPairCounts.Length);
                ctx.Node1 = new BTreeBranch(ctx.TransactionId, splitKeys, splitChildren, splitPairCounts);

                splitKeys = new byte[keyCountRight - 1][];
                splitChildren = new IBTreeNode[keyCountRight];
                splitPairCounts = new long[keyCountRight];
                Array.Copy(newKeys, keyCountLeft, splitKeys, 0, splitKeys.Length);
                Array.Copy(newChildren, keyCountLeft, splitChildren, 0, splitChildren.Length);
                Array.Copy(newPairCounts, keyCountLeft, splitPairCounts, 0, splitPairCounts.Length);
                ctx.Node2 = new BTreeBranch(ctx.TransactionId, splitKeys, splitChildren, splitPairCounts);

                if (index < keyCountLeft)
                {
                    ctx.Stack[ctx.Depth] = new NodeIdxPair { Node = ctx.Node1, Idx = index };
                    ctx.SplitInRight = false;
                }
                else
                {
                    ctx.Stack[ctx.Depth] = new NodeIdxPair { Node = ctx.Node2, Idx = index - keyCountLeft };
                    ctx.SplitInRight = true;
                }
                return;
            }
            if (ctx.Update)
            {
                if (TransactionId != ctx.TransactionId)
                {
                    var newKeys = new byte[_keys.Length][];
                    var newChildren = new IBTreeNode[_children.Length];
                    var newPairCounts = new long[_children.Length];
                    Array.Copy(_keys, newKeys, _keys.Length);
                    Array.Copy(_children, newChildren, _children.Length);
                    _children[index] = ctx.Node1;
                    Array.Copy(_pairCounts, newPairCounts, _pairCounts.Length);
                    newBranch = new BTreeBranch(ctx.TransactionId, newKeys, newChildren, newPairCounts);
                    ctx.Node1 = newBranch;
                }
                else
                {
                    _children[index] = ctx.Node1;
                    ctx.Update = false;
                }
                ctx.Node1 = null;
                ctx.Stack[ctx.Depth] = new NodeIdxPair { Node = newBranch, Idx = index };
            }
            Debug.Assert(newBranch.TransactionId == ctx.TransactionId);
            if (!ctx.Created) return;
            var pairCounts = newBranch._pairCounts;
            for (var i = index; i < pairCounts.Length; i++)
            {
                pairCounts[i]++;
            }
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