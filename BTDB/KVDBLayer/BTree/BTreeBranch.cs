using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTree
{
    class BTreeBranch : IBTreeNode
    {
        internal readonly long TransactionId;
        byte[][] _keys;
        IBTreeNode[] _children;
        long[] _pairCounts;
        internal const int MaxChildren = 30;

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

        public BTreeBranch(long transactionId, int count, Func<IBTreeNode> generator)
        {
            Debug.Assert(count > 0 && count <= MaxChildren);
            TransactionId = transactionId;
            _keys = new byte[count - 1][];
            _pairCounts = new long[count];
            _children = new IBTreeNode[count];
            long pairs = 0;
            for (var i = 0; i < _children.Length; i++)
            {
                var child = generator();
                _children[i] = child;
                pairs += child.CalcKeyCount();
                _pairCounts[i] = pairs;
                if (i > 0)
                {
                    _keys[i - 1] = child.GetLeftMostKey();
                }
            }
        }

        int Find(byte[] prefix, ByteBuffer key)
        {
            var left = 0;
            var right = _keys.Length;
            while (left < right)
            {
                var middle = (left + right) / 2;
                var currentKey = _keys[middle];
                var result = BitArrayManipulation.CompareByteArray(prefix, prefix.Length,
                                                                   currentKey, Math.Min(currentKey.Length, prefix.Length));
                if (result == 0)
                {
                    result = BitArrayManipulation.CompareByteArray(key.Buffer, key.Offset, key.Length,
                                                                   currentKey, prefix.Length, currentKey.Length - prefix.Length);
                }
                if (result < 0)
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
                    newPairCounts[i] = previousPairCount;
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
                var keyCountRight = newChildren.Length - keyCountLeft;

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
                for (int i = 0; i < splitPairCounts.Length; i++)
                {
                    splitPairCounts[i] = newPairCounts[keyCountLeft + i] - newPairCounts[keyCountLeft - 1];
                }
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
                    newChildren[index] = ctx.Node1;
                    Array.Copy(_pairCounts, newPairCounts, _pairCounts.Length);
                    newBranch = new BTreeBranch(ctx.TransactionId, newKeys, newChildren, newPairCounts);
                    ctx.Node1 = newBranch;
                }
                else
                {
                    _children[index] = ctx.Node1;
                    ctx.Update = false;
                    ctx.Node1 = null;
                }
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

        public FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, byte[] prefix, ByteBuffer key)
        {
            var idx = Find(prefix, key);
            stack.Add(new NodeIdxPair { Node = this, Idx = idx });
            var result = _children[idx].FindKey(stack, out keyIndex, prefix, key);
            if (idx > 0) keyIndex += _pairCounts[idx - 1];
            return result;
        }

        public long CalcKeyCount()
        {
            return _pairCounts[_pairCounts.Length - 1];
        }

        public byte[] GetLeftMostKey()
        {
            return _children[0].GetLeftMostKey();
        }

        public void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex)
        {
            var left = 0;
            var right = _pairCounts.Length - 1;
            while (left < right)
            {
                var middle = (left + right) / 2;
                var currentIndex = _pairCounts[middle];
                if (keyIndex < currentIndex)
                {
                    right = middle;
                }
                else
                {
                    left = middle + 1;
                }
            }
            stack.Add(new NodeIdxPair { Node = this, Idx = left });
            _children[left].FillStackByIndex(stack, keyIndex - (left > 0 ? _pairCounts[left - 1] : 0));
        }

        public long FindLastWithPrefix(byte[] prefix)
        {
            var left = 0;
            var right = _keys.Length;
            while (left < right)
            {
                var middle = (left + right) / 2;
                var currentKey = _keys[middle];
                var result = BitArrayManipulation.CompareByteArray(prefix, prefix.Length,
                                                                   currentKey, Math.Min(currentKey.Length, prefix.Length));
                if (result < 0)
                {
                    right = middle;
                }
                else
                {
                    left = middle + 1;
                }
            }
            return _children[left].FindLastWithPrefix(prefix) + (left > 0 ? _pairCounts[left - 1] : 0);
        }

        public bool NextIdxValid(int idx)
        {
            return idx + 1 < _children.Length;
        }

        public void FillStackByLeftMost(List<NodeIdxPair> stack, int idx)
        {
            var leftMost = _children[idx];
            stack.Add(new NodeIdxPair { Node = leftMost, Idx = 0 });
            leftMost.FillStackByLeftMost(stack, 0);
        }

        public void FillStackByRightMost(List<NodeIdxPair> stack, int idx)
        {
            var rightMost = _children[idx];
            var lastIdx = rightMost.GetLastChildrenIdx();
            stack.Add(new NodeIdxPair { Node = rightMost, Idx = lastIdx });
            rightMost.FillStackByRightMost(stack, lastIdx);
        }

        public int GetLastChildrenIdx()
        {
            return _children.Length - 1;
        }

        public IBTreeNode EraseRange(long transactionId, long firstKeyIndex, long lastKeyIndex)
        {

            int firstRemoved = -1;
            int lastRemoved = -1;
            IBTreeNode firstPartialNode = null;
            IBTreeNode lastPartialNode = null;

            for (int i = 0; i < _pairCounts.Length; i++)
            {
                var prevPairCount = i > 0 ? _pairCounts[i - 1] : 0;
                if (lastKeyIndex < prevPairCount) break;
                var nextPairCount = _pairCounts[i];
                if (nextPairCount <= firstKeyIndex) continue;
                if (firstKeyIndex <= prevPairCount && nextPairCount - 1 <= lastKeyIndex)
                {
                    if (firstRemoved == -1) firstRemoved = i;
                    lastRemoved = i;
                    continue;
                }
                if (prevPairCount <= firstKeyIndex && lastKeyIndex < nextPairCount)
                {
                    firstRemoved = i;
                    lastRemoved = i;
                    firstPartialNode = _children[i].EraseRange(transactionId, firstKeyIndex - prevPairCount,
                                                               lastKeyIndex - prevPairCount);
                    lastPartialNode = firstPartialNode;
                    break;
                }
                if (firstRemoved == -1 && firstKeyIndex < nextPairCount)
                {
                    if (prevPairCount > firstKeyIndex) throw new InvalidOperationException();
                    if (nextPairCount > lastKeyIndex) throw new InvalidOperationException();
                    firstRemoved = i;
                    firstPartialNode = _children[i].EraseRange(transactionId, firstKeyIndex - prevPairCount,
                                                               nextPairCount - 1 - prevPairCount);
                    continue;
                }
                if (lastKeyIndex >= nextPairCount - 1) throw new InvalidOperationException();
                lastRemoved = i;
                lastPartialNode = _children[i].EraseRange(transactionId, 0, lastKeyIndex - prevPairCount);
                break;
            }
            var finalChildrenCount = firstRemoved - (firstPartialNode == null ? 1 : 0)
                                   + _children.Length + 1 - lastRemoved - (lastPartialNode == null ? 1 : 0)
                                   - (firstPartialNode == lastPartialNode && firstPartialNode != null ? 1 : 0);
            var newKeys = new byte[finalChildrenCount - 1][];
            var newChildren = new IBTreeNode[finalChildrenCount];
            var newPairCounts = new long[finalChildrenCount];
            Array.Copy(_children, 0, newChildren, 0, firstRemoved);
            var idx = firstRemoved;
            if (firstPartialNode != null && firstPartialNode != lastPartialNode)
            {
                newChildren[idx] = firstPartialNode;
                idx++;
            }
            if (lastPartialNode != null)
            {
                newChildren[idx] = lastPartialNode;
                idx++;
            }
            Array.Copy(_children, lastRemoved + 1, newChildren, idx, finalChildrenCount - idx);
            var previousPairCount = 0L;
            for (var i = 0; i < finalChildrenCount; i++)
            {
                previousPairCount += newChildren[i].CalcKeyCount();
                newPairCounts[i] = previousPairCount;
            }
            for (var i = 0; i < finalChildrenCount - 1; i++)
            {
                newKeys[i] = newChildren[i + 1].GetLeftMostKey();
            }
            if (transactionId == TransactionId)
            {
                _keys = newKeys;
                _children = newChildren;
                _pairCounts = newPairCounts;
                return this;
            }
            return new BTreeBranch(transactionId, newKeys, newChildren, newPairCounts);
        }

        public void Iterate(BTreeIterateAction action)
        {
            foreach (var child in _children)
            {
                child.Iterate(action);
            }
        }

        public IBTreeNode RemappingIterate(long transactionId, BTreeRemappingIterateAction action)
        {
            var result = this;
            var children = _children;
            for (var i = 0; i < children.Length; i++)
            {
                var child = children[i];
                var newChild = child.RemappingIterate(transactionId, action);
                if (newChild == child) continue;
                if (result.TransactionId != transactionId)
                {
                    var newKeys = new byte[_keys.Length][];
                    Array.Copy(_keys, newKeys, newKeys.Length);
                    var newChildren = new IBTreeNode[_children.Length];
                    Array.Copy(_children, newChildren, newChildren.Length);
                    var newPairCounts = new long[_pairCounts.Length];
                    Array.Copy(_pairCounts, newPairCounts, newPairCounts.Length);
                    result = new BTreeBranch(transactionId, newKeys, newChildren, newPairCounts);
                    children = newChildren;
                }
                children[i] = newChild;
            }
            return result;
        }
    }
}