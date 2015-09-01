using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTreeMem
{
    class BTreeLeaf : IBTreeLeafNode, IBTreeNode
    {
        internal readonly long TransactionId;
        BTreeLeafMember[] _keyvalues;
        internal const int MaxMembers = 30;

        BTreeLeaf(long transactionId, int length)
        {
            TransactionId = transactionId;
            _keyvalues = new BTreeLeafMember[length];
        }

        internal BTreeLeaf(long transactionId, int length, Func<BTreeLeafMember> memberGenerator)
        {
            Debug.Assert(length > 0 && length <= MaxMembers);
            TransactionId = transactionId;
            _keyvalues = new BTreeLeafMember[length];
            for (int i = 0; i < _keyvalues.Length; i++)
            {
                _keyvalues[i] = memberGenerator();
            }
        }

        internal BTreeLeaf(long transactionId, BTreeLeafMember[] newKeyValues)
        {
            TransactionId = transactionId;
            _keyvalues = newKeyValues;
        }

        internal static IBTreeNode CreateFirst(CreateOrUpdateCtx ctx)
        {
            var result = new BTreeLeaf(ctx.TransactionId, 1);
            result._keyvalues[0] = NewMemberFromCtx(ctx);
            return result;
        }

        int Find(byte[] prefix, ByteBuffer key)
        {
            var left = 0;
            var right = _keyvalues.Length;
            while (left < right)
            {
                var middle = (left + right) / 2;
                var currentKey = _keyvalues[middle].Key;
                var result = BitArrayManipulation.CompareByteArray(prefix, prefix.Length,
                                                                   currentKey, Math.Min(currentKey.Length, prefix.Length));
                if (result == 0)
                {
                    result = BitArrayManipulation.CompareByteArray(key.Buffer, key.Offset, key.Length,
                                                                   currentKey, prefix.Length, currentKey.Length - prefix.Length);
                    if (result == 0)
                    {
                        return middle * 2 + 1;
                    }
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
            return left * 2;
        }

        public void CreateOrUpdate(CreateOrUpdateCtx ctx)
        {
            var index = Find(ctx.KeyPrefix, ctx.Key);
            if ((index & 1) == 1)
            {
                index = index / 2;
                ctx.Created = false;
                ctx.KeyIndex = index;
                var m = _keyvalues[index];
                m.Value = ctx.Value.ToByteArray();
                var leaf = this;
                if (ctx.TransactionId != TransactionId)
                {
                    leaf = new BTreeLeaf(ctx.TransactionId, _keyvalues.Length);
                    Array.Copy(_keyvalues, leaf._keyvalues, _keyvalues.Length);
                    ctx.Node1 = leaf;
                    ctx.Update = true;
                }
                leaf._keyvalues[index] = m;
                ctx.Stack.Add(new NodeIdxPair { Node = leaf, Idx = index });
                return;
            }
            index = index / 2;
            ctx.Created = true;
            ctx.KeyIndex = index;
            if (_keyvalues.Length < MaxMembers)
            {
                var newKeyValues = new BTreeLeafMember[_keyvalues.Length + 1];
                Array.Copy(_keyvalues, 0, newKeyValues, 0, index);
                newKeyValues[index] = NewMemberFromCtx(ctx);
                Array.Copy(_keyvalues, index, newKeyValues, index + 1, _keyvalues.Length - index);
                var leaf = this;
                if (ctx.TransactionId != TransactionId)
                {
                    leaf = new BTreeLeaf(ctx.TransactionId, newKeyValues);
                    ctx.Node1 = leaf;
                    ctx.Update = true;
                }
                else
                {
                    _keyvalues = newKeyValues;
                }
                ctx.Stack.Add(new NodeIdxPair { Node = leaf, Idx = index });
                return;
            }
            ctx.Split = true;
            var keyCountLeft = (_keyvalues.Length + 1) / 2;
            var keyCountRight = _keyvalues.Length + 1 - keyCountLeft;
            var leftNode = new BTreeLeaf(ctx.TransactionId, keyCountLeft);
            var rightNode = new BTreeLeaf(ctx.TransactionId, keyCountRight);
            ctx.Node1 = leftNode;
            ctx.Node2 = rightNode;
            if (index < keyCountLeft)
            {
                Array.Copy(_keyvalues, 0, leftNode._keyvalues, 0, index);
                leftNode._keyvalues[index] = NewMemberFromCtx(ctx);
                Array.Copy(_keyvalues, index, leftNode._keyvalues, index + 1, keyCountLeft - index - 1);
                Array.Copy(_keyvalues, keyCountLeft - 1, rightNode._keyvalues, 0, keyCountRight);
                ctx.Stack.Add(new NodeIdxPair { Node = leftNode, Idx = index });
                ctx.SplitInRight = false;
            }
            else
            {
                Array.Copy(_keyvalues, 0, leftNode._keyvalues, 0, keyCountLeft);
                Array.Copy(_keyvalues, keyCountLeft, rightNode._keyvalues, 0, index - keyCountLeft);
                rightNode._keyvalues[index - keyCountLeft] = NewMemberFromCtx(ctx);
                Array.Copy(_keyvalues, index, rightNode._keyvalues, index - keyCountLeft + 1, keyCountLeft + keyCountRight - 1 - index);
                ctx.Stack.Add(new NodeIdxPair { Node = rightNode, Idx = index - keyCountLeft });
                ctx.SplitInRight = true;
            }
        }

        public FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, byte[] prefix, ByteBuffer key)
        {
            var idx = Find(prefix, key);
            FindResult result;
            if ((idx & 1) == 1)
            {
                result = FindResult.Exact;
                idx = idx / 2;
            }
            else
            {
                result = FindResult.Previous;
                idx = idx / 2 - 1;
            }
            stack.Add(new NodeIdxPair { Node = this, Idx = idx });
            keyIndex = idx;
            return result;
        }

        static BTreeLeafMember NewMemberFromCtx(CreateOrUpdateCtx ctx)
        {
            return new BTreeLeafMember
                {
                    Key = ctx.WholeKey(),
                    Value = ctx.Value.ToByteArray()
                };
        }

        public long CalcKeyCount()
        {
            return _keyvalues.Length;
        }

        public byte[] GetLeftMostKey()
        {
            return _keyvalues[0].Key;
        }

        public void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex)
        {
            stack.Add(new NodeIdxPair { Node = this, Idx = (int)keyIndex });
        }

        public long FindLastWithPrefix(byte[] prefix)
        {
            var left = 0;
            var right = _keyvalues.Length - 1;
            byte[] currentKey;
            int result;
            while (left < right)
            {
                var middle = (left + right) / 2;
                currentKey = _keyvalues[middle].Key;
                result = BitArrayManipulation.CompareByteArray(prefix, prefix.Length,
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
            currentKey = _keyvalues[left].Key;
            result = BitArrayManipulation.CompareByteArray(prefix, prefix.Length,
                                                           currentKey, Math.Min(currentKey.Length, prefix.Length));
            if (result < 0) left--;
            return left;
        }

        public bool NextIdxValid(int idx)
        {
            return idx + 1 < _keyvalues.Length;
        }

        public void FillStackByLeftMost(List<NodeIdxPair> stack, int idx)
        {
            // Nothing to do
        }

        public void FillStackByRightMost(List<NodeIdxPair> stack, int i)
        {
            // Nothing to do
        }

        public int GetLastChildrenIdx()
        {
            return _keyvalues.Length - 1;
        }

        public IBTreeNode EraseRange(long transactionId, long firstKeyIndex, long lastKeyIndex)
        {
            var newKeyValues = new BTreeLeafMember[_keyvalues.Length + firstKeyIndex - lastKeyIndex - 1];
            Array.Copy(_keyvalues, 0, newKeyValues, 0, (int)firstKeyIndex);
            Array.Copy(_keyvalues, (int)lastKeyIndex + 1, newKeyValues, (int)firstKeyIndex, newKeyValues.Length - (int)firstKeyIndex);
            if (TransactionId == transactionId)
            {
                _keyvalues = newKeyValues;
                return this;
            }
            return new BTreeLeaf(transactionId, newKeyValues);
        }

        public ByteBuffer GetKey(int idx)
        {
            return ByteBuffer.NewAsync(_keyvalues[idx].Key);
        }

        public ByteBuffer GetMemberValue(int idx)
        {
            var kv = _keyvalues[idx];
            return ByteBuffer.NewAsync(kv.Value);
        }

        public void SetMemberValue(int idx, ByteBuffer value)
        {
            var kv = _keyvalues[idx];
            kv.Value = value.ToByteArray();
            _keyvalues[idx] = kv;
        }
    }
}