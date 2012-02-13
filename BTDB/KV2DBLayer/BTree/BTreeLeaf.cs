using System;
using BTDB.Buffer;

namespace BTDB.KV2DBLayer.BTree
{
    internal class BTreeLeaf : IBTreeNode
    {
        internal readonly long TransactionId;
        Member[] _keyvalues;
        const int MaxLength = 10;

        BTreeLeaf(long transactionId, int length)
        {
            TransactionId = transactionId;
            _keyvalues = new Member[length];
        }

        BTreeLeaf(long transactionId, Member[] newKeyValues)
        {
            TransactionId = transactionId;
            _keyvalues = newKeyValues;
        }

        internal static IBTreeNode CreateFirst(CreateOrUpdateCtx ctx)
        {
            var result = new BTreeLeaf(ctx.TransactionId, 1);
            result._keyvalues[0] = new Member
                {
                    Key = ctx.WholeKey(),
                    ValueFileId = ctx.ValueFileId,
                    ValueOfs = ctx.ValueOfs,
                    ValueSize = ctx.ValueSize
                };
            return result;
        }

        struct Member
        {
            internal byte[] Key;
            internal int ValueFileId;
            internal int ValueOfs;
            internal int ValueSize;
        }

        int Find(byte[] prefix, ByteBuffer key)
        {
            var left = 0;
            var right = _keyvalues.Length;
            while (left < right)
            {
                var middle = (left + right) / 2;
                var currentKey = _keyvalues[middle].Key;
                var result = BitArrayManipulation.CompareByteArray(prefix, 0, prefix.Length,
                                                                   currentKey, 0, Math.Min(currentKey.Length, prefix.Length));
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
                ctx.OldValueFileId = m.ValueFileId;
                ctx.OldValueOfs = m.ValueOfs;
                ctx.OldValueSize = m.ValueSize;
                m.ValueFileId = ctx.ValueFileId;
                m.ValueOfs = ctx.ValueOfs;
                m.ValueSize = ctx.ValueSize;
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
            if (_keyvalues.Length < MaxLength)
            {
                var newKeyValues = new Member[_keyvalues.Length + 1];
                Array.Copy(_keyvalues, 0, newKeyValues, 0, index);
                _keyvalues[index] = NewMemberFromCtx(ctx);
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

        static Member NewMemberFromCtx(CreateOrUpdateCtx ctx)
        {
            return new Member
                {
                    Key = ctx.WholeKey(),
                    ValueFileId = ctx.ValueFileId,
                    ValueOfs = ctx.OldValueOfs,
                    ValueSize = ctx.ValueSize
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
    }
}