using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTree
{
    class BTreeLeafComp : IBTreeLeafNode, IBTreeNode
    {
        internal readonly long TransactionId;
        byte[] _keyBytes;

        struct Member
        {
            internal ushort KeyOffset;
            internal ushort KeyLength;
            internal uint ValueFileId;
            internal uint ValueOfs;
            internal int ValueSize; // Negative length means compressed
        }

        Member[] _keyvalues;
        internal const long MaxTotalLen = ushort.MaxValue;
        internal const int MaxMembers = 30;

        BTreeLeafComp(long transactionId, int length)
        {
            TransactionId = transactionId;
            _keyvalues = new Member[length];
        }

        internal BTreeLeafComp(long transactionId, BTreeLeafMember[] newKeyValues)
        {
            Debug.Assert(newKeyValues.Length > 0 && newKeyValues.Length <= MaxMembers);
            TransactionId = transactionId;
            _keyBytes = new byte[newKeyValues.Sum(m => m.Key.Length)];
            _keyvalues = new Member[newKeyValues.Length];
            ushort ofs = 0;
            for (var i = 0; i < newKeyValues.Length; i++)
            {
                _keyvalues[i] = new Member
                {
                    KeyOffset = ofs,
                    KeyLength = (ushort) newKeyValues[i].Key.Length,
                    ValueFileId = newKeyValues[i].ValueFileId,
                    ValueOfs = newKeyValues[i].ValueOfs,
                    ValueSize = newKeyValues[i].ValueSize
                };
                Array.Copy(newKeyValues[i].Key, 0, _keyBytes, ofs, _keyvalues[i].KeyLength);
                ofs += _keyvalues[i].KeyLength;
            }
        }

        BTreeLeafComp(long transactionId, byte[] newKeyBytes, Member[] newKeyValues)
        {
            TransactionId = transactionId;
            _keyBytes = newKeyBytes;
            _keyvalues = newKeyValues;
        }

        internal static IBTreeNode CreateFirst(CreateOrUpdateCtx ctx)
        {
            Debug.Assert(ctx.WholeKeyLen <= MaxTotalLen);
            var result = new BTreeLeafComp(ctx.TransactionId, 1);
            result._keyBytes = ctx.WholeKey();
            result._keyvalues[0] = new Member
            {
                KeyOffset = 0,
                KeyLength = (ushort) result._keyBytes.Length,
                ValueFileId = ctx.ValueFileId,
                ValueOfs = ctx.ValueOfs,
                ValueSize = ctx.ValueSize
            };
            return result;
        }

        int Find(byte[] prefix, ByteBuffer key)
        {
            var left = 0;
            var right = _keyvalues.Length;
            var keyBytes = _keyBytes;
            while (left < right)
            {
                var middle = (left + right) / 2;
                int currentKeyOfs = _keyvalues[middle].KeyOffset;
                int currentKeyLen = _keyvalues[middle].KeyLength;
                var result = BitArrayManipulation.CompareByteArray(prefix, 0, prefix.Length,
                    keyBytes, currentKeyOfs, Math.Min(currentKeyLen, prefix.Length));
                if (result == 0)
                {
                    result = BitArrayManipulation.CompareByteArray(key.Buffer, key.Offset, key.Length,
                        keyBytes, currentKeyOfs + prefix.Length, currentKeyLen - prefix.Length);
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
                m.ValueFileId = ctx.ValueFileId;
                m.ValueOfs = ctx.ValueOfs;
                m.ValueSize = ctx.ValueSize;
                var leaf = this;
                if (ctx.TransactionId != TransactionId)
                {
                    leaf = new BTreeLeafComp(ctx.TransactionId, _keyvalues.Length);
                    Array.Copy(_keyvalues, leaf._keyvalues, _keyvalues.Length);
                    leaf._keyBytes = _keyBytes;
                    ctx.Node1 = leaf;
                    ctx.Update = true;
                }

                leaf._keyvalues[index] = m;
                ctx.Stack.Add(new NodeIdxPair {Node = leaf, Idx = index});
                return;
            }

            if ((long) _keyBytes.Length + ctx.WholeKeyLen > MaxTotalLen)
            {
                var currentKeyValues = new BTreeLeafMember[_keyvalues.Length];
                for (int i = 0; i < currentKeyValues.Length; i++)
                {
                    var member = _keyvalues[i];
                    currentKeyValues[i] = new BTreeLeafMember
                    {
                        Key = ByteBuffer.NewAsync(_keyBytes, member.KeyOffset, member.KeyLength).ToByteArray(),
                        ValueFileId = member.ValueFileId,
                        ValueOfs = member.ValueOfs,
                        ValueSize = member.ValueSize
                    };
                }

                new BTreeLeaf(ctx.TransactionId - 1, currentKeyValues).CreateOrUpdate(ctx);
                return;
            }

            index = index / 2;
            ctx.Created = true;
            ctx.KeyIndex = index;
            var newKey = ctx.WholeKey();
            if (_keyvalues.Length < MaxMembers)
            {
                var newKeyValues = new Member[_keyvalues.Length + 1];
                var newKeyBytes = new byte[_keyBytes.Length + newKey.Length];
                Array.Copy(_keyvalues, 0, newKeyValues, 0, index);
                var ofs = (ushort) (index == 0
                    ? 0
                    : newKeyValues[index - 1].KeyOffset + newKeyValues[index - 1].KeyLength);
                newKeyValues[index] = new Member
                {
                    KeyOffset = ofs,
                    KeyLength = (ushort) newKey.Length,
                    ValueFileId = ctx.ValueFileId,
                    ValueOfs = ctx.ValueOfs,
                    ValueSize = ctx.ValueSize
                };
                Array.Copy(_keyBytes, 0, newKeyBytes, 0, ofs);
                Array.Copy(newKey, 0, newKeyBytes, ofs, newKey.Length);
                Array.Copy(_keyBytes, ofs, newKeyBytes, ofs + newKey.Length, _keyBytes.Length - ofs);
                Array.Copy(_keyvalues, index, newKeyValues, index + 1, _keyvalues.Length - index);
                RecalculateOffsets(newKeyValues);
                var leaf = this;
                if (ctx.TransactionId != TransactionId)
                {
                    leaf = new BTreeLeafComp(ctx.TransactionId, newKeyBytes, newKeyValues);
                    ctx.Node1 = leaf;
                    ctx.Update = true;
                }
                else
                {
                    _keyvalues = newKeyValues;
                    _keyBytes = newKeyBytes;
                }

                ctx.Stack.Add(new NodeIdxPair {Node = leaf, Idx = index});
                return;
            }

            ctx.Split = true;
            var keyCountLeft = (_keyvalues.Length + 1) / 2;
            var keyCountRight = _keyvalues.Length + 1 - keyCountLeft;
            var leftNode = new BTreeLeafComp(ctx.TransactionId, keyCountLeft);
            var rightNode = new BTreeLeafComp(ctx.TransactionId, keyCountRight);
            ctx.Node1 = leftNode;
            ctx.Node2 = rightNode;
            if (index < keyCountLeft)
            {
                Array.Copy(_keyvalues, 0, leftNode._keyvalues, 0, index);
                var ofs = (ushort) (index == 0 ? 0 : _keyvalues[index - 1].KeyOffset + _keyvalues[index - 1].KeyLength);
                leftNode._keyvalues[index] = new Member
                {
                    KeyOffset = ofs,
                    KeyLength = (ushort) newKey.Length,
                    ValueFileId = ctx.ValueFileId,
                    ValueOfs = ctx.ValueOfs,
                    ValueSize = ctx.ValueSize
                };
                Array.Copy(_keyvalues, index, leftNode._keyvalues, index + 1, keyCountLeft - index - 1);
                Array.Copy(_keyvalues, keyCountLeft - 1, rightNode._keyvalues, 0, keyCountRight);
                var leftKeyBytesLen = _keyvalues[keyCountLeft - 1].KeyOffset + newKey.Length;
                var newKeyBytes = new byte[leftKeyBytesLen];
                Array.Copy(_keyBytes, 0, newKeyBytes, 0, ofs);
                Array.Copy(newKey, 0, newKeyBytes, ofs, newKey.Length);
                Array.Copy(_keyBytes, ofs, newKeyBytes, ofs + newKey.Length, leftKeyBytesLen - (ofs + newKey.Length));
                leftNode._keyBytes = newKeyBytes;
                newKeyBytes = new byte[_keyBytes.Length + newKey.Length - leftKeyBytesLen];
                Array.Copy(_keyBytes, leftKeyBytesLen - newKey.Length, newKeyBytes, 0, newKeyBytes.Length);
                rightNode._keyBytes = newKeyBytes;
                ctx.Stack.Add(new NodeIdxPair {Node = leftNode, Idx = index});
                ctx.SplitInRight = false;
                RecalculateOffsets(leftNode._keyvalues);
            }
            else
            {
                Array.Copy(_keyvalues, 0, leftNode._keyvalues, 0, keyCountLeft);
                var leftKeyBytesLen = _keyvalues[keyCountLeft].KeyOffset;
                var newKeyBytes = new byte[leftKeyBytesLen];
                Array.Copy(_keyBytes, 0, newKeyBytes, 0, leftKeyBytesLen);
                leftNode._keyBytes = newKeyBytes;
                newKeyBytes = new byte[_keyBytes.Length + newKey.Length - leftKeyBytesLen];
                var ofs = (index == _keyvalues.Length ? _keyBytes.Length : _keyvalues[index].KeyOffset) -
                          leftKeyBytesLen;
                Array.Copy(_keyBytes, leftKeyBytesLen, newKeyBytes, 0, ofs);
                Array.Copy(newKey, 0, newKeyBytes, ofs, newKey.Length);
                Array.Copy(_keyBytes, ofs + leftKeyBytesLen, newKeyBytes, ofs + newKey.Length,
                    _keyBytes.Length - ofs - leftKeyBytesLen);
                rightNode._keyBytes = newKeyBytes;
                Array.Copy(_keyvalues, keyCountLeft, rightNode._keyvalues, 0, index - keyCountLeft);
                rightNode._keyvalues[index - keyCountLeft] = new Member
                {
                    KeyOffset = 0,
                    KeyLength = (ushort) newKey.Length,
                    ValueFileId = ctx.ValueFileId,
                    ValueOfs = ctx.ValueOfs,
                    ValueSize = ctx.ValueSize
                };
                Array.Copy(_keyvalues, index, rightNode._keyvalues, index - keyCountLeft + 1,
                    keyCountLeft + keyCountRight - 1 - index);
                ctx.Stack.Add(new NodeIdxPair {Node = rightNode, Idx = index - keyCountLeft});
                ctx.SplitInRight = true;
            }

            RecalculateOffsets(rightNode._keyvalues);
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

            stack.Add(new NodeIdxPair {Node = this, Idx = idx});
            keyIndex = idx;
            return result;
        }

        static BTreeLeafMember NewMemberFromCtx(CreateOrUpdateCtx ctx)
        {
            return new BTreeLeafMember
            {
                Key = ctx.WholeKey(),
                ValueFileId = ctx.ValueFileId,
                ValueOfs = ctx.ValueOfs,
                ValueSize = ctx.ValueSize
            };
        }

        public long CalcKeyCount()
        {
            return _keyvalues.Length;
        }

        public byte[] GetLeftMostKey()
        {
            return ByteBuffer.NewAsync(_keyBytes, _keyvalues[0].KeyOffset, _keyvalues[0].KeyLength).ToByteArray();
        }

        public void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex)
        {
            stack.Add(new NodeIdxPair {Node = this, Idx = (int) keyIndex});
        }

        public long FindLastWithPrefix(byte[] prefix)
        {
            var left = 0;
            var right = _keyvalues.Length - 1;
            var keyBytes = _keyBytes;
            int result;
            int currentKeyOfs;
            int currentKeyLen;
            while (left < right)
            {
                var middle = (left + right) / 2;
                currentKeyOfs = _keyvalues[middle].KeyOffset;
                currentKeyLen = _keyvalues[middle].KeyLength;
                result = BitArrayManipulation.CompareByteArray(prefix, 0, prefix.Length,
                    keyBytes, currentKeyOfs, Math.Min(currentKeyLen, prefix.Length));
                if (result < 0)
                {
                    right = middle;
                }
                else
                {
                    left = middle + 1;
                }
            }

            currentKeyOfs = _keyvalues[left].KeyOffset;
            currentKeyLen = _keyvalues[left].KeyLength;
            result = BitArrayManipulation.CompareByteArray(prefix, 0, prefix.Length,
                keyBytes, currentKeyOfs, Math.Min(currentKeyLen, prefix.Length));
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
            var newKeyValues = new Member[_keyvalues.Length + firstKeyIndex - lastKeyIndex - 1];
            var newKeyBytes = new byte[_keyBytes.Length + _keyvalues[firstKeyIndex].KeyOffset -
                                       _keyvalues[lastKeyIndex].KeyOffset - _keyvalues[lastKeyIndex].KeyLength];
            Array.Copy(_keyvalues, 0, newKeyValues, 0, (int) firstKeyIndex);
            Array.Copy(_keyvalues, (int) lastKeyIndex + 1, newKeyValues, (int) firstKeyIndex,
                newKeyValues.Length - (int) firstKeyIndex);
            Array.Copy(_keyBytes, 0, newKeyBytes, 0, _keyvalues[firstKeyIndex].KeyOffset);
            Array.Copy(_keyBytes, _keyvalues[lastKeyIndex].KeyOffset + _keyvalues[lastKeyIndex].KeyLength, newKeyBytes,
                _keyvalues[firstKeyIndex].KeyOffset, newKeyBytes.Length - _keyvalues[firstKeyIndex].KeyOffset);
            RecalculateOffsets(newKeyValues);
            if (TransactionId == transactionId)
            {
                _keyvalues = newKeyValues;
                _keyBytes = newKeyBytes;
                return this;
            }

            return new BTreeLeafComp(transactionId, newKeyBytes, newKeyValues);
        }

        static void RecalculateOffsets(Member[] keyvalues)
        {
            ushort ofs = 0;
            for (var i = 0; i < keyvalues.Length; i++)
            {
                keyvalues[i].KeyOffset = ofs;
                ofs += keyvalues[i].KeyLength;
            }
        }

        public void Iterate(ValuesIterateAction action)
        {
            var kv = _keyvalues;
            foreach (var member in kv)
            {
                if (member.ValueFileId == 0) continue;
                action(member.ValueFileId, member.ValueOfs, member.ValueSize);
            }
        }

        public IBTreeNode ReplaceValues(ReplaceValuesCtx ctx)
        {
            var result = this;
            var keyValues = _keyvalues;
            var map = ctx._newPositionMap;
            for (var i = 0; i < keyValues.Length; i++)
            {
                ref var ii = ref keyValues[i];
                if (map.TryGetValue(((ulong) ii.ValueFileId << 32) | ii.ValueOfs, out var newOffset))
                {
                    if (result.TransactionId != ctx._transactionId)
                    {
                        var newKeyValues = new Member[keyValues.Length];
                        Array.Copy(keyValues, newKeyValues, newKeyValues.Length);
                        result = new BTreeLeafComp(ctx._transactionId, _keyBytes, newKeyValues);
                        keyValues = newKeyValues;
                    }

                    keyValues[i].ValueFileId = (uint) (newOffset >> 32);
                    keyValues[i].ValueOfs = (uint) newOffset;
                }
            }

            return result;
        }

        public ByteBuffer GetKey(int idx)
        {
            return ByteBuffer.NewAsync(_keyBytes, _keyvalues[idx].KeyOffset, _keyvalues[idx].KeyLength);
        }

        public BTreeValue GetMemberValue(int idx)
        {
            var kv = _keyvalues[idx];
            return new BTreeValue
            {
                ValueFileId = kv.ValueFileId,
                ValueOfs = kv.ValueOfs,
                ValueSize = kv.ValueSize
            };
        }

        public void SetMemberValue(int idx, BTreeValue value)
        {
            var kv = _keyvalues[idx];
            kv.ValueFileId = value.ValueFileId;
            kv.ValueOfs = value.ValueOfs;
            kv.ValueSize = value.ValueSize;
            _keyvalues[idx] = kv;
        }
    }
}
