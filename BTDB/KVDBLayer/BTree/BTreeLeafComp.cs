using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BTDB.Collections;

namespace BTDB.KVDBLayer.BTree;

class BTreeLeafComp : IBTreeLeafNode, IBTreeNode
{
    internal readonly long TransactionId;
    byte[]? _keyBytes;

    struct Member
    {
        internal ushort KeyOffset;
        internal ushort KeyLength;
        internal uint ValueFileId;
        internal uint ValueOfs;
        internal int ValueSize; // Negative length means compressed
    }

    Member[] _keyValues;
    internal const long MaxTotalLen = ushort.MaxValue;
    internal const int MaxMembers = 30;

    BTreeLeafComp(long transactionId, int length)
    {
        TransactionId = transactionId;
        _keyValues = new Member[length];
    }

    internal BTreeLeafComp(long transactionId, BTreeLeafMember[] newKeyValues)
    {
        Debug.Assert(newKeyValues.Length > 0 && newKeyValues.Length <= MaxMembers);
        TransactionId = transactionId;
        _keyBytes = new byte[newKeyValues.Sum(m => m.Key.Length)];
        _keyValues = new Member[newKeyValues.Length];
        ushort ofs = 0;
        for (var i = 0; i < newKeyValues.Length; i++)
        {
            _keyValues[i] = new Member
            {
                KeyOffset = ofs,
                KeyLength = (ushort)newKeyValues[i].Key.Length,
                ValueFileId = newKeyValues[i].ValueFileId,
                ValueOfs = newKeyValues[i].ValueOfs,
                ValueSize = newKeyValues[i].ValueSize
            };
            Array.Copy(newKeyValues[i].Key, 0, _keyBytes, ofs, _keyValues[i].KeyLength);
            ofs += _keyValues[i].KeyLength;
        }
    }

    BTreeLeafComp(long transactionId, byte[] newKeyBytes, Member[] newKeyValues)
    {
        TransactionId = transactionId;
        _keyBytes = newKeyBytes;
        _keyValues = newKeyValues;
    }

    internal static IBTreeNode CreateFirst(ref CreateOrUpdateCtx ctx)
    {
        Debug.Assert(ctx.Key.Length <= MaxTotalLen);
        var result = new BTreeLeafComp(ctx.TransactionId, 1) { _keyBytes = ctx.Key.ToArray() };
        result._keyValues[0] = new Member
        {
            KeyOffset = 0,
            KeyLength = (ushort)result._keyBytes.Length,
            ValueFileId = ctx.ValueFileId,
            ValueOfs = ctx.ValueOfs,
            ValueSize = ctx.ValueSize
        };
        return result;
    }

    int Find(in ReadOnlySpan<byte> key)
    {
        var left = 0;
        var right = _keyValues.Length;
        var keyBytes = _keyBytes;
        while (left < right)
        {
            var middle = (left + right) / 2;
            int currentKeyOfs = _keyValues[middle].KeyOffset;
            int currentKeyLen = _keyValues[middle].KeyLength;
            var result = key.SequenceCompareTo(keyBytes.AsSpan(currentKeyOfs, currentKeyLen));
            if (result == 0)
            {
                return middle * 2 + 1;
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

    public void CreateOrUpdate(ref CreateOrUpdateCtx ctx)
    {
        var index = Find(ctx.Key);
        if ((index & 1) == 1)
        {
            index = (int)((uint)index / 2);
            ctx.Created = false;
            ctx.KeyIndex = index;
            var m = _keyValues[index];
            m.ValueFileId = ctx.ValueFileId;
            m.ValueOfs = ctx.ValueOfs;
            m.ValueSize = ctx.ValueSize;
            var leaf = this;
            if (ctx.TransactionId != TransactionId)
            {
                leaf = new BTreeLeafComp(ctx.TransactionId, _keyValues.Length);
                Array.Copy(_keyValues, leaf._keyValues, _keyValues.Length);
                leaf._keyBytes = _keyBytes;
                ctx.Node1 = leaf;
                ctx.Update = true;
            }

            leaf._keyValues[index] = m;
            ctx.Stack!.Add(new NodeIdxPair { Node = leaf, Idx = index });
            return;
        }

        if ((long)_keyBytes!.Length + ctx.Key.Length > MaxTotalLen)
        {
            var currentKeyValues = new BTreeLeafMember[_keyValues.Length];
            for (var i = 0; i < currentKeyValues.Length; i++)
            {
                var member = _keyValues[i];
                currentKeyValues[i] = new BTreeLeafMember
                {
                    Key = _keyBytes.AsSpan(member.KeyOffset, member.KeyLength).ToArray(),
                    ValueFileId = member.ValueFileId,
                    ValueOfs = member.ValueOfs,
                    ValueSize = member.ValueSize
                };
            }

            new BTreeLeaf(ctx.TransactionId - 1, currentKeyValues).CreateOrUpdate(ref ctx);
            return;
        }

        index = (int)((uint)index / 2);
        ctx.Created = true;
        ctx.KeyIndex = index;
        var newKey = ctx.Key;
        if (_keyValues.Length < MaxMembers)
        {
            var newKeyValues = new Member[_keyValues.Length + 1];
            var newKeyBytes = new byte[_keyBytes.Length + newKey.Length];
            Array.Copy(_keyValues, 0, newKeyValues, 0, index);
            var ofs = (ushort)(index == 0
                ? 0
                : newKeyValues[index - 1].KeyOffset + newKeyValues[index - 1].KeyLength);
            newKeyValues[index] = new Member
            {
                KeyOffset = ofs,
                KeyLength = (ushort)newKey.Length,
                ValueFileId = ctx.ValueFileId,
                ValueOfs = ctx.ValueOfs,
                ValueSize = ctx.ValueSize
            };
            Array.Copy(_keyBytes, 0, newKeyBytes, 0, ofs);
            newKey.CopyTo(newKeyBytes.AsSpan(ofs));
            Array.Copy(_keyBytes, ofs, newKeyBytes, ofs + newKey.Length, _keyBytes.Length - ofs);
            Array.Copy(_keyValues, index, newKeyValues, index + 1, _keyValues.Length - index);
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
                _keyValues = newKeyValues;
                _keyBytes = newKeyBytes;
            }

            ctx.Stack!.Add(new NodeIdxPair { Node = leaf, Idx = index });
            return;
        }

        ctx.Split = true;
        var keyCountLeft = (_keyValues.Length + 1) / 2;
        var keyCountRight = _keyValues.Length + 1 - keyCountLeft;
        var leftNode = new BTreeLeafComp(ctx.TransactionId, keyCountLeft);
        var rightNode = new BTreeLeafComp(ctx.TransactionId, keyCountRight);
        ctx.Node1 = leftNode;
        ctx.Node2 = rightNode;
        if (index < keyCountLeft)
        {
            Array.Copy(_keyValues, 0, leftNode._keyValues, 0, index);
            var ofs = (ushort)(index == 0 ? 0 : _keyValues[index - 1].KeyOffset + _keyValues[index - 1].KeyLength);
            leftNode._keyValues[index] = new Member
            {
                KeyOffset = ofs,
                KeyLength = (ushort)newKey.Length,
                ValueFileId = ctx.ValueFileId,
                ValueOfs = ctx.ValueOfs,
                ValueSize = ctx.ValueSize
            };
            Array.Copy(_keyValues, index, leftNode._keyValues, index + 1, keyCountLeft - index - 1);
            Array.Copy(_keyValues, keyCountLeft - 1, rightNode._keyValues, 0, keyCountRight);
            var leftKeyBytesLen = _keyValues[keyCountLeft - 1].KeyOffset + newKey.Length;
            var newKeyBytes = new byte[leftKeyBytesLen];
            Array.Copy(_keyBytes, 0, newKeyBytes, 0, ofs);
            newKey.CopyTo(newKeyBytes.AsSpan(ofs));
            Array.Copy(_keyBytes, ofs, newKeyBytes, ofs + newKey.Length, leftKeyBytesLen - (ofs + newKey.Length));
            leftNode._keyBytes = newKeyBytes;
            newKeyBytes = new byte[_keyBytes.Length + newKey.Length - leftKeyBytesLen];
            Array.Copy(_keyBytes, leftKeyBytesLen - newKey.Length, newKeyBytes, 0, newKeyBytes.Length);
            rightNode._keyBytes = newKeyBytes;
            ctx.Stack!.Add(new NodeIdxPair { Node = leftNode, Idx = index });
            ctx.SplitInRight = false;
            RecalculateOffsets(leftNode._keyValues);
        }
        else
        {
            Array.Copy(_keyValues, 0, leftNode._keyValues, 0, keyCountLeft);
            var leftKeyBytesLen = _keyValues[keyCountLeft].KeyOffset;
            var newKeyBytes = new byte[leftKeyBytesLen];
            Array.Copy(_keyBytes, 0, newKeyBytes, 0, leftKeyBytesLen);
            leftNode._keyBytes = newKeyBytes;
            newKeyBytes = new byte[_keyBytes.Length + newKey.Length - leftKeyBytesLen];
            var ofs = (index == _keyValues.Length ? _keyBytes.Length : _keyValues[index].KeyOffset) -
                      leftKeyBytesLen;
            Array.Copy(_keyBytes, leftKeyBytesLen, newKeyBytes, 0, ofs);
            newKey.CopyTo(newKeyBytes.AsSpan(ofs));
            Array.Copy(_keyBytes, ofs + leftKeyBytesLen, newKeyBytes, ofs + newKey.Length,
                _keyBytes.Length - ofs - leftKeyBytesLen);
            rightNode._keyBytes = newKeyBytes;
            Array.Copy(_keyValues, keyCountLeft, rightNode._keyValues, 0, index - keyCountLeft);
            rightNode._keyValues[index - keyCountLeft] = new Member
            {
                KeyOffset = 0,
                KeyLength = (ushort)newKey.Length,
                ValueFileId = ctx.ValueFileId,
                ValueOfs = ctx.ValueOfs,
                ValueSize = ctx.ValueSize
            };
            Array.Copy(_keyValues, index, rightNode._keyValues, index - keyCountLeft + 1,
                keyCountLeft + keyCountRight - 1 - index);
            ctx.Stack!.Add(new NodeIdxPair { Node = rightNode, Idx = index - keyCountLeft });
            ctx.SplitInRight = true;
        }

        RecalculateOffsets(rightNode._keyValues);
    }

    public FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key)
    {
        var idx = Find(key);
        FindResult result;
        if ((idx & 1) == 1)
        {
            result = FindResult.Exact;
            idx = (int)((uint)idx / 2);
        }
        else
        {
            result = FindResult.Previous;
            idx = (int)((uint)idx / 2) - 1;
        }

        stack.Add(new NodeIdxPair { Node = this, Idx = idx });
        keyIndex = idx;
        return result;
    }

    public long CalcKeyCount()
    {
        return _keyValues.Length;
    }

    public byte[] GetLeftMostKey()
    {
        Debug.Assert(_keyValues[0].KeyOffset == 0);
        return _keyBytes.AsSpan(0, _keyValues[0].KeyLength).ToArray();
    }

    public void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex)
    {
        stack.Add(new NodeIdxPair { Node = this, Idx = (int)keyIndex });
    }

    public long FindLastWithPrefix(in ReadOnlySpan<byte> prefix)
    {
        var left = 0;
        var right = _keyValues.Length - 1;
        var keyBytes = _keyBytes;
        int result;
        int currentKeyOfs;
        int currentKeyLen;
        while (left < right)
        {
            var middle = (left + right) / 2;
            currentKeyOfs = _keyValues[middle].KeyOffset;
            currentKeyLen = _keyValues[middle].KeyLength;
            result = prefix.SequenceCompareTo(
                keyBytes.AsSpan(currentKeyOfs, Math.Min(currentKeyLen, prefix.Length)));
            if (result < 0)
            {
                right = middle;
            }
            else
            {
                left = middle + 1;
            }
        }

        currentKeyOfs = _keyValues[left].KeyOffset;
        currentKeyLen = _keyValues[left].KeyLength;
        result = prefix.SequenceCompareTo(keyBytes.AsSpan(currentKeyOfs, Math.Min(currentKeyLen, prefix.Length)));
        if (result < 0) left--;
        return left;
    }

    public bool NextIdxValid(int idx)
    {
        return idx + 1 < _keyValues.Length;
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
        return _keyValues.Length - 1;
    }

    public IBTreeNode EraseRange(long transactionId, long firstKeyIndex, long lastKeyIndex)
    {
        var newKeyValues = new Member[_keyValues.Length + firstKeyIndex - lastKeyIndex - 1];
        var newKeyBytes = new byte[_keyBytes!.Length + _keyValues[firstKeyIndex].KeyOffset -
                                   _keyValues[lastKeyIndex].KeyOffset - _keyValues[lastKeyIndex].KeyLength];
        Array.Copy(_keyValues, 0, newKeyValues, 0, (int)firstKeyIndex);
        Array.Copy(_keyValues, (int)lastKeyIndex + 1, newKeyValues, (int)firstKeyIndex,
            newKeyValues.Length - (int)firstKeyIndex);
        Array.Copy(_keyBytes, 0, newKeyBytes, 0, _keyValues[firstKeyIndex].KeyOffset);
        Array.Copy(_keyBytes, _keyValues[lastKeyIndex].KeyOffset + _keyValues[lastKeyIndex].KeyLength, newKeyBytes,
            _keyValues[firstKeyIndex].KeyOffset, newKeyBytes.Length - _keyValues[firstKeyIndex].KeyOffset);
        RecalculateOffsets(newKeyValues);
        if (TransactionId == transactionId)
        {
            _keyValues = newKeyValues;
            _keyBytes = newKeyBytes;
            return this;
        }

        return new BTreeLeafComp(transactionId, newKeyBytes, newKeyValues);
    }

    public IBTreeNode EraseOne(long transactionId, long keyIndex)
    {
        var newKeyValues = new Member[_keyValues.Length - 1];
        var newKeyBytes = new byte[_keyBytes!.Length - _keyValues[keyIndex].KeyLength];
        Array.Copy(_keyValues, 0, newKeyValues, 0, (int)keyIndex);
        Array.Copy(_keyValues, (int)keyIndex + 1, newKeyValues, (int)keyIndex,
            newKeyValues.Length - (int)keyIndex);
        Array.Copy(_keyBytes, 0, newKeyBytes, 0, _keyValues[keyIndex].KeyOffset);
        Array.Copy(_keyBytes, _keyValues[keyIndex].KeyOffset + _keyValues[keyIndex].KeyLength, newKeyBytes,
            _keyValues[keyIndex].KeyOffset, newKeyBytes.Length - _keyValues[keyIndex].KeyOffset);
        RecalculateOffsets(newKeyValues);
        if (TransactionId == transactionId)
        {
            _keyValues = newKeyValues;
            _keyBytes = newKeyBytes;
            return this;
        }

        return new BTreeLeafComp(transactionId, newKeyBytes, newKeyValues);
    }

    static void RecalculateOffsets(Member[] keyValues)
    {
        ushort ofs = 0;
        for (var i = 0; i < keyValues.Length; i++)
        {
            keyValues[i].KeyOffset = ofs;
            ofs += keyValues[i].KeyLength;
        }
    }

    public void Iterate(ValuesIterateAction action)
    {
        var kv = _keyValues;
        foreach (var member in kv)
        {
            if (member.ValueFileId == 0) continue;
            action(member.ValueFileId, member.ValueOfs, member.ValueSize);
        }
    }

    public IBTreeNode ReplaceValues(ReplaceValuesCtx ctx)
    {
        var result = this;
        var keyValues = _keyValues;
        var map = ctx._newPositionMap;
        for (var i = 0; i < keyValues.Length; i++)
        {
            ref var ii = ref keyValues[i];
            if (map.TryGetValue(((ulong)ii.ValueFileId << 32) | ii.ValueOfs, out var newOffset))
            {
                if (result.TransactionId != ctx._transactionId)
                {
                    var newKeyValues = new Member[keyValues.Length];
                    Array.Copy(keyValues, newKeyValues, newKeyValues.Length);
                    result = new BTreeLeafComp(ctx._transactionId, _keyBytes!, newKeyValues);
                    keyValues = newKeyValues;
                }

                keyValues[i].ValueFileId = ctx._targetFileId;
                keyValues[i].ValueOfs = newOffset;
            }
        }

        return result;
    }

    public void CalcBTreeStats(RefDictionary<(uint Depth, uint Children), uint> stats, uint depth)
    {
        stats.GetOrAddValueRef((depth, (uint)_keyValues.Length))++;
    }

    public ReadOnlySpan<byte> GetKey(int idx)
    {
        ref var kv = ref _keyValues[idx];
        return _keyBytes.AsSpan(kv.KeyOffset, kv.KeyLength);
    }

    public BTreeValue GetMemberValue(int idx)
    {
        ref var kv = ref _keyValues[idx];
        return new BTreeValue
        {
            ValueFileId = kv.ValueFileId,
            ValueOfs = kv.ValueOfs,
            ValueSize = kv.ValueSize
        };
    }

    public void SetMemberValue(int idx, in BTreeValue value)
    {
        ref var kv = ref _keyValues[idx];
        kv.ValueFileId = value.ValueFileId;
        kv.ValueOfs = value.ValueOfs;
        kv.ValueSize = value.ValueSize;
    }
}
