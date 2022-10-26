using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.BTree;

class BTreeRoot : IBTreeRootNode
{
    readonly long _transactionId;
    long _keyValueCount;
    IBTreeNode? _rootNode;

    public BTreeRoot(long transactionId)
    {
        _transactionId = transactionId;
    }

    public void CreateOrUpdate(ref CreateOrUpdateCtx ctx)
    {
        ctx.TransactionId = _transactionId;
        if (ctx.Stack == null) ctx.Stack = new List<NodeIdxPair>();
        else ctx.Stack.Clear();
        if (_rootNode == null)
        {
            _rootNode = ctx.Key.Length > BTreeLeafComp.MaxTotalLen
                ? BTreeLeaf.CreateFirst(ref ctx)
                : BTreeLeafComp.CreateFirst(ref ctx);
            _keyValueCount = 1;
            ctx.Stack!.Add(new NodeIdxPair { Node = _rootNode, Idx = 0 });
            ctx.KeyIndex = 0;
            ctx.Created = true;
            return;
        }
        ctx.Depth = 0;
        _rootNode.CreateOrUpdate(ref ctx);
        if (ctx.Split)
        {
            _rootNode = new BTreeBranch(ctx.TransactionId, ctx.Node1!, ctx.Node2!);
            ctx.Stack!.Insert(0, new NodeIdxPair { Node = _rootNode, Idx = ctx.SplitInRight ? 1 : 0 });
        }
        else if (ctx.Update)
        {
            _rootNode = ctx.Node1;
        }
        if (ctx.Created)
        {
            _keyValueCount++;
        }
    }

    public FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key)
    {
        throw new InvalidOperationException();
    }

    public FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key, uint prefixLen)
    {
        stack.Clear();
        if (_rootNode == null)
        {
            keyIndex = -1;
            return FindResult.NotFound;
        }
        var result = _rootNode.FindKey(stack, out keyIndex, key);
        if (result == FindResult.Previous)
        {
            if (keyIndex < 0)
            {
                keyIndex = 0;
                stack[^1] = new NodeIdxPair { Node = stack[^1].Node, Idx = 0 };
                result = FindResult.Next;
            }
            else
            {
                if (!KeyStartsWithPrefix(key.Slice(0, (int)prefixLen), GetKeyFromStack(stack)))
                {
                    result = FindResult.Next;
                    keyIndex++;
                    if (!FindNextKey(stack))
                    {
                        return FindResult.NotFound;
                    }
                }
            }
            if (!KeyStartsWithPrefix(key.Slice(0, (int)prefixLen), GetKeyFromStack(stack)))
            {
                return FindResult.NotFound;
            }
        }
        return result;
    }

    internal static bool KeyStartsWithPrefix(in ReadOnlySpan<byte> prefix, in ReadOnlySpan<byte> key)
    {
        if (prefix.Length == 0) return true;
        if (key.Length < prefix.Length) return false;
        return prefix.SequenceEqual(key.Slice(0, prefix.Length));
    }

    static ReadOnlySpan<byte> GetKeyFromStack(List<NodeIdxPair> stack)
    {
        var last = stack[^1];
        return ((IBTreeLeafNode)last.Node).GetKey(last.Idx);
    }

    public long CalcKeyCount()
    {
        return _keyValueCount;
    }

    public byte[] GetLeftMostKey()
    {
        return _rootNode!.GetLeftMostKey();
    }

    public void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex)
    {
        Debug.Assert(keyIndex >= 0 && keyIndex < _keyValueCount);
        stack.Clear();
        _rootNode!.FillStackByIndex(stack, keyIndex);
    }

    public long FindLastWithPrefix(in ReadOnlySpan<byte> prefix)
    {
        if (_rootNode == null) return -1;
        return _rootNode.FindLastWithPrefix(prefix);
    }

    public bool NextIdxValid(int idx)
    {
        return false;
    }

    public void FillStackByLeftMost(List<NodeIdxPair> stack, int idx)
    {
        stack.Add(new NodeIdxPair { Node = _rootNode!, Idx = 0 });
        _rootNode.FillStackByLeftMost(stack, 0);
    }

    public void FillStackByRightMost(List<NodeIdxPair> stack, int idx)
    {
        throw new ArgumentException();
    }

    public int GetLastChildrenIdx()
    {
        return 0;
    }

    public IBTreeNode EraseRange(long transactionId, long firstKeyIndex, long lastKeyIndex)
    {
        throw new ArgumentException();
    }

    public IBTreeNode EraseOne(long transactionId, long keyIndex)
    {
        throw new ArgumentException();
    }

    public void Iterate(ValuesIterateAction action)
    {
        _rootNode?.Iterate(action);
    }

    public long TransactionId => _transactionId;

    public string? DescriptionForLeaks { get; set; }
    public uint TrLogFileId { get; set; }
    public uint TrLogOffset { get; set; }

    public int UseCount { get; set; }
    public ulong CommitUlong { get; set; }

    ulong[]? CloneUlongs()
    {
        ulong[]? newulongs = null;
        if (_ulongs != null)
        {
            newulongs = new ulong[_ulongs.Length];
            Array.Copy(_ulongs, newulongs, newulongs.Length);
        }
        return newulongs;
    }

    public IBTreeRootNode NewTransactionRoot()
    {
        return new BTreeRoot(_transactionId + 1)
        {
            _keyValueCount = _keyValueCount,
            _rootNode = _rootNode,
            TrLogFileId = TrLogFileId,
            TrLogOffset = TrLogOffset,
            CommitUlong = CommitUlong,
            _ulongs = CloneUlongs()
        };
    }

    public IBTreeRootNode CloneRoot()
    {
        return new BTreeRoot(_transactionId)
        {
            _keyValueCount = _keyValueCount,
            _rootNode = _rootNode,
            TrLogFileId = TrLogFileId,
            TrLogOffset = TrLogOffset,
            CommitUlong = CommitUlong,
            _ulongs = CloneUlongs()
        };
    }

    public void EraseOne(long keyIndex)
    {
        Debug.Assert(keyIndex >= 0);
        Debug.Assert(keyIndex < _keyValueCount);
        if (1 == _keyValueCount)
        {
            _rootNode = null;
            _keyValueCount = 0;
            return;
        }
        _keyValueCount--;
        _rootNode = _rootNode!.EraseOne(TransactionId, keyIndex);
    }

    public void EraseRange(long firstKeyIndex, long lastKeyIndex)
    {
        Debug.Assert(firstKeyIndex >= 0);
        Debug.Assert(lastKeyIndex < _keyValueCount);
        if (firstKeyIndex == 0 && lastKeyIndex == _keyValueCount - 1)
        {
            _rootNode = null;
            _keyValueCount = 0;
            return;
        }
        _keyValueCount -= lastKeyIndex - firstKeyIndex + 1;
        _rootNode = _rootNode!.EraseRange(TransactionId, firstKeyIndex, lastKeyIndex);
    }

    public bool FindNextKey(List<NodeIdxPair> stack)
    {
        var idx = stack.Count - 1;
        while (idx >= 0)
        {
            var pair = stack[idx];
            if (pair.Node.NextIdxValid(pair.Idx))
            {
                stack.RemoveRange(idx + 1, stack.Count - idx - 1);
                stack[idx] = new NodeIdxPair { Node = pair.Node, Idx = pair.Idx + 1 };
                pair.Node.FillStackByLeftMost(stack, pair.Idx + 1);
                return true;
            }
            idx--;
        }
        return false;
    }

    public bool FindPreviousKey(List<NodeIdxPair> stack)
    {
        int idx = stack.Count - 1;
        while (idx >= 0)
        {
            var pair = stack[idx];
            if (pair.Idx > 0)
            {
                stack.RemoveRange(idx + 1, stack.Count - idx - 1);
                stack[idx] = new NodeIdxPair { Node = pair.Node, Idx = pair.Idx - 1 };
                pair.Node.FillStackByRightMost(stack, pair.Idx - 1);
                return true;
            }
            idx--;
        }
        return false;
    }

    public void BuildTree(long keyCount, ref SpanReader reader, BuildTreeCallback memberGenerator)
    {
        _keyValueCount = keyCount;
        if (keyCount == 0)
        {
            _rootNode = null;
            return;
        }
        _rootNode = BuildTreeNode(keyCount, ref reader, memberGenerator);
    }

    IBTreeNode IBTreeNode.ReplaceValues(ReplaceValuesCtx ctx)
    {
        throw new InvalidOperationException();
    }

    public void CalcBTreeStats(RefDictionary<(uint Depth, uint Children), uint> stats, uint depth)
    {
        _rootNode?.CalcBTreeStats(stats, depth);
    }

    public void ReplaceValues(ReplaceValuesCtx ctx)
    {
        if (_rootNode == null) return;
        ctx._transactionId = TransactionId;
        _rootNode = _rootNode.ReplaceValues(ctx);
    }

    IBTreeNode BuildTreeNode(long keyCount, ref SpanReader outsideReader, BuildTreeCallback memberGenerator)
    {
        var leafs = (keyCount + BTreeLeafComp.MaxMembers - 1) / BTreeLeafComp.MaxMembers;
        var order = 0L;
        var done = 0L;
        return BuildBranchNode(leafs, ref outsideReader, (ref SpanReader reader) =>
        {
            order++;
            var reach = keyCount * order / leafs;
            var todo = (int)(reach - done);
            done = reach;
            var keyValues = new BTreeLeafMember[todo];
            long totalKeyLen = 0;
            for (var i = 0; i < keyValues.Length; i++)
            {
                keyValues[i] = memberGenerator(ref reader);
                totalKeyLen += keyValues[i].Key.Length;
            }
            if (totalKeyLen > BTreeLeafComp.MaxTotalLen)
            {
                return new BTreeLeaf(_transactionId, keyValues);
            }
            return new BTreeLeafComp(_transactionId, keyValues);
        });
    }

    IBTreeNode BuildBranchNode(long count, ref SpanReader reader, BuildBranchNodeGenerator generator)
    {
        if (count == 1) return generator(ref reader);
        var children = (count + BTreeBranch.MaxChildren - 1) / BTreeBranch.MaxChildren;
        var order = 0L;
        var done = 0L;
        return BuildBranchNode(children, ref reader, (ref SpanReader reader2) =>
        {
            order++;
            var reach = count * order / children;
            var todo = (int)(reach - done);
            done = reach;
            return new BTreeBranch(_transactionId, todo, ref reader2, generator);
        });
    }

    ulong[]? _ulongs;

    public ulong GetUlong(uint idx)
    {
        if (_ulongs == null) return 0;
        if (idx >= _ulongs.Length) return 0;
        return _ulongs[idx];
    }

    public void SetUlong(uint idx, ulong value)
    {
        if (_ulongs == null || idx >= _ulongs.Length)
            Array.Resize(ref _ulongs, (int)(idx + 1));
        _ulongs[idx] = value;
    }

    public ulong[]? UlongsArray
    {
        get => _ulongs;
        set => _ulongs = value;
    }
}
