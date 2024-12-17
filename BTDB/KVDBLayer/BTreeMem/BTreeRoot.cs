using System;
using System.Diagnostics;
using BTDB.Collections;

namespace BTDB.KVDBLayer.BTreeMem;

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
        ctx.Stack.Clear();
        if (_rootNode == null)
        {
            _rootNode = ctx.Key.Length > BTreeLeafComp.MaxTotalLen
                ? BTreeLeaf.CreateFirst(ref ctx)
                : BTreeLeafComp.CreateFirst(ref ctx);
            _keyValueCount = 1;
            ctx.Stack.Add(new() { Node = _rootNode, Idx = 0 });
            ctx.KeyIndex = 0;
            ctx.Created = true;
            return;
        }

        ctx.Depth = 0;
        _rootNode.CreateOrUpdate(ref ctx);
        if (ctx.Split)
        {
            _rootNode = new BTreeBranch(ctx.TransactionId, ctx.Node1!, ctx.Node2!);
            ctx.Stack.Insert(0) = new() { Node = _rootNode, Idx = ctx.SplitInRight ? 1 : 0 };
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

    public void UpdateKeySuffix(ref UpdateKeySuffixCtx ctx)
    {
        ctx.TransactionId = _transactionId;
        if (FindKey(ref ctx.Stack, out ctx.KeyIndex, ctx.Key[..(int)ctx.PrefixLen], ctx.PrefixLen) ==
            FindResult.NotFound)
        {
            ctx.Result = UpdateKeySuffixResult.NotFound;
            return;
        }

        if (FindNextKey(ref ctx.Stack))
        {
            if (KeyStartsWithPrefix(ctx.Key[..(int)ctx.PrefixLen], GetKeyFromStack(ref ctx.Stack).Span))
            {
                ctx.Result = UpdateKeySuffixResult.NotUniquePrefix;
                return;
            }

            FindPreviousKey(ref ctx.Stack);
        }

        if (ctx.Key.SequenceEqual(GetKeyFromStack(ref ctx.Stack).Span))
        {
            ctx.Result = UpdateKeySuffixResult.NothingToDo;
            return;
        }

        ctx.Depth = 0;
        _rootNode!.UpdateKeySuffix(ref ctx);
        if (ctx.Update)
        {
            _rootNode = ctx.Node;
        }
    }

    public FindResult FindKey(ref StructList<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key)
    {
        throw new InvalidOperationException();
    }

    public FindResult FindKey(ref StructList<NodeIdxPair> stack, out long keyIndex, in ReadOnlySpan<byte> key,
        uint prefixLen)
    {
        stack.Clear();
        if (_rootNode == null)
        {
            keyIndex = -1;
            return FindResult.NotFound;
        }

        var result = _rootNode.FindKey(ref stack, out keyIndex, key);
        if (result == FindResult.Previous)
        {
            if (keyIndex < 0)
            {
                keyIndex = 0;
                stack[^1] = new() { Node = stack[^1].Node, Idx = 0 };
                result = FindResult.Next;
            }
            else
            {
                if (!KeyStartsWithPrefix(key[..(int)prefixLen], GetKeyFromStack(ref stack).Span))
                {
                    result = FindResult.Next;
                    keyIndex++;
                    if (!FindNextKey(ref stack))
                    {
                        return FindResult.NotFound;
                    }
                }
            }

            if (!KeyStartsWithPrefix(key[..(int)prefixLen], GetKeyFromStack(ref stack).Span))
            {
                return FindResult.NotFound;
            }
        }

        return result;
    }

    internal static bool KeyStartsWithPrefix(in ReadOnlySpan<byte> prefix, in ReadOnlySpan<byte> key)
    {
        if (key.Length < prefix.Length) return false;
        return prefix.SequenceEqual(key[..prefix.Length]);
    }

    static ReadOnlyMemory<byte> GetKeyFromStack(ref StructList<NodeIdxPair> stack)
    {
        ref var last = ref stack.Last;
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

    public void FillStackByIndex(ref StructList<NodeIdxPair> stack, long keyIndex)
    {
        Debug.Assert(keyIndex >= 0 && keyIndex < _keyValueCount);
        stack.Clear();
        _rootNode!.FillStackByIndex(ref stack, keyIndex);
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

    public void FillStackByLeftMost(ref StructList<NodeIdxPair> stack, int idx)
    {
        stack.Add(new() { Node = _rootNode!, Idx = 0 });
        _rootNode!.FillStackByLeftMost(ref stack, 0);
    }

    public void FillStackByRightMost(ref StructList<NodeIdxPair> stack, int idx)
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

    public void CalcBTreeStats(RefDictionary<(uint Depth, uint Children), uint> stats, uint depth)
    {
        _rootNode?.CalcBTreeStats(stats, depth);
    }

    public void FastIterate(int deepness, ref StructList<NodeIdxPair> stack, ref long keyIndex,
        CursorIterateCallback callback)
    {
        throw new ArgumentException();
    }

    public long TransactionId => _transactionId;
    public ulong CommitUlong { get; set; }

    public IBTreeRootNode NewTransactionRoot()
    {
        ulong[]? newUlongs = null;
        if (_ulongs != null)
        {
            newUlongs = new ulong[_ulongs.Length];
            Array.Copy(_ulongs, newUlongs, newUlongs.Length);
        }

        return new BTreeRoot(_transactionId + 1)
            { _keyValueCount = _keyValueCount, _rootNode = _rootNode, CommitUlong = CommitUlong, _ulongs = newUlongs };
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

        if (firstKeyIndex == lastKeyIndex)
        {
            _keyValueCount--;
            _rootNode = _rootNode!.EraseOne(TransactionId, firstKeyIndex);
        }
        else
        {
            _keyValueCount -= lastKeyIndex - firstKeyIndex + 1;
            _rootNode = _rootNode!.EraseRange(TransactionId, firstKeyIndex, lastKeyIndex);
        }
    }

    public bool FindNextKey(ref StructList<NodeIdxPair> stack)
    {
        int idx = (int)stack.Count - 1;
        while (idx >= 0)
        {
            var pair = stack[idx];
            if (pair.Node.NextIdxValid(pair.Idx))
            {
                stack.SetCount((uint)idx + 1);
                stack[idx] = new() { Node = pair.Node, Idx = pair.Idx + 1 };
                pair.Node.FillStackByLeftMost(ref stack, pair.Idx + 1);
                return true;
            }

            idx--;
        }

        return false;
    }

    public bool FindPreviousKey(ref StructList<NodeIdxPair> stack)
    {
        var idx = (int)stack.Count - 1;
        while (idx >= 0)
        {
            var pair = stack[idx];
            if (pair.Idx > 0)
            {
                stack.SetCount((uint)idx + 1);
                stack[idx] = new() { Node = pair.Node, Idx = pair.Idx - 1 };
                pair.Node.FillStackByRightMost(ref stack, pair.Idx - 1);
                return true;
            }

            idx--;
        }

        return false;
    }

    public void BuildTree(long keyCount, Func<BTreeLeafMember> memberGenerator)
    {
        _keyValueCount = keyCount;
        if (keyCount == 0)
        {
            _rootNode = null;
            return;
        }

        _rootNode = BuildTreeNode(keyCount, memberGenerator);
    }

    public void FastIterate(ref StructList<NodeIdxPair> stack, ref long keyIndex, ref Span<byte> buffer,
        CursorIterateCallback callback)
    {
        if (_rootNode == null) return;
        _rootNode.FastIterate(0, ref stack, ref keyIndex, callback);
    }

    IBTreeNode BuildTreeNode(long keyCount, Func<BTreeLeafMember> memberGenerator)
    {
        var leafCount = (keyCount + BTreeLeafComp.MaxMembers - 1) / BTreeLeafComp.MaxMembers;
        var order = 0L;
        var done = 0L;
        return BuildBranchNode(leafCount, () =>
        {
            order++;
            var reach = keyCount * order / leafCount;
            var todo = (int)(reach - done);
            done = reach;
            var keyValues = new BTreeLeafMember[todo];
            long totalKeyLen = 0;
            for (var i = 0; i < keyValues.Length; i++)
            {
                keyValues[i] = memberGenerator();
                totalKeyLen += keyValues[i].Key.Length;
            }

            if (totalKeyLen > BTreeLeafComp.MaxTotalLen)
            {
                return new BTreeLeaf(_transactionId, keyValues);
            }

            return new BTreeLeafComp(_transactionId, keyValues);
        });
    }

    IBTreeNode BuildBranchNode(long count, Func<IBTreeNode> generator)
    {
        if (count == 1) return generator();
        var children = (count + BTreeBranch.MaxChildren - 1) / BTreeBranch.MaxChildren;
        var order = 0L;
        var done = 0L;
        return BuildBranchNode(children, () =>
        {
            order++;
            var reach = count * order / children;
            var todo = (int)(reach - done);
            done = reach;
            return new BTreeBranch(_transactionId, todo, generator);
        });
    }

    ulong[]? _ulongs;

    public ulong GetUlong(uint idx)
    {
        if (_ulongs == null) return 0;
        return idx >= _ulongs.Length ? 0 : _ulongs[idx];
    }

    public void SetUlong(uint idx, ulong value)
    {
        if (_ulongs == null || idx >= _ulongs.Length)
            Array.Resize(ref _ulongs, (int)(idx + 1));
        _ulongs[idx] = value;
    }

    public uint GetUlongCount()
    {
        return _ulongs == null ? 0U : (uint)_ulongs.Length;
    }

    public string? DescriptionForLeaks { get; set; }
}
