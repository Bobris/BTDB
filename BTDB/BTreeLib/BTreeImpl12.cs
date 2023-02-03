using BTDB.Allocators;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;
using BTDB.KVDBLayer.BTree;
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using BTDB.StreamLayer;

namespace BTDB.BTreeLib;

public class BTreeImpl12
{
    readonly IOffHeapAllocator _allocator;
    internal const int PtrSize = 12;
    internal const int MaxPrefixSize = 0xffff;
    internal const int MaxChildren = 30;

    internal BTreeImpl12(IOffHeapAllocator allocator)
    {
        _allocator = allocator;
    }

    public static IRootNode CreateEmptyRoot(IOffHeapAllocator allocator)
    {
        return new RootNode12(new BTreeImpl12(allocator));
    }

    internal ref struct KeyPusher
    {
        int _pos;
        internal ushort _prefixLen;
        internal bool _hasLongKeys;
        internal BTreeImpl12 _impl;
        internal Span<byte> _prefixBytes;
        internal Span<ushort> _keyOffsets;
        internal Span<byte> _keySuffixes;
        internal Span<IntPtr> _longKeys;

        internal void AddKey(in ReadOnlySpan<byte> key)
        {
            if (_pos == 0)
            {
                key.Slice(0, _prefixLen).CopyTo(_prefixBytes);
            }

            if (_hasLongKeys)
            {
                _longKeys[_pos++] = _impl.AllocateLongKey(key.Slice(_prefixLen));
            }
            else
            {
                var ofs = _keyOffsets[_pos];
                var len = key.Length - _prefixLen;
                _keyOffsets[++_pos] = (ushort)(ofs + len);
                key.Slice(_prefixLen).CopyTo(_keySuffixes.Slice(ofs, len));
            }
        }

        internal void AddKey(in ReadOnlySpan<byte> keyPrefix, in ReadOnlySpan<byte> keySuffix)
        {
            if (_pos == 0)
            {
                if (keyPrefix.Length >= _prefixLen)
                {
                    keyPrefix.Slice(0, _prefixLen).CopyTo(_prefixBytes);
                }
                else
                {
                    keyPrefix.CopyTo(_prefixBytes);
                    keySuffix.Slice(0, _prefixLen - keyPrefix.Length).CopyTo(_prefixBytes.Slice(keyPrefix.Length));
                }
            }

            if (keyPrefix.Length <= _prefixLen)
            {
                var key = keySuffix.Slice(_prefixLen - keyPrefix.Length);
                if (_hasLongKeys)
                {
                    _longKeys[_pos++] = _impl.AllocateLongKey(key);
                }
                else
                {
                    var ofs = _keyOffsets[_pos];
                    var len = key.Length;
                    _keyOffsets[++_pos] = (ushort)(ofs + len);
                    key.CopyTo(_keySuffixes.Slice(ofs, len));
                }

                return;
            }

            if (_hasLongKeys)
            {
                _longKeys[_pos++] = _impl.AllocateLongKey(_prefixLen, keyPrefix, keySuffix);
            }
            else
            {
                var ofs = _keyOffsets[_pos];
                var len = keyPrefix.Length + keySuffix.Length - _prefixLen;
                _keyOffsets[++_pos] = (ushort)(ofs + len);
                var dest = _keySuffixes.Slice(ofs, len);
                keyPrefix.Slice(_prefixLen).CopyTo(dest);
                dest = dest.Slice(keyPrefix.Length - _prefixLen);
                keySuffix.CopyTo(dest);
            }
        }

        internal void Finish()
        {
            if (_hasLongKeys)
            {
                Debug.Assert(_pos == _longKeys.Length);
            }
            else
            {
                Debug.Assert(_pos == _keyOffsets.Length - 1);
            }
        }
    }

    internal unsafe IntPtr AllocateLeaf(uint childCount, uint keyPrefixLength, ulong totalSuffixLength,
        out KeyPusher keyPusher)
    {
        Debug.Assert(keyPrefixLength <= MaxPrefixSize);
        keyPusher = new KeyPusher();
        var nodeType = NodeType12.IsLeaf;
        uint size = 8;
        size += keyPrefixLength;
        if (totalSuffixLength <= ushort.MaxValue)
        {
            size = TreeNodeUtils.AlignUIntUpInt16(size);
            size += 2 * childCount + 2;
            size += (uint)totalSuffixLength;
            size = TreeNodeUtils.AlignUIntUpInt32(size);
        }
        else
        {
            size = TreeNodeUtils.AlignUIntUpInt64(size);
            nodeType |= NodeType12.HasLongKeys;
            size += 8 * childCount;
            keyPusher._hasLongKeys = true;
        }

        keyPusher._prefixLen = (ushort)keyPrefixLength;
        size += 12 * childCount;
        var node = _allocator.Allocate((IntPtr)size);
        ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeaderInit(node);
        nodeHeader._nodeType = nodeType;
        nodeHeader._childCount = (byte)childCount;
        nodeHeader._keyPrefixLength = (ushort)keyPrefixLength;
        nodeHeader._referenceCount = 1;
        new Span<byte>(node.ToPointer(), (int)size).Slice(8).Clear();
        keyPusher._prefixBytes = NodeUtils12.GetPrefixSpan(node);
        keyPusher._impl = this;
        if (nodeHeader.HasLongKeys)
        {
            keyPusher._longKeys = NodeUtils12.GetLongKeyPtrs(node);
        }
        else
        {
            keyPusher._keyOffsets =
                NodeUtils12.GetKeySpans(node, (uint)totalSuffixLength, out keyPusher._keySuffixes);
        }

        return node;
    }

    internal ref struct BranchKeyPusher
    {
        int _pos;
        internal bool _hasLongKeys;
        internal BTreeImpl12 _impl;
        internal Span<ushort> _keyOffsets;
        internal Span<byte> _keySuffixes;
        internal Span<IntPtr> _longKeys;

        internal void AddKey(in ReadOnlySpan<byte> key)
        {
            if (_hasLongKeys)
            {
                _longKeys[_pos++] = _impl.AllocateLongKey(key);
            }
            else
            {
                var ofs = _keyOffsets[_pos];
                var len = key.Length;
                _keyOffsets[++_pos] = (ushort)(ofs + len);
                key.CopyTo(_keySuffixes.Slice(ofs, len));
            }
        }

        internal void AddKey(in ReadOnlySpan<byte> keyPrefix, in ReadOnlySpan<byte> keySuffix)
        {
            if (keyPrefix.Length == 0)
            {
                if (_hasLongKeys)
                {
                    _longKeys[_pos++] = _impl.AllocateLongKey(keySuffix);
                }
                else
                {
                    var ofs = _keyOffsets[_pos];
                    var len = keySuffix.Length;
                    _keyOffsets[++_pos] = (ushort)(ofs + len);
                    keySuffix.CopyTo(_keySuffixes.Slice(ofs, len));
                }

                return;
            }

            if (_hasLongKeys)
            {
                _longKeys[_pos++] = _impl.AllocateLongKey(0, keyPrefix, keySuffix);
            }
            else
            {
                var ofs = _keyOffsets[_pos];
                var len = keyPrefix.Length + keySuffix.Length;
                _keyOffsets[++_pos] = (ushort)(ofs + len);
                var dest = _keySuffixes.Slice(ofs, len);
                keyPrefix.CopyTo(dest);
                dest = dest.Slice(keyPrefix.Length);
                keySuffix.CopyTo(dest);
            }
        }

        internal void Finish()
        {
            if (_hasLongKeys)
            {
                Debug.Assert(_pos == _longKeys.Length);
            }
            else
            {
                Debug.Assert(_pos == _keyOffsets.Length - 1);
            }
        }
    }

    internal unsafe IntPtr AllocateBranch(uint childCount, ulong totalSuffixLength,
        out BranchKeyPusher keyPusher)
    {
        Debug.Assert(childCount > 0);
        keyPusher = new BranchKeyPusher();
        var nodeType = NodeType12.IsBranch;
        uint size = 16;
        if (totalSuffixLength <= ushort.MaxValue)
        {
            size = TreeNodeUtils.AlignUIntUpInt16(size);
            size += 2 * childCount;
            size += (uint)totalSuffixLength;
            size = TreeNodeUtils.AlignUIntUpInt64(size);
        }
        else
        {
            size = TreeNodeUtils.AlignUIntUpInt64(size);
            nodeType |= NodeType12.HasLongKeys;
            size += 8 * childCount - 8;
            keyPusher._hasLongKeys = true;
        }

        size += 8 * childCount;
        var node = _allocator.Allocate((IntPtr)size);
        ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeaderInit(node);
        nodeHeader._nodeType = nodeType;
        nodeHeader._keyPrefixLength = 0;
        nodeHeader._childCount = (byte)childCount;
        nodeHeader._referenceCount = 1;
        new Span<byte>(node.ToPointer(), (int)size).Slice(8).Clear();
        keyPusher._impl = this;
        if (nodeHeader.HasLongKeys)
        {
            keyPusher._longKeys = NodeUtils12.GetLongKeyPtrs(node);
        }
        else
        {
            keyPusher._keyOffsets =
                NodeUtils12.GetKeySpans(node, (uint)totalSuffixLength, out keyPusher._keySuffixes);
        }

        return node;
    }

    internal void BuildTree(RootNode12 rootNode, long keyCount, ref SpanReader reader, BuildTreeCallback generator)
    {
        Dereference(rootNode.Root);
        if (keyCount == 0)
        {
            rootNode.Root = IntPtr.Zero;
            return;
        }

        rootNode.Root = BuildTreeNode(keyCount, ref reader, generator);
    }

    internal IntPtr ValueReplacer(ref ValueReplacerCtx ctx, in Span<CursorItem> stack, int stackIdx)
    {
        ref var stackItem = ref stack[stackIdx];
        ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
        if (header.IsNodeLeaf)
        {
            if (ctx._afterFirst)
            {
                if (ctx._cancellation.IsCancellationRequested || DateTime.UtcNow > ctx._operationTimeout)
                {
                    ctx._interrupted = true;
                    if (header.HasLongKeys)
                    {
                        var keyPtr = NodeUtils12.GetLongKeyPtrs(stackItem._node)[0];
                        var suffix = NodeUtils12.LongKeyPtrToSpan(keyPtr);
                        var len = header._keyPrefixLength + suffix.Length;
                        ctx._interruptedKey = new byte[len];
                        NodeUtils12.GetPrefixSpan(stackItem._node).CopyTo(ctx._interruptedKey);
                        suffix.CopyTo(ctx._interruptedKey.AsSpan(header._keyPrefixLength));
                    }
                    else
                    {
                        var keyOffsets = NodeUtils12.GetKeySpans(stackItem._node, out var keySuffixes);
                        var lenSuffix = keyOffsets[1];
                        var len = header._keyPrefixLength + lenSuffix;
                        ctx._interruptedKey = new byte[len];
                        NodeUtils12.GetPrefixSpan(stackItem._node).CopyTo(ctx._interruptedKey);
                        keySuffixes.Slice(0, lenSuffix).CopyTo(ctx._interruptedKey.AsSpan(header._keyPrefixLength));
                    }

                    return stackItem._node;
                }
            }

            ctx._afterFirst = true;
            var cloned = false;
            var values = NodeUtils12.GetLeafValues(stackItem._node);
            for (var i = 0; i < values.Length; i += 12)
            {
                var value = values.Slice(i, 12);
                if (ctx._positionMap.TryGetValue(
                        ((ulong)MemoryMarshal.Read<uint>(value) << 32) + MemoryMarshal.Read<uint>(value.Slice(4)),
                        out var targetOfs))
                {
                    if (!cloned)
                    {
                        stackItem._node = CloneNode(stackItem._node);
                        values = NodeUtils12.GetLeafValues(stackItem._node);
                        value = values.Slice(i, 12);
                        cloned = true;
                    }

                    var valueFileId = ctx._targetFileId;
                    var valueFileOfs = targetOfs;
                    MemoryMarshal.Write(value, ref valueFileId);
                    MemoryMarshal.Write(value.Slice(4), ref valueFileOfs);
                }
            }
        }
        else
        {
            var children = NodeUtils12.GetBranchValuePtrs(stackItem._node);
            var cloned = false;
            var stackIdx1 = stackIdx + 1;
            ref var stackItem1 = ref stack[stackIdx1];
            for (var i = ctx._afterFirst ? 0 : stackItem._posInNode; i < children.Length; i++)
            {
                stackItem1._node = children[i];
                var newChildPtr = ValueReplacer(ref ctx, stack, stackIdx1);
                if (newChildPtr != children[i])
                {
                    if (!cloned)
                    {
                        stackItem._node = CloneNode(stackItem._node);
                        children = NodeUtils12.GetBranchValuePtrs(stackItem._node);
                        cloned = true;
                    }

                    Dereference(children[i]);
                    children[i] = newChildPtr;
                }

                if (ctx._interrupted) break;
            }
        }

        return stackItem._node;
    }

    IntPtr BuildTreeNode(long keyCount, ref SpanReader reader, BuildTreeCallback generator)
    {
        var leafs = (keyCount + MaxChildren - 1) / MaxChildren;
        var order = 0L;
        var done = 0L;
        var values = new byte[MaxChildren * 12];
        var keys = new ByteBuffer[MaxChildren];
        return BuildBranchNode(leafs, ref reader, (ref SpanReader reader) =>
        {
            order++;
            var reach = keyCount * order / leafs;
            var todo = (int)(reach - done);
            done = reach;
            long totalKeyLen = 0;
            for (var i = 0; i < todo; i++)
            {
                generator(ref reader, ref keys[i], values.AsSpan(i * 12, 12));
                totalKeyLen += keys[i].Length;
            }

            var newPrefixSize = TreeNodeUtils.CalcCommonPrefix(keys.AsSpan(0, todo));
            var newSuffixSize = totalKeyLen - todo * newPrefixSize;
            var newNode = AllocateLeaf((uint)todo, newPrefixSize, (ulong)newSuffixSize, out var keyPusher);
            for (var i = 0; i < todo; i++)
            {
                keyPusher.AddKey(keys[i].AsSyncReadOnlySpan());
            }

            keyPusher.Finish();
            values.AsSpan(0, todo * 12).CopyTo(NodeUtils12.GetLeafValues(newNode));
            return newNode;
        });
    }

    delegate IntPtr BuildBranchNodeGenerator(ref SpanReader reader);

    IntPtr BuildBranchNode(long count, ref SpanReader reader, BuildBranchNodeGenerator generator)
    {
        if (count == 1) return generator(ref reader);
        var children = (count + MaxChildren - 1) / MaxChildren;
        var order = 0L;
        var done = 0L;
        var nodes = new IntPtr[MaxChildren];
        return BuildBranchNode(children, ref reader, (ref SpanReader reader) =>
        {
            order++;
            var reach = count * order / children;
            var todo = (int)(reach - done);
            done = reach;
            var totalSuffixLength = 0UL;
            var recursiveChildCount = 0UL;
            for (var i = 0; i < todo; i++)
            {
                var child = generator(ref reader);
                nodes[i] = child;
                recursiveChildCount += NodeUtils12.Ptr2NodeHeader(child).RecursiveChildCount;
            }

            for (var i = 1; i < todo; i++)
            {
                var prefix = NodeUtils12.GetLeftestKey(nodes[i], out var suffix);
                totalSuffixLength += (uint)prefix.Length + (uint)suffix.Length;
            }

            var newNode = AllocateBranch((uint)todo, totalSuffixLength, out var keyPusher);
            NodeUtils12.Ptr2NodeHeader(newNode)._recursiveChildCount = recursiveChildCount;
            for (var i = 1; i < todo; i++)
            {
                var prefix = NodeUtils12.GetLeftestKey(nodes[i], out var suffix);
                keyPusher.AddKey(prefix, suffix);
            }

            keyPusher.Finish();
            nodes.AsSpan(0, todo).CopyTo(NodeUtils12.GetBranchValuePtrs(newNode));
            return newNode;
        });
    }

    internal static long CalcIndex(Span<CursorItem> stack)
    {
        if (stack.Length == 0)
            return -1;
        var res = 0L;
        for (var i = 0;; i++)
        {
            var node = stack[i]._node;
            var pos = stack[i]._posInNode;
            ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeader(node);
            if (nodeHeader.IsNodeLeaf)
            {
                return res + pos;
            }

            if (pos > 0)
            {
                var children = NodeUtils12.GetBranchValuePtrs(node);
                for (var j = 0; j < pos; j++)
                {
                    res += (long)NodeUtils12.Ptr2NodeHeader(children[j]).RecursiveChildCount;
                }
            }
        }
    }

    internal long EraseRange(RootNode12 rootNode, ref StructList<CursorItem> left, ref StructList<CursorItem> right)
    {
        Debug.Assert(left.Count == right.Count);
        var idx = 0u;
        IntPtr node;
        long count;
        while (true)
        {
            ref var leftItem = ref left[idx];
            ref var rightItem = ref right[idx];
            if (rightItem._posInNode < leftItem._posInNode)
                return 0;
            if (idx + 1 == left.Count)
            {
                (node, count) = EraseFromLeaf(leftItem._node, leftItem._posInNode, rightItem._posInNode);
                break;
            }

            if (leftItem._posInNode == rightItem._posInNode)
            {
                idx++;
                continue;
            }

            (node, count) = EraseFromBranch(leftItem._node, leftItem._posInNode, EraseLeftToEnd(left, idx + 1),
                rightItem._posInNode, EraseStartToRight(right, idx + 1));
            break;
        }

        up:
        if (idx == 0)
        {
            Dereference(rootNode.Root);
            rootNode.Root = node;
            goto finish;
        }

        idx--;
        ref var curItem = ref left[(int)idx];
        if (node == IntPtr.Zero)
        {
            node = EraseFromBranch(curItem._node, curItem._posInNode, IntPtr.Zero,
                curItem._posInNode, IntPtr.Zero).newNode;
            goto up;
        }

        node = OverwriteInEraseFromBranch(curItem._node, curItem._posInNode, node, -count);
        goto up;

        finish:
        left.Clear();
        right.Clear();
        while (rootNode.Root != IntPtr.Zero && NodeUtils12.Ptr2NodeHeader(rootNode.Root).IsDegenerated)
        {
            var newRoot = NodeUtils12.GetBranchValuePtr(rootNode.Root, 0);
            NodeUtils12.Reference(newRoot);
            Dereference(rootNode.Root);
            rootNode.Root = newRoot;
        }

        return count;
    }

    IntPtr EraseStartToRight(in StructList<CursorItem> right, uint idx)
    {
        if (idx + 1 == right.Count)
        {
            return EraseFromLeaf(right[idx]._node, 0, right[idx]._posInNode).newNode;
        }

        return EraseFromBranch(right[idx]._node, 0, IntPtr.Zero, right[idx]._posInNode,
            EraseStartToRight(right, idx + 1)).newNode;
    }

    IntPtr EraseLeftToEnd(in StructList<CursorItem> left, uint idx)
    {
        if (idx + 1 == left.Count)
        {
            return EraseFromLeaf(left[idx]._node, left[idx]._posInNode, int.MaxValue).newNode;
        }

        return EraseFromBranch(left[idx]._node, left[idx]._posInNode, EraseLeftToEnd(left, idx + 1), int.MaxValue,
            IntPtr.Zero).newNode;
    }

    IntPtr OverwriteInEraseFromBranch(IntPtr node, int leftPos, IntPtr leftChild, long delta)
    {
        ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
        Debug.Assert(!header.IsNodeLeaf);

        var leftChildPrefix = new Span<byte>();
        var leftChildSuffix = new Span<byte>();
        if (leftPos > 0)
        {
            leftChildPrefix = NodeUtils12.GetLeftestKey(leftChild, out leftChildSuffix);
        }

        var leftPosKey = leftPos > 0 ? leftPos - 1 : 0;
        var newSuffixLen =
            leftPos == 0
                ? NodeUtils12.GetTotalSuffixLen(node)
                : NodeUtils12.GetTotalSuffixLenExcept(node, leftPosKey, leftPosKey);
        newSuffixLen += leftChildPrefix.Length + leftChildSuffix.Length;

        var newNode = AllocateBranch(header._childCount, (ulong)newSuffixLen, out var keyPusher);
        if (header.HasLongKeys)
        {
            var longKeys = NodeUtils12.GetLongKeyPtrs(node);
            for (var i = 0; i < longKeys.Length; i++)
            {
                if (i == leftPosKey)
                {
                    if (leftPos > 0)
                    {
                        keyPusher.AddKey(leftChildPrefix, leftChildSuffix);
                        continue;
                    }
                }

                keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
            }
        }
        else
        {
            var keyOfs = NodeUtils12.GetKeySpans(node, out var keyData);
            for (var i = 0; i < keyOfs.Length - 1; i++)
            {
                if (i == leftPosKey)
                {
                    if (leftPos > 0)
                    {
                        keyPusher.AddKey(leftChildPrefix, leftChildSuffix);
                        continue;
                    }
                }

                keyPusher.AddKey(GetShortKey(keyOfs, keyData, i));
            }
        }

        keyPusher.Finish();
        var newValues = NodeUtils12.GetBranchValuePtrs(newNode);
        var oldValues = NodeUtils12.GetBranchValuePtrs(node);
        NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(0, leftPos), newValues);
        newValues = newValues.Slice(leftPos);
        newValues[0] = leftChild;
        newValues = newValues.Slice(1);

        NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(leftPos + 1), newValues);
        ref var newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
        newHeader._recursiveChildCount = (ulong)((long)header._recursiveChildCount + delta);

        return newNode;
    }

    (IntPtr newNode, long count) EraseFromBranch(IntPtr node, int leftPos, IntPtr leftChild, int rightPos,
        IntPtr rightChild)
    {
        ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
        Debug.Assert(!header.IsNodeLeaf);
        if (rightPos >= header._childCount)
        {
            rightPos = header._childCount - 1;
        }

        if (leftPos == rightPos)
        {
            if (leftChild == IntPtr.Zero)
            {
                leftChild = rightChild;
            }

            rightChild = IntPtr.Zero;
        }

        var newCount = header._childCount - (rightPos + 1 - leftPos) + (leftChild != IntPtr.Zero ? 1 : 0) +
                       (rightChild != IntPtr.Zero ? 1 : 0);
        if (newCount == 0)
        {
            return (IntPtr.Zero, (long)header.RecursiveChildCount);
        }

        var leftChildPrefix = new Span<byte>();
        var leftChildSuffix = new Span<byte>();
        var rightChildPrefix = new Span<byte>();
        var rightChildSuffix = new Span<byte>();
        if (leftChild != IntPtr.Zero && leftPos > 0)
        {
            leftChildPrefix = NodeUtils12.GetLeftestKey(leftChild, out leftChildSuffix);
        }

        if (rightChild != IntPtr.Zero && (leftPos > 0 || leftChild != IntPtr.Zero))
        {
            rightChildPrefix = NodeUtils12.GetLeftestKey(rightChild, out rightChildSuffix);
        }

        var leftPosKey = leftPos > 0 ? leftPos - 1 : 0;
        var rightPosKey = rightPos > 0 ? rightPos - 1 : 0;
        if (leftPos == 0 && rightPos > 0 && leftChild == IntPtr.Zero && rightChild == IntPtr.Zero) rightPosKey++;
        var newSuffixLen =
            (leftChild != IntPtr.Zero && rightPos == 0)
                ? NodeUtils12.GetTotalSuffixLen(node)
                : NodeUtils12.GetTotalSuffixLenExcept(node, leftPosKey, rightPosKey);
        if (leftChild != IntPtr.Zero)
        {
            newSuffixLen += leftChildPrefix.Length + leftChildSuffix.Length;
        }

        if (rightChild != IntPtr.Zero)
        {
            newSuffixLen += rightChildPrefix.Length + rightChildSuffix.Length;
        }

        var newNode = AllocateBranch((uint)newCount, (ulong)newSuffixLen, out var keyPusher);
        if (header.HasLongKeys)
        {
            var longKeys = NodeUtils12.GetLongKeyPtrs(node);
            for (var i = 0; i < longKeys.Length; i++)
            {
                if (i == leftPosKey)
                {
                    if (leftChild != IntPtr.Zero && leftPos > 0)
                    {
                        keyPusher.AddKey(leftChildPrefix, leftChildSuffix);
                    }

                    if (rightChild != IntPtr.Zero && (leftPos > 0 || leftChild != IntPtr.Zero))
                    {
                        keyPusher.AddKey(rightChildPrefix, rightChildSuffix);
                    }

                    i = rightPosKey;
                    if (leftChild == IntPtr.Zero || rightPos > 0)
                        continue;
                }

                keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
            }
        }
        else
        {
            var keyOfs = NodeUtils12.GetKeySpans(node, out var keyData);
            for (var i = 0; i < keyOfs.Length - 1; i++)
            {
                if (i == leftPosKey)
                {
                    if (leftChild != IntPtr.Zero && leftPos > 0)
                    {
                        keyPusher.AddKey(leftChildPrefix, leftChildSuffix);
                    }

                    if (rightChild != IntPtr.Zero && (leftPos > 0 || leftChild != IntPtr.Zero))
                    {
                        keyPusher.AddKey(rightChildPrefix, rightChildSuffix);
                    }

                    i = rightPosKey;
                    if (leftChild == IntPtr.Zero || rightPos > 0)
                        continue;
                }

                keyPusher.AddKey(GetShortKey(keyOfs, keyData, i));
            }
        }

        keyPusher.Finish();
        var newValues = NodeUtils12.GetBranchValuePtrs(newNode);
        var oldValues = NodeUtils12.GetBranchValuePtrs(node);
        NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(0, leftPos), newValues);
        newValues = newValues.Slice(leftPos);
        if (leftChild != IntPtr.Zero)
        {
            newValues[0] = leftChild;
            newValues = newValues.Slice(1);
        }

        if (rightChild != IntPtr.Zero)
        {
            newValues[0] = rightChild;
            newValues = newValues.Slice(1);
        }

        NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(rightPos + 1), newValues);
        return (newNode, (long)header.RecursiveChildCount - NodeUtils12.RecalcRecursiveChildrenCount(newNode));
    }

    (IntPtr newNode, long count) EraseFromLeaf(IntPtr node, int leftPos, int rightPos)
    {
        ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
        Debug.Assert(header.IsNodeLeaf);
        if (rightPos >= header._childCount)
        {
            rightPos = header._childCount - 1;
        }

        var newCount = header._childCount - (rightPos + 1 - leftPos);
        if (newCount == 0)
        {
            return (IntPtr.Zero, header._childCount);
        }

        var newPrefixLen = CalcCommonPrefixExcept(node, leftPos, rightPos);
        var newSuffixLen = NodeUtils12.GetTotalSuffixLenExcept(node, leftPos, rightPos) +
                           header._keyPrefixLength * newCount;
        newSuffixLen -= newPrefixLen * newCount;
        var newNode = AllocateLeaf((uint)newCount, (uint)newPrefixLen, (ulong)newSuffixLen, out var keyPusher);
        var prefixBytes = NodeUtils12.GetPrefixSpan(node);
        if (header.HasLongKeys)
        {
            var longKeys = NodeUtils12.GetLongKeyPtrs(node);
            for (var i = 0; i < longKeys.Length; i++)
            {
                if (i == leftPos)
                {
                    i = rightPos;
                    continue;
                }

                keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
            }
        }
        else
        {
            var keyOfs = NodeUtils12.GetKeySpans(node, out var keyData);
            for (var i = 0; i < keyOfs.Length - 1; i++)
            {
                if (i == leftPos)
                {
                    i = rightPos;
                    continue;
                }

                keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
            }
        }

        keyPusher.Finish();
        var newValues = NodeUtils12.GetLeafValues(newNode);
        var oldValues = NodeUtils12.GetLeafValues(node);
        oldValues.Slice(0, 12 * leftPos).CopyTo(newValues);
        oldValues.Slice((rightPos + 1) * 12).CopyTo(newValues.Slice(12 * leftPos));
        return (newNode, rightPos + 1 - leftPos);
    }

    internal bool SeekIndex(long index, IntPtr top, ref StructList<CursorItem> stack)
    {
        if (top == IntPtr.Zero)
        {
            return false;
        }

        ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
        if ((ulong)index >= header.RecursiveChildCount)
            return false;
        while (true)
        {
            if (header.IsNodeLeaf)
            {
                stack.AddRef().Set(top, (byte)index);
                return true;
            }

            var ptrs = NodeUtils12.GetBranchValuePtrs(top);
            for (var i = 0; i < ptrs.Length; i++)
            {
                var recursiveChildCount = NodeUtils12.Ptr2NodeHeader(ptrs[i]).RecursiveChildCount;
                if ((ulong)index < recursiveChildCount)
                {
                    stack.AddRef().Set(top, (byte)i);
                    top = ptrs[i];
                    header = ref NodeUtils12.Ptr2NodeHeader(top);
                    break;
                }

                index -= (long)recursiveChildCount;
            }
        }
    }

    internal bool MoveNext(ref StructList<CursorItem> stack)
    {
        while (stack.Count > 0)
        {
            ref var stackItem = ref stack[stack.Count - 1];
            ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
            stackItem._posInNode++;
            if (stackItem._posInNode < header._childCount)
            {
                while (!header.IsNodeLeaf)
                {
                    stack.AddRef().Set(NodeUtils12.GetBranchValuePtr(stackItem._node, stackItem._posInNode), 0);
                    stackItem = ref stack[stack.Count - 1];
                    header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
                }

                return true;
            }

            stack.Pop();
        }

        return false;
    }

    internal bool MovePrevious(ref StructList<CursorItem> stack)
    {
        while (stack.Count > 0)
        {
            ref var stackItem = ref stack[stack.Count - 1];
            ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
            if (stackItem._posInNode > 0)
            {
                stackItem._posInNode--;
                while (!header.IsNodeLeaf)
                {
                    stack.AddRef().Set(NodeUtils12.GetBranchValuePtr(stackItem._node, stackItem._posInNode), 0);
                    stackItem = ref stack[stack.Count - 1];
                    header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
                    stackItem._posInNode = (byte)(header._childCount - 1);
                }

                return true;
            }

            stack.Pop();
        }

        return false;
    }

    internal static bool MoveToLast(IntPtr root, ref StructList<CursorItem> stack)
    {
        stack.Clear();
        if (root == IntPtr.Zero)
            return false;
        ref var header = ref NodeUtils12.Ptr2NodeHeader(root);
        stack.AddRef().Set(root, (byte)(header._childCount - 1));
        while (!header.IsNodeLeaf)
        {
            ref var stackItem = ref stack[stack.Count - 1];
            stack.AddRef().Set(NodeUtils12.GetBranchValuePtr(stackItem._node, stackItem._posInNode), 0);
            header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
            stackItem._posInNode = (byte)(header._childCount - 1);
        }

        return true;
    }

    internal IntPtr CloneNode(IntPtr nodePtr)
    {
        var size = NodeUtils12.NodeSize(nodePtr);
        var newNode = _allocator.Allocate((IntPtr)size);
        TreeNodeUtils.CopyMemory(nodePtr, newNode, size);
        ref var header = ref NodeUtils12.Ptr2NodeHeader(newNode);
        header._referenceCount = 1;
        if (header.HasLongKeys)
        {
            var keys = NodeUtils12.GetLongKeyPtrs(newNode);
            for (var i = 0; i < keys.Length; i++)
            {
                keys[i] = CloneKey(keys[i]);
            }
        }

        ReferenceAllChildren(newNode);
        return newNode;
    }

    IntPtr CloneKey(IntPtr ptr)
    {
        var size = TreeNodeUtils.ReadInt32Aligned(ptr);
        var res = _allocator.Allocate((IntPtr)size + 4);
        TreeNodeUtils.CopyMemory(ptr, res, size + 4);
        return res;
    }

    void ReferenceAllChildren(IntPtr node)
    {
        if (NodeUtils12.Ptr2NodeHeader(node).IsNodeLeaf) return;
        var ptrs = NodeUtils12.GetBranchValuePtrs(node);
        for (var i = 0; i < ptrs.Length; i++)
        {
            NodeUtils12.Reference(ptrs[i]);
        }
    }

    void FreeLongKey(IntPtr ptr)
    {
        var size = TreeNodeUtils.ReadInt32Aligned(ptr) + 4;
        _allocator.Deallocate(ptr, (IntPtr)size);
    }

    internal void Dereference(IntPtr node)
    {
        if (node == IntPtr.Zero)
            return;
        ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeader(node);
        if (!nodeHeader.Dereference()) return;
        if (nodeHeader.HasLongKeys)
        {
            var ptrs = NodeUtils12.GetLongKeyPtrs(node);
            for (var i = 0; i < ptrs.Length; i++)
            {
                FreeLongKey(ptrs[i]);
            }
        }

        if (!nodeHeader.IsNodeLeaf)
        {
            var ptrs = NodeUtils12.GetBranchValuePtrs(node);
            for (var i = 0; i < ptrs.Length; i++)
            {
                Dereference(ptrs[i]);
            }
        }

        _allocator.Deallocate(node, (IntPtr)NodeUtils12.NodeSize(node));
    }

    static void CheckContent12(ReadOnlySpan<byte> content)
    {
        if (content.Length != 12) throw new ArgumentOutOfRangeException(nameof(content));
    }

    internal void WriteValue(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> content)
    {
        CheckContent12(content);
        MakeUnique(rootNode, stack.AsSpan());
        ref var stackItem = ref stack[stack.Count - 1];
        content.CopyTo(NodeUtils12.GetLeafValues(stackItem._node).Slice(stackItem._posInNode * 12, 12));
    }

    void MakeUnique(RootNode12 rootNode, Span<CursorItem> stack)
    {
        for (var i = 0; i < stack.Length; i++)
        {
            ref var stackItem = ref stack[i];
            ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
            if (header._referenceCount == 1)
                continue;
            var newNode = CloneNode(stackItem._node);
            OverwriteNodePtrInStack(rootNode, stack, i, newNode);
        }
    }

    void OverwriteNodePtrInStack(RootNode12 rootNode, Span<CursorItem> stack, int i, IntPtr newNode)
    {
        ref var stackItem = ref stack[i];
        stackItem._node = newNode;
        if (i == 0)
        {
            Dereference(rootNode.Root);
            rootNode.Root = newNode;
        }
        else
        {
            WritePtrInNode(stack[i - 1], newNode);
        }
    }

    void WritePtrInNode(in CursorItem stackItem, IntPtr newNode)
    {
        ref var ptr = ref NodeUtils12.GetBranchValuePtr(stackItem._node, stackItem._posInNode);
        Dereference(ptr);
        ptr = newNode;
    }

    internal unsafe bool FindExact(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> key)
    {
        stack.Clear();
        var top = rootNode.Root;
        if (top == IntPtr.Zero)
        {
            return false;
        }

        while (true)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(top);

            int idx;
            if (header.IsNodeLeaf)
            {
                if (header._keyPrefixLength > 0)
                {
                    var prefix = new Span<byte>((top + NodeHeader12.LeafHeaderSize).ToPointer(),
                        header._keyPrefixLength);
                    if (header._keyPrefixLength > key.Length)
                    {
                        stack.Clear();
                        return false;
                    }

                    var comp = key.Slice(0, header._keyPrefixLength).SequenceCompareTo(prefix);
                    if (comp != 0)
                    {
                        stack.Clear();
                        return false;
                    }
                }

                var keyRest = key.Slice(header._keyPrefixLength);
                if (header.HasLongKeys)
                {
                    var keys = NodeUtils12.GetLongKeyPtrs(top);
                    idx = BinarySearchLongKeys(keys, keyRest);
                }
                else
                {
                    var offsets = NodeUtils12.GetKeySpans(top, out var keys);
                    idx = BinarySearchKeys(offsets, keys, keyRest);
                }

                if ((idx & 1) != 0)
                {
                    stack.AddRef().Set(top, (byte)(idx >> 1));
                    return true;
                }

                stack.Clear();
                return false;
            }

            if (header.HasLongKeys)
            {
                var keys = NodeUtils12.GetLongKeyPtrs(top);
                idx = BinarySearchLongKeys(keys, key);
            }
            else
            {
                var offsets = NodeUtils12.GetKeySpans(top, out var keys);
                idx = BinarySearchKeys(offsets, keys, key);
            }

            idx = (idx + 1) >> 1;
            stack.AddRef().Set(top, (byte)idx);
            top = NodeUtils12.GetBranchValuePtr(top, idx);
        }
    }

    static int BinarySearchKeys(Span<ushort> offsets, Span<byte> keys, ReadOnlySpan<byte> key)
    {
        var l = 0;
        var r = offsets.Length - 1;
        while (l < r)
        {
            var m = (l + r) / 2;
            var start = offsets[m];
            var middleKey = keys.Slice(start, offsets[m + 1] - start);
            var comp = middleKey.SequenceCompareTo(key);
            if (comp == 0)
            {
                return m * 2 + 1;
            }

            if (comp < 0)
            {
                l = m + 1;
            }
            else
            {
                r = m;
            }
        }

        return l * 2;
    }

    static int BinarySearchKeysLast(Span<ushort> offsets, Span<byte> keys, ReadOnlySpan<byte> key)
    {
        var l = 0;
        var r = offsets.Length - 1;
        while (l < r)
        {
            var m = (l + r) / 2;
            var start = offsets[m];
            var middleKey = keys.Slice(start, Math.Min(offsets[m + 1] - start, key.Length));
            var comp = middleKey.SequenceCompareTo(key);
            if (comp > 0)
            {
                r = m;
            }
            else
            {
                l = m + 1;
            }
        }

        return l;
    }

    static int BinarySearchKeysLastLeaf(Span<ushort> offsets, Span<byte> keys, ReadOnlySpan<byte> key)
    {
        var l = 0;
        var r = offsets.Length - 2;
        ushort start;
        Span<byte> middleKey;
        int comp;
        while (l < r)
        {
            var m = (l + r) / 2;
            start = offsets[m];
            middleKey = keys.Slice(start, Math.Min(offsets[m + 1] - start, key.Length));
            comp = middleKey.SequenceCompareTo(key);
            if (comp > 0)
            {
                r = m;
            }
            else
            {
                l = m + 1;
            }
        }

        start = offsets[l];
        middleKey = keys.Slice(start, Math.Min(offsets[l + 1] - start, key.Length));
        comp = middleKey.SequenceCompareTo(key);
        if (comp > 0) l--;

        return l;
    }

    static int BinarySearchLongKeys(Span<IntPtr> keys, ReadOnlySpan<byte> key)
    {
        var l = 0;
        var r = keys.Length;
        while (l < r)
        {
            var m = (l + r) / 2;
            var middleKey = NodeUtils12.LongKeyPtrToSpan(keys[m]);
            var comp = middleKey.SequenceCompareTo(key);
            if (comp == 0)
            {
                return m * 2 + 1;
            }

            if (comp < 0)
            {
                l = m + 1;
            }
            else
            {
                r = m;
            }
        }

        return l * 2;
    }

    static int BinarySearchLongKeysLast(Span<IntPtr> keys, ReadOnlySpan<byte> key)
    {
        var l = 0;
        var r = keys.Length;
        while (l < r)
        {
            var m = (l + r) / 2;
            var middleKey = NodeUtils12.LongKeyPtrToSpan(keys[m]);
            var comp = middleKey.Slice(0, Math.Min(middleKey.Length, key.Length)).SequenceCompareTo(key);
            if (comp > 0)
            {
                r = m;
            }
            else
            {
                l = m + 1;
            }
        }

        return l;
    }

    static int BinarySearchLongKeysLastLeaf(Span<IntPtr> keys, ReadOnlySpan<byte> key)
    {
        var l = 0;
        var r = keys.Length - 1;
        Span<byte> middleKey;
        int comp;
        while (l < r)
        {
            var m = (l + r) / 2;
            middleKey = NodeUtils12.LongKeyPtrToSpan(keys[m]);
            comp = middleKey.Slice(0, Math.Min(middleKey.Length, key.Length)).SequenceCompareTo(key);
            if (comp > 0)
            {
                r = m;
            }
            else
            {
                l = m + 1;
            }
        }

        middleKey = NodeUtils12.LongKeyPtrToSpan(keys[l]);
        comp = middleKey.Slice(0, Math.Min(middleKey.Length, key.Length)).SequenceCompareTo(key);
        if (comp > 0) l--;
        return l;
    }

    internal static unsafe FindResult Find(RootNode12 rootNode, ref StructList<CursorItem> stack,
        ReadOnlySpan<byte> key)
    {
        stack.Clear();
        var top = rootNode.Root;
        if (top == IntPtr.Zero)
        {
            return FindResult.NotFound;
        }

        while (true)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
            if (header._keyPrefixLength > 0)
            {
                var prefix = new Span<byte>((top + (int)header.Size).ToPointer(), header._keyPrefixLength);
                var comp = key.Slice(0, Math.Min(key.Length, header._keyPrefixLength)).SequenceCompareTo(prefix);
                if (comp < 0)
                {
                    stack.AddRef().Set(top, 0);
                    if (header.IsNodeLeaf)
                    {
                        return FindResult.Next;
                    }

                    top = NodeUtils12.GetBranchValuePtr(top, 0);
                    continue;
                }

                if (comp > 0)
                {
                    stack.AddRef().Set(top, (byte)(header._childCount - 1));
                    if (header.IsNodeLeaf)
                    {
                        return FindResult.Previous;
                    }

                    top = NodeUtils12.GetBranchValuePtr(top, header._childCount - 1);
                    continue;
                }
            }

            var keyRest = key.Slice(header._keyPrefixLength);
            int idx;
            if (header.HasLongKeys)
            {
                var keys = NodeUtils12.GetLongKeyPtrs(top);
                idx = BinarySearchLongKeys(keys, keyRest);
            }
            else
            {
                var offsets = NodeUtils12.GetKeySpans(top, out var keys);
                idx = BinarySearchKeys(offsets, keys, keyRest);
            }

            if (header.IsNodeLeaf)
            {
                if ((idx & 1) != 0)
                {
                    stack.AddRef().Set(top, (byte)(idx >> 1));
                    return FindResult.Exact;
                }

                if (idx == 0)
                {
                    stack.AddRef().Set(top, 0);
                    return FindResult.Next;
                }

                stack.AddRef().Set(top, (byte)((idx - 1) >> 1));
                return FindResult.Previous;
            }

            idx = (idx + 1) >> 1;
            stack.AddRef().Set(top, (byte)idx);
            top = NodeUtils12.GetBranchValuePtr(top, idx);
        }
    }

    internal bool FindFirst(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
    {
        stack.Clear();
        var top = rootNode.Root;
        if (top == IntPtr.Zero)
        {
            return false;
        }

        while (true)
        {
            var idx = BinarySearch(top, keyPrefix);
            ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
            if (header.IsNodeLeaf)
            {
                if ((idx & 1) == 1)
                {
                    stack.AddRef().Set(top, (byte)(idx / 2));
                    return true;
                }

                idx = (int)((uint)idx / 2);
                if (idx < header._childCount && IsKeyPrefix(top, idx, keyPrefix))
                {
                    stack.AddRef().Set(top, (byte)idx);
                    return true;
                }

                if (idx == header._childCount)
                {
                    stack.AddRef().Set(top, (byte)(idx - 1));
                    if (MoveNext(ref stack) && IsKeyPrefix(stack[stack.Count - 1]._node,
                            stack[stack.Count - 1]._posInNode, keyPrefix))
                    {
                        return true;
                    }
                }

                stack.Clear();
                return false;
            }

            if ((idx & 1) == 1)
            {
                idx++;
            }

            idx = (int)((uint)idx / 2);
            stack.AddRef().Set(top, (byte)idx);
            top = NodeUtils12.GetBranchValuePtr(top, idx);
        }
    }

    internal static bool IsKeyPrefix(IntPtr nodePtr, int idx, ReadOnlySpan<byte> prefix)
    {
        ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
        var nodePrefix = NodeUtils12.GetPrefixSpan(nodePtr);
        if (nodePrefix.Length >= prefix.Length)
        {
            return nodePrefix.Slice(0, prefix.Length).SequenceEqual(prefix);
        }

        if (nodePrefix.Length > 0)
        {
            if (!nodePrefix.SequenceEqual(prefix.Slice(0, nodePrefix.Length)))
                return false;
            prefix = prefix.Slice(nodePrefix.Length);
        }

        if (header.HasLongKeys)
        {
            var keys = NodeUtils12.GetLongKeyPtrs(nodePtr);
            return TreeNodeUtils.IsPrefix(NodeUtils12.LongKeyPtrToSpan(keys[idx]), prefix);
        }
        else
        {
            var keyOffsets = NodeUtils12.GetKeySpans(nodePtr, out var keySuffixes);
            var ofs = keyOffsets[idx];
            var lenSuffix = keyOffsets[idx + 1] - ofs;
            return TreeNodeUtils.IsPrefix(keySuffixes.Slice(ofs, lenSuffix), prefix);
        }
    }

    internal static unsafe long FindLastWithPrefix(RootNode12 rootNode, in ReadOnlySpan<byte> keyPrefix)
    {
        var top = rootNode.Root;
        if (top == IntPtr.Zero)
        {
            return -1;
        }

        long res = 0;
        while (true)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
            if (header._keyPrefixLength > 0)
            {
                var prefix = new Span<byte>((top + (int)header.Size).ToPointer(), header._keyPrefixLength);
                var commonLen = Math.Min(prefix.Length, keyPrefix.Length);
                var comp = prefix.Slice(0, commonLen).SequenceCompareTo(keyPrefix.Slice(0, commonLen));
                if (comp > 0)
                {
                    if (header.IsNodeLeaf) return res - 1;
                    top = NodeUtils12.GetBranchValuePtr(top, 0);
                    continue;
                }

                if (comp < 0 || prefix.Length >= keyPrefix.Length)
                {
                    if (header.IsNodeLeaf)
                    {
                        if (comp < 0)
                        {
                            return -1;
                        }

                        return res + header._childCount - 1;
                    }

                    // Trick here is to calculate children from back
                    res += (long)header._recursiveChildCount;
                    top = NodeUtils12.GetBranchValuePtr(top, header._childCount - 1);
                    res -= (long)NodeUtils12.Ptr2NodeHeader(top).RecursiveChildCount;
                    continue;
                }
            }

            var keyRest = keyPrefix.Slice(header._keyPrefixLength);
            if (header.IsNodeLeaf)
            {
                if (header.HasLongKeys)
                {
                    var keys = NodeUtils12.GetLongKeyPtrs(top);
                    return res + BinarySearchLongKeysLastLeaf(keys, keyRest);
                }
                else
                {
                    var offsets = NodeUtils12.GetKeySpans(top, out var keys);
                    return res + BinarySearchKeysLastLeaf(offsets, keys, keyRest);
                }
            }

            int idx;
            if (header.HasLongKeys)
            {
                var keys = NodeUtils12.GetLongKeyPtrs(top);
                idx = BinarySearchLongKeysLast(keys, keyRest);
            }
            else
            {
                var offsets = NodeUtils12.GetKeySpans(top, out var keys);
                idx = BinarySearchKeysLast(offsets, keys, keyRest);
            }

            if (idx > 0)
            {
                var children = NodeUtils12.GetBranchValuePtrs(top);
                for (var j = 0; j < idx; j++)
                {
                    res += (long)NodeUtils12.Ptr2NodeHeader(children[j]).RecursiveChildCount;
                }
            }

            top = NodeUtils12.GetBranchValuePtr(top, idx);
        }
    }

    internal bool Upsert(RootNode12 rootNode, ref StructList<CursorItem> stack, in ReadOnlySpan<byte> key,
        in ReadOnlySpan<byte> content)
    {
        CheckContent12(content);
        stack.Clear();
        var top = rootNode.Root;
        if (top == IntPtr.Zero)
        {
            KeyPusher keyPusher;
            if (key.Length > MaxPrefixSize)
            {
                top = AllocateLeaf(1, MaxPrefixSize, (uint)key.Length - MaxPrefixSize, out keyPusher);
            }
            else
            {
                top = AllocateLeaf(1, (ushort)key.Length, 0, out keyPusher);
            }

            keyPusher.AddKey(key);
            keyPusher.Finish();
            content.CopyTo(NodeUtils12.GetLeafValues(top));
            stack.AddRef().Set(top, 0);
            rootNode.Root = top;
            return true;
        }

        while (true)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
            var idx = BinarySearch(top, key);

            if (!header.IsNodeLeaf)
            {
                idx = (int)((uint)(idx + 1) / 2);
                stack.AddRef().Set(top, (byte)idx);
                top = NodeUtils12.GetBranchValuePtr(top, idx);
                continue;
            }

            if ((idx & 1) != 0)
            {
                // Key found => Update
                stack.AddRef().Set(top, (byte)(idx / 2));
                WriteValue(rootNode, ref stack, content);
                return false;
            }

            idx = idx / 2;
            if (header._childCount < MaxChildren)
            {
                var newPrefixLen = CalcCommonPrefix(top, key);
                var newSuffixLen = NodeUtils12.GetTotalSuffixLen(top) +
                                   header._keyPrefixLength * header._childCount;
                newSuffixLen -= newPrefixLen * (header._childCount + 1) - key.Length;
                var newNode = AllocateLeaf((uint)header._childCount + 1, (uint)newPrefixLen, (ulong)newSuffixLen,
                    out var keyPusher);
                var prefixBytes = NodeUtils12.GetPrefixSpan(top);
                if (header.HasLongKeys)
                {
                    var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                    for (var i = 0; i < idx; i++)
                    {
                        keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                    }

                    keyPusher.AddKey(key);
                    for (var i = idx; i < longKeys.Length; i++)
                    {
                        keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                    }
                }
                else
                {
                    var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                    for (var i = 0; i < idx; i++)
                    {
                        keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                    }

                    keyPusher.AddKey(key);
                    for (var i = idx; i < keyOfs.Length - 1; i++)
                    {
                        keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                    }
                }

                keyPusher.Finish();
                var newValues = NodeUtils12.GetLeafValues(newNode);
                var oldValues = NodeUtils12.GetLeafValues(top);
                oldValues.Slice(0, 12 * idx).CopyTo(newValues);
                newValues = newValues.Slice(12 * idx);
                content.CopyTo(newValues);
                newValues = newValues.Slice(12);
                oldValues.Slice(12 * idx).CopyTo(newValues);
                MakeUnique(rootNode, stack.AsSpan());
                AdjustRecursiveChildCount(stack.AsSpan(), 1);
                stack.AddRef().Set(top, (byte)idx);
                OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, newNode);
                return true;
            }
            else
            {
                if (idx < MaxChildren / 2)
                {
                    var newPrefixLen = CalcCommonPrefix(top, 0, MaxChildren / 2, key);
                    var newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, 0, MaxChildren / 2) +
                                       header._keyPrefixLength * (MaxChildren / 2);
                    newSuffixLen -= newPrefixLen * (MaxChildren / 2 + 1) - key.Length;
                    var newNode = AllocateLeaf((uint)MaxChildren / 2 + 1, (uint)newPrefixLen,
                        (ulong)newSuffixLen, out var keyPusher);
                    var prefixBytes = NodeUtils12.GetPrefixSpan(top);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = 0; i < idx; i++)
                        {
                            keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }

                        keyPusher.AddKey(key);
                        for (var i = idx; i < MaxChildren / 2; i++)
                        {
                            keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = 0; i < idx; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }

                        keyPusher.AddKey(key);
                        for (var i = idx; i < MaxChildren / 2; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    var newValues = NodeUtils12.GetLeafValues(newNode);
                    var oldValues = NodeUtils12.GetLeafValues(top);
                    oldValues.Slice(0, 12 * idx).CopyTo(newValues);
                    newValues = newValues.Slice(12 * idx);
                    content.CopyTo(newValues);
                    newValues = newValues.Slice(12);
                    oldValues.Slice(12 * idx, newValues.Length).CopyTo(newValues);
                    oldValues = oldValues.Slice(12 * idx + newValues.Length);

                    newPrefixLen = CalcCommonPrefix(top, MaxChildren / 2, MaxChildren);
                    var rightCount = MaxChildren - MaxChildren / 2;
                    newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, MaxChildren / 2, MaxChildren) +
                                   header._keyPrefixLength * rightCount;
                    newSuffixLen -= newPrefixLen * rightCount;
                    var newNode2 = AllocateLeaf((uint)rightCount, (uint)newPrefixLen, (ulong)newSuffixLen,
                        out keyPusher);
                    prefixBytes = NodeUtils12.GetPrefixSpan(top);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = MaxChildren / 2; i < MaxChildren; i++)
                        {
                            keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = MaxChildren / 2; i < MaxChildren; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    newValues = NodeUtils12.GetLeafValues(newNode2);
                    oldValues.CopyTo(newValues);
                    stack.AddRef().Set(newNode, (byte)idx);
                    var splitInserter = new SplitInserter(this, rootNode, newNode, newNode2);
                    splitInserter.Run(ref stack, false);
                }
                else
                {
                    var newPrefixLen = CalcCommonPrefix(top, 0, MaxChildren / 2);
                    var newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, 0, MaxChildren / 2) +
                                       header._keyPrefixLength * (MaxChildren / 2);
                    newSuffixLen -= newPrefixLen * (MaxChildren / 2);
                    var newNode = AllocateLeaf((uint)MaxChildren / 2, (uint)newPrefixLen, (ulong)newSuffixLen,
                        out var keyPusher);
                    var prefixBytes = NodeUtils12.GetPrefixSpan(top);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = 0; i < MaxChildren / 2; i++)
                        {
                            keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = 0; i < MaxChildren / 2; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    var newValues = NodeUtils12.GetLeafValues(newNode);
                    var oldValues = NodeUtils12.GetLeafValues(top);
                    oldValues.Slice(0, 12 * (MaxChildren / 2)).CopyTo(newValues);
                    oldValues = oldValues.Slice(12 * (MaxChildren / 2));

                    newPrefixLen = CalcCommonPrefix(top, MaxChildren / 2, MaxChildren, key);
                    var rightCount = MaxChildren - MaxChildren / 2 + 1;
                    newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, MaxChildren / 2, MaxChildren) +
                                   header._keyPrefixLength * (rightCount - 1);
                    newSuffixLen -= newPrefixLen * rightCount - key.Length;
                    var newNode2 = AllocateLeaf((uint)rightCount, (uint)newPrefixLen, (ulong)newSuffixLen,
                        out keyPusher);
                    prefixBytes = NodeUtils12.GetPrefixSpan(top);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = MaxChildren / 2; i < idx; i++)
                        {
                            keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }

                        keyPusher.AddKey(key);
                        for (var i = idx; i < MaxChildren; i++)
                        {
                            keyPusher.AddKey(prefixBytes, NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = MaxChildren / 2; i < idx; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }

                        keyPusher.AddKey(key);
                        for (var i = idx; i < MaxChildren; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    newValues = NodeUtils12.GetLeafValues(newNode2);
                    oldValues.Slice(0, 12 * (idx - MaxChildren / 2)).CopyTo(newValues);
                    newValues = newValues.Slice(12 * (idx - MaxChildren / 2));
                    content.CopyTo(newValues);
                    newValues = newValues.Slice(12);
                    oldValues.Slice(12 * (idx - MaxChildren / 2)).CopyTo(newValues);
                    stack.AddRef().Set(newNode2, (byte)(idx - MaxChildren / 2));
                    var splitInserter = new SplitInserter(this, rootNode, newNode, newNode2);
                    splitInserter.Run(ref stack, true);
                }

                return true;
            }
        }
    }

    ReadOnlySpan<byte> GetShortKey(ReadOnlySpan<ushort> keyOfs, ReadOnlySpan<byte> keyData, int idx)
    {
        var start = keyOfs[idx];
        return keyData.Slice(start, keyOfs[idx + 1] - start);
    }

    int CalcCommonPrefix(IntPtr nodePtr, in ReadOnlySpan<byte> key)
    {
        var prefix = NodeUtils12.GetPrefixSpan(nodePtr);
        return TreeNodeUtils.FindFirstDifference(prefix, key);
    }

    int CalcCommonPrefix(IntPtr nodePtr, int startIdx, int endIdx, in ReadOnlySpan<byte> key)
    {
        var prefix = NodeUtils12.GetPrefixSpan(nodePtr);
        var common = TreeNodeUtils.FindFirstDifference(prefix, key);
        if (common < prefix.Length || key.Length == common)
            return common;
        ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
        var keyWithoutPrefix = key.Slice(prefix.Length);
        common = keyWithoutPrefix.Length;
        if (header.HasLongKeys)
        {
            var longKeys = NodeUtils12.GetLongKeyPtrs(nodePtr);
            for (var i = startIdx; i < endIdx; i++)
            {
                var newCommon =
                    TreeNodeUtils.FindFirstDifference(NodeUtils12.LongKeyPtrToSpan(longKeys[i]), keyWithoutPrefix);
                if (newCommon < common)
                {
                    common = newCommon;
                    if (common == 0)
                        return prefix.Length;
                }
            }
        }
        else
        {
            var keyOfs = NodeUtils12.GetKeySpans(nodePtr, out var keyData);
            for (var i = startIdx; i < endIdx; i++)
            {
                var newCommon =
                    TreeNodeUtils.FindFirstDifference(GetShortKey(keyOfs, keyData, i), keyWithoutPrefix);
                if (newCommon < common)
                {
                    common = newCommon;
                    if (common == 0)
                        return prefix.Length;
                }
            }
        }

        return ClampPrefixSize(prefix.Length + common);
    }

    int CalcCommonPrefix(IntPtr nodePtr, int startIdx, int endIdx)
    {
        var prefix = NodeUtils12.GetPrefixSpan(nodePtr);
        ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
        var common = MaxPrefixSize;
        ReadOnlySpan<byte> first;
        if (header.HasLongKeys)
        {
            var longKeys = NodeUtils12.GetLongKeyPtrs(nodePtr);
            first = NodeUtils12.LongKeyPtrToSpan(longKeys[startIdx]);
            for (var i = startIdx + 1; i < endIdx; i++)
            {
                var newCommon = TreeNodeUtils.FindFirstDifference(NodeUtils12.LongKeyPtrToSpan(longKeys[i]), first);
                if (newCommon < common)
                {
                    common = newCommon;
                    if (common == 0)
                        return prefix.Length;
                }
            }
        }
        else
        {
            var keyOfs = NodeUtils12.GetKeySpans(nodePtr, out var keyData);
            first = GetShortKey(keyOfs, keyData, startIdx);
            for (var i = startIdx + 1; i < endIdx; i++)
            {
                var newCommon = TreeNodeUtils.FindFirstDifference(GetShortKey(keyOfs, keyData, i), first);
                if (newCommon < common)
                {
                    common = newCommon;
                    if (common == 0)
                        return prefix.Length;
                }
            }
        }

        return ClampPrefixSize(prefix.Length + common);
    }

    static int ClampPrefixSize(int prefixSize)
    {
        return Math.Min(prefixSize, MaxPrefixSize);
    }

    int CalcCommonPrefixExcept(IntPtr nodePtr, int startIdx, int endIdx)
    {
        var prefix = NodeUtils12.GetPrefixSpan(nodePtr);
        ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
        if (startIdx == 0 && endIdx + 1 == header.KeyCount)
        {
            return 0;
        }

        var common = int.MaxValue;
        ReadOnlySpan<byte> first;
        if (header.HasLongKeys)
        {
            var longKeys = NodeUtils12.GetLongKeyPtrs(nodePtr);
            first = NodeUtils12.LongKeyPtrToSpan(longKeys[startIdx == 0 ? endIdx + 1 : 0]);
            for (var i = 0; i < longKeys.Length; i++)
            {
                if (i == startIdx)
                {
                    i = endIdx;
                    continue;
                }

                var newCommon = TreeNodeUtils.FindFirstDifference(NodeUtils12.LongKeyPtrToSpan(longKeys[i]), first);
                if (newCommon < common)
                {
                    common = newCommon;
                    if (common == 0)
                        return prefix.Length;
                }
            }
        }
        else
        {
            var keyOfs = NodeUtils12.GetKeySpans(nodePtr, out var keyData);
            first = GetShortKey(keyOfs, keyData, startIdx == 0 ? endIdx + 1 : 0);
            for (var i = 0; i < keyOfs.Length - 1; i++)
            {
                if (i == startIdx)
                {
                    i = endIdx;
                    continue;
                }

                var newCommon = TreeNodeUtils.FindFirstDifference(GetShortKey(keyOfs, keyData, i), first);
                if (newCommon < common)
                {
                    common = newCommon;
                    if (common == 0)
                        return prefix.Length;
                }
            }
        }

        return ClampPrefixSize(prefix.Length + common);
    }

    int BinarySearch(IntPtr nodePtr, in ReadOnlySpan<byte> keyWhole)
    {
        ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
        var prefix = NodeUtils12.GetPrefixSpan(nodePtr);
        var key = keyWhole;
        if (prefix.Length > 0)
        {
            if (key.Length < prefix.Length)
            {
                if (key.SequenceCompareTo(prefix) < 0)
                {
                    return 0;
                }
                else
                {
                    return 2 * header.KeyCount;
                }
            }

            var comp = key.Slice(0, prefix.Length).SequenceCompareTo(prefix);
            if (comp < 0)
            {
                return 0;
            }

            if (comp > 0)
            {
                return 2 * header.KeyCount;
            }

            key = key.Slice(prefix.Length);
        }

        int idx;
        if (header.HasLongKeys)
        {
            var keys = NodeUtils12.GetLongKeyPtrs(nodePtr);
            idx = BinarySearchLongKeys(keys, key);
        }
        else
        {
            var offsets = NodeUtils12.GetKeySpans(nodePtr, out var keys);
            idx = BinarySearchKeys(offsets, keys, key);
        }

        return idx;
    }

    unsafe IntPtr AllocateLongKey(in ReadOnlySpan<byte> data)
    {
        var res = _allocator.Allocate((IntPtr)(4 + data.Length));
        TreeNodeUtils.WriteInt32Aligned(res, data.Length);
        data.CopyTo(new Span<byte>((res + 4).ToPointer(), data.Length));
        return res;
    }

    unsafe IntPtr AllocateLongKey(ushort prefixLen, ReadOnlySpan<byte> keyPrefix, ReadOnlySpan<byte> keySuffix)
    {
        if (prefixLen >= keyPrefix.Length)
        {
            return AllocateLongKey(keySuffix.Slice(prefixLen - keyPrefix.Length));
        }

        var totalLen = keyPrefix.Length + keySuffix.Length - prefixLen;
        var res = _allocator.Allocate((IntPtr)(4 + totalLen));
        TreeNodeUtils.WriteInt32Aligned(res, totalLen);
        var dest = new Span<byte>((res + 4).ToPointer(), totalLen);
        keyPrefix.Slice(prefixLen).CopyTo(dest);
        dest = dest.Slice(keyPrefix.Length - prefixLen);
        keySuffix.CopyTo(dest);
        return res;
    }

    void AdjustRecursiveChildCount(in Span<CursorItem> stack, long delta)
    {
        for (var i = 0; i < stack.Length; i++)
        {
            ref var stackItem = ref stack[i];
            ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
            header._recursiveChildCount = (ulong)unchecked((long)header._recursiveChildCount + delta);
        }
    }

    struct SplitInserter
    {
        RootNode12 _rootNode;
        IntPtr _newChildNode;
        IntPtr _newChildNode2;
        BTreeImpl12 _owner;

        public SplitInserter(BTreeImpl12 owner, RootNode12 rootNode, IntPtr newNode, IntPtr newNode2)
        {
            _owner = owner;
            _rootNode = rootNode;
            _newChildNode = newNode;
            _newChildNode2 = newNode2;
        }

        internal void Run(ref StructList<CursorItem> stack, bool rightInsert)
        {
            var stackIdx = (int)stack.Count - 2;
            again:
            if (stackIdx < 0)
            {
                var keyPrefix = NodeUtils12.GetLeftestKey(_newChildNode2, out var keySuffix);
                var newRootNode = _owner.AllocateBranch(2, (uint)keyPrefix.Length + (ulong)keySuffix.Length,
                    out var keyPusher);
                keyPusher.AddKey(keyPrefix, keySuffix);
                keyPusher.Finish();
                var branchValues = NodeUtils12.GetBranchValuePtrs(newRootNode);
                branchValues[1] = _newChildNode2;
                branchValues[0] = _newChildNode;
                var leftC = NodeUtils12.Ptr2NodeHeader(_newChildNode).RecursiveChildCount;
                var rightC = NodeUtils12.Ptr2NodeHeader(_newChildNode2).RecursiveChildCount;
                NodeUtils12.Ptr2NodeHeader(newRootNode)._recursiveChildCount = leftC + rightC;
                stack.Insert(0).Set(newRootNode, (byte)(rightInsert ? 1 : 0));
                _owner.OverwriteNodePtrInStack(_rootNode, stack.AsSpan(), 0, newRootNode);
                return;
            }

            var top = stack[(uint)stackIdx]._node;
            var idx = stack[(uint)stackIdx]._posInNode;
            ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
            if (header._childCount < MaxChildren)
            {
                var keyPrefix = NodeUtils12.GetLeftestKey(_newChildNode2, out var keySuffix);
                var newSuffixLen = NodeUtils12.GetTotalSuffixLen(top);
                newSuffixLen += keyPrefix.Length + keySuffix.Length;
                var newNode = _owner.AllocateBranch((uint)header._childCount + 1,
                    (ulong)newSuffixLen, out var keyPusher);
                NodeUtils12.Ptr2NodeHeader(newNode)._recursiveChildCount = header._recursiveChildCount + 1;
                if (header.HasLongKeys)
                {
                    var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                    for (var i = 0; i < idx; i++)
                    {
                        keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                    }

                    keyPusher.AddKey(keyPrefix, keySuffix);
                    for (int i = idx; i < longKeys.Length; i++)
                    {
                        keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                    }
                }
                else
                {
                    var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                    for (var i = 0; i < idx; i++)
                    {
                        keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                    }

                    keyPusher.AddKey(keyPrefix, keySuffix);
                    for (int i = idx; i < keyOfs.Length - 1; i++)
                    {
                        keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                    }
                }

                keyPusher.Finish();
                var newValues = NodeUtils12.GetBranchValuePtrs(newNode);
                var oldValues = NodeUtils12.GetBranchValuePtrs(top);
                NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(0, idx), newValues);
                newValues = newValues.Slice(idx);
                newValues[1] = _newChildNode2;
                newValues[0] = _newChildNode;
                newValues = newValues.Slice(2);
                NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(idx + 1), newValues);
                _owner.MakeUnique(_rootNode, stack.AsSpan().Slice(0, stackIdx));
                _owner.AdjustRecursiveChildCount(stack.AsSpan().Slice(0, stackIdx), 1);
                _owner.OverwriteNodePtrInStack(_rootNode, stack.AsSpan(), stackIdx, newNode);
                stack[(uint)stackIdx]._posInNode = (byte)(idx + (rightInsert ? 1 : 0));
            }
            else
            {
                if (idx < MaxChildren / 2)
                {
                    const int splitPos = MaxChildren / 2;
                    var keyPrefix = NodeUtils12.GetLeftestKey(_newChildNode2, out var keySuffix);
                    var newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, 0, splitPos - 1);
                    newSuffixLen += keyPrefix.Length + keySuffix.Length;
                    var newNode = _owner.AllocateBranch((uint)splitPos + 1,
                        (ulong)newSuffixLen, out var keyPusher);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = 0; i < idx; i++)
                        {
                            keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }

                        keyPusher.AddKey(keyPrefix, keySuffix);
                        for (int i = idx; i < splitPos - 1; i++)
                        {
                            keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = 0; i < idx; i++)
                        {
                            keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                        }

                        keyPusher.AddKey(keyPrefix, keySuffix);
                        for (int i = idx; i < splitPos - 1; i++)
                        {
                            keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    var newValues = NodeUtils12.GetBranchValuePtrs(newNode);
                    var oldValues = NodeUtils12.GetBranchValuePtrs(top);
                    NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(0, idx), newValues);
                    newValues = newValues.Slice(idx);
                    newValues[1] = _newChildNode2;
                    newValues[0] = _newChildNode;
                    newValues = newValues.Slice(2);
                    NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(idx + 1, splitPos - idx - 1),
                        newValues);
                    oldValues = oldValues.Slice(splitPos);

                    var rightCount = MaxChildren - splitPos;
                    newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, splitPos, MaxChildren - 1);
                    var newNode2 = _owner.AllocateBranch((uint)rightCount, (ulong)newSuffixLen, out keyPusher);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = splitPos; i < MaxChildren - 1; i++)
                        {
                            keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = splitPos; i < MaxChildren - 1; i++)
                        {
                            keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    newValues = NodeUtils12.GetBranchValuePtrs(newNode2);
                    NodeUtils12.CopyAndReferenceBranchValues(oldValues, newValues);
                    stack[(uint)stackIdx].Set(newNode, (byte)(idx + (rightInsert ? 1 : 0)));
                    NodeUtils12.RecalcRecursiveChildrenCount(newNode);
                    NodeUtils12.RecalcRecursiveChildrenCount(newNode2);
                    _newChildNode = newNode;
                    _newChildNode2 = newNode2;
                    rightInsert = false;
                }
                else
                {
                    const int splitPos = MaxChildren / 2;
                    var keyPrefix = NodeUtils12.GetLeftestKey(_newChildNode2, out var keySuffix);
                    var newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, 0, splitPos - 1);
                    var newNode = _owner.AllocateBranch(splitPos, (ulong)newSuffixLen, out var keyPusher);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = 0; i < splitPos - 1; i++)
                        {
                            keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = 0; i < splitPos - 1; i++)
                        {
                            keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    var newValues = NodeUtils12.GetBranchValuePtrs(newNode);
                    var oldValues = NodeUtils12.GetBranchValuePtrs(top);
                    NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(0, splitPos), newValues);
                    oldValues = oldValues.Slice(splitPos);

                    var rightCount = MaxChildren - splitPos + 1;
                    newSuffixLen = NodeUtils12.GetTotalSuffixLen(top, splitPos, MaxChildren - 1);
                    newSuffixLen += keyPrefix.Length + keySuffix.Length;
                    var newNode2 = _owner.AllocateBranch((uint)rightCount, (ulong)newSuffixLen, out keyPusher);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (var i = splitPos; i < idx; i++)
                        {
                            keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }

                        keyPusher.AddKey(keyPrefix, keySuffix);
                        for (int i = idx; i < MaxChildren - 1; i++)
                        {
                            keyPusher.AddKey(NodeUtils12.LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (var i = splitPos; i < idx; i++)
                        {
                            keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                        }

                        keyPusher.AddKey(keyPrefix, keySuffix);
                        for (int i = idx; i < MaxChildren - 1; i++)
                        {
                            keyPusher.AddKey(_owner.GetShortKey(keyOfs, keyData, i));
                        }
                    }

                    keyPusher.Finish();
                    newValues = NodeUtils12.GetBranchValuePtrs(newNode2);
                    NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(0, idx - splitPos), newValues);
                    newValues = newValues.Slice(idx - splitPos);
                    newValues[1] = _newChildNode2;
                    newValues[0] = _newChildNode;
                    newValues = newValues.Slice(2);
                    NodeUtils12.CopyAndReferenceBranchValues(oldValues.Slice(idx - splitPos + 1), newValues);
                    stack[(uint)stackIdx].Set(newNode, (byte)(idx - splitPos + (rightInsert ? 1 : 0)));
                    NodeUtils12.RecalcRecursiveChildrenCount(newNode);
                    NodeUtils12.RecalcRecursiveChildrenCount(newNode2);
                    _newChildNode = newNode;
                    _newChildNode2 = newNode2;
                    rightInsert = true;
                }

                stackIdx--;
                goto again;
            }
        }
    }

    internal static void ValuesIterate(IntPtr node, ValuesIterateAction visit)
    {
        ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
        if (header.IsNodeLeaf)
        {
            var values = NodeUtils12.GetLeafValues(node);
            while (values.Length >= 12)
            {
                var valueFileId = MemoryMarshal.Read<uint>(values);
                if (valueFileId != 0)
                {
                    var valueOfs = MemoryMarshal.Read<uint>(values.Slice(4));
                    var valueSize = MemoryMarshal.Read<int>(values.Slice(8));
                    visit(valueFileId, valueOfs, valueSize);
                }

                values = values.Slice(12);
            }
        }
        else
        {
            var children = NodeUtils12.GetBranchValuePtrs(node);
            foreach (var child in children)
            {
                ValuesIterate(child, visit);
            }
        }
    }

    public static void KeyValueIterate(IntPtr nodePtr, ref KeyValueIterateCtx ctx, KeyValueIterateCallback callback)
    {
        ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
        if (header.IsNodeLeaf)
        {
            ctx.CancellationToken.ThrowIfCancellationRequested();
            var values = NodeUtils12.GetLeafValues(nodePtr);
            ctx.CurrentPrefix = NodeUtils12.GetPrefixSpan(nodePtr);
            if (header.HasLongKeys)
            {
                var longKeys = NodeUtils12.GetLongKeyPtrs(nodePtr);
                for (var i = 0; i < longKeys.Length; i++)
                {
                    ctx.CurrentSuffix = NodeUtils12.LongKeyPtrToSpan(longKeys[i]);
                    ctx.CurrentValue = values.Slice(i * 12, 12);
                    if (i == 0)
                    {
                        ctx.CalcCommonLength();
                        callback(ref ctx);
                        ctx.PreviousPrefix = ctx.CurrentPrefix;
                    }
                    else
                    {
                        ctx.CalcCommonLengthWithIdenticalPrefixes();
                        callback(ref ctx);
                    }

                    ctx.PreviousSuffix = ctx.CurrentSuffix;
                }
            }
            else
            {
                var keyOffsets = NodeUtils12.GetKeySpans(nodePtr, out var keys);
                for (var i = 0; i < keyOffsets.Length - 1; i++)
                {
                    var ofs = keyOffsets[i];
                    ctx.CurrentSuffix = keys.Slice(ofs, keyOffsets[i + 1] - ofs);
                    ctx.CurrentValue = values.Slice(i * 12, 12);
                    if (i == 0)
                    {
                        ctx.CalcCommonLength();
                        callback(ref ctx);
                        ctx.PreviousPrefix = ctx.CurrentPrefix;
                    }
                    else
                    {
                        ctx.CalcCommonLengthWithIdenticalPrefixes();
                        callback(ref ctx);
                    }

                    ctx.PreviousSuffix = ctx.CurrentSuffix;
                }
            }
        }
        else
        {
            var children = NodeUtils12.GetBranchValuePtrs(nodePtr);
            foreach (var child in children)
            {
                KeyValueIterate(child, ref ctx, callback);
            }
        }
    }

    internal static void TestTreeCorrectness(IntPtr top, ref StructList<CursorItem> stack)
    {
        if (top == IntPtr.Zero)
            return;
        ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
        if (!header.IsNodeLeaf)
        {
            var children = NodeUtils12.GetBranchValuePtrs(top);
            if (header.HasLongKeys)
            {
                var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                for (var i = 0; i < longKeys.Length; i++)
                {
                    var keyPrefix = NodeUtils12.GetLeftestKey(children[i + 1], out var keySuffix);
                    var key = NodeUtils12.LongKeyPtrToSpan(longKeys[i]);
                    if (TreeNodeUtils.FindFirstDifference(key, keyPrefix, keySuffix) != key.Length)
                    {
                        Debugger.Break();
                    }
                }
            }
            else
            {
                var keyOffsets = NodeUtils12.GetKeySpans(top, out var keys);
                for (var i = 0; i < keyOffsets.Length - 1; i++)
                {
                    var keyPrefix = NodeUtils12.GetLeftestKey(children[i + 1], out var keySuffix);
                    var ofs = keyOffsets[i];
                    var key = keys.Slice(ofs, keyOffsets[i + 1] - ofs);
                    if (TreeNodeUtils.FindFirstDifference(key, keyPrefix, keySuffix) != key.Length)
                    {
                        Debugger.Break();
                    }
                }
            }

            for (var i = 0; i < children.Length; i++)
            {
                var child = children[i];
                stack.Add(new(top, (byte)i));
                TestTreeCorrectness(child, ref stack);
                stack.Pop();
            }
        }
    }

    public static void CalcBTreeStats(IntPtr top, RefDictionary<(uint Depth, uint Children), uint> stats, uint depth)
    {
        if (top == IntPtr.Zero)
            return;
        ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
        stats.GetOrAddValueRef((depth, header._childCount))++;
        if (header.IsNodeLeaf) return;
        var children = NodeUtils12.GetBranchValuePtrs(top);

        foreach (var child in children)
        {
            CalcBTreeStats(child, stats, depth + 1);
        }
    }
}
