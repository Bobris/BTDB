using BTDB.Allocators;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;
using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.BTreeLib
{
    public class BTreeImpl12
    {
        readonly IOffHeapAllocator _allocator;
        internal const int PtrSize = 12;

        internal BTreeImpl12(IOffHeapAllocator allocator)
        {
            _allocator = allocator;
        }

        public static IRootNode CreateEmptyRoot(IOffHeapAllocator allocator)
        {
            return new RootNode12(new BTreeImpl12(allocator));
        }

        internal unsafe IntPtr AllocateLeaf(uint childCount, uint keyPrefixLength, ulong totalSuffixLength)
        {
            IntPtr node;
            var nodeType = NodeType12.IsLeaf;
            uint size = 8;
            size += keyPrefixLength;
            size = TreeNodeUtils.AlignUIntUpInt16(size);
            if (totalSuffixLength <= ushort.MaxValue)
            {
                size += 2 * childCount + 2;
                size += (uint)totalSuffixLength;
                size = TreeNodeUtils.AlignUIntUpInt32(size);
            }
            else
            {
                nodeType |= NodeType12.HasLongKeys;
                size += 8 * childCount;
            }
            size += 12 * childCount;
            node = _allocator.Allocate((IntPtr)size);
            ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeader(node);
            nodeHeader._nodeType = nodeType;
            nodeHeader._childCount = (byte)childCount;
            nodeHeader._keyPrefixLength = (ushort)keyPrefixLength;
            nodeHeader._referenceCount = 1;
            new Span<byte>(node.ToPointer(), (int)size).Slice(8).Clear();
            return node;
        }

        internal long CalcIndex(in StructList<CursorItem> stack)
        {
            throw new NotImplementedException();
        }

        internal long EraseRange(RootNode12 rootNode, ref StructList<CursorItem> left, ref StructList<CursorItem> right)
        {
            throw new NotImplementedException();
        }

        internal bool SeekIndex(long index, IntPtr top, ref StructList<CursorItem> stack)
        {
            if (top == IntPtr.Zero)
            {
                return false;
            }

            ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
            if (index >= (long)header.RecursiveChildCount)
                return false;
            while (true)
            {
                if (header.IsNodeLeaf)
                {
                    stack.Add().Set(top, (byte)index);
                    return true;
                }
                var ptrs = NodeUtils12.GetBranchValuePtrs(top);
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
                        stack.Add().Set(NodeUtils12.GetBranchValuePtr(stackItem._node, stackItem._posInNode), 0);
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
                        stack.Add().Set(NodeUtils12.GetBranchValuePtr(stackItem._node, stackItem._posInNode), 0);
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

        internal unsafe IntPtr CloneNode(IntPtr nodePtr)
        {
            var size = NodeUtils12.NodeSize(nodePtr);
            var newNode = _allocator.Allocate((IntPtr)size);
            TreeNodeUtils.CopyMemory(nodePtr, newNode, size);
            ref NodeHeader12 header = ref NodeUtils12.Ptr2NodeHeader(newNode);
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
            _allocator.Deallocate(ptr);
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
            _allocator.Deallocate(node);
        }

        void CheckContent12(ReadOnlySpan<byte> content)
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
            for (int i = 0; i < stack.Length; i++)
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
                Dereference(rootNode._root);
                rootNode._root = newNode;
            }
            else
            {
                WritePtrInNode(stack[i - 1], newNode);
            }
        }

        void WritePtrInNode(in CursorItem stackItem, IntPtr newNode)
        {
            ref var ptr = ref NodeUtils12.GetBranchValuePtrs(stackItem._node)[stackItem._posInNode];
            Dereference(ptr);
            ptr = newNode;
        }

        internal unsafe bool FindExact(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> key)
        {
            stack.Clear();
            var top = rootNode._root;
            if (top == IntPtr.Zero)
            {
                return false;
            }
            while (true)
            {
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                if (header._keyPrefixLength > 0)
                {
                    if (header._keyPrefixLength > key.Length)
                    {
                        stack.Clear();
                        return false;
                    }
                    var prefix = new Span<byte>((top + (int)header.Size).ToPointer(), header._keyPrefixLength);
                    if (!key.Slice(0, header._keyPrefixLength).SequenceEqual(prefix))
                    {
                        stack.Clear();
                        return false;
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
                        stack.Add().Set(top, (byte)(idx >> 1));
                        return true;
                    }
                    stack.Clear();
                    return false;
                }
                idx = (idx + 1) >> 2;
                stack.Add().Set(top, (byte)idx);
                top = NodeUtils12.GetBranchValuePtr(top, idx);
            }
        }

        int BinarySearchKeys(Span<ushort> offsets, Span<byte> keys, ReadOnlySpan<byte> key)
        {
            var l = 0;
            var r = offsets.Length - 1;
            while (l < r)
            {
                var m = (l + r) / 2;
                var middleKey = keys.Slice(keys[m], keys[m + 1] - keys[m]);
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

        int BinarySearchLongKeys(Span<IntPtr> keys, ReadOnlySpan<byte> key)
        {
            var l = 0;
            var r = keys.Length;
            while (l < r)
            {
                var m = (l + r) / 2;
                var middleKey = LongKeyPtrToSpan(keys[m]);
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

        unsafe Span<byte> LongKeyPtrToSpan(IntPtr ptr)
        {
            var size = TreeNodeUtils.ReadInt32Aligned(ptr);
            return new Span<byte>((ptr + 4).ToPointer(), size);
        }

        internal unsafe FindResult Find(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix,
            ReadOnlySpan<byte> key)
        {
            stack.Clear();
            var top = rootNode._root;
            if (top == IntPtr.Zero)
            {
                return FindResult.NotFound;
            }
            while (true)
            {
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                var restKeyPrefix = keyPrefix;
                var restKey = key;
                if (header._keyPrefixLength > 0)
                {
                    var prefix = new Span<byte>((top + (int)header.Size).ToPointer(), header._keyPrefixLength);
                    throw new NotImplementedException();
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
                        stack.Add().Set(top, (byte)(idx >> 1));
                        return FindResult.Exact;
                    }
                    if (idx == 0)
                    {
                        stack.Add().Set(top, 0);
                        return FindResult.Previous;
                    }
                    stack.Add().Set(top, (byte)((idx + 1) >> 1));
                    return FindResult.Next;
                }
                idx = (idx + 1) >> 2;
                stack.Add().Set(top, (byte)idx);
                top = NodeUtils12.GetBranchValuePtr(top, idx);
            }
        }

        byte GetByteFromKeyPair(in ReadOnlySpan<byte> key1, in ReadOnlySpan<byte> key2, int offset)
        {
            if (offset >= key1.Length)
            {
                return key2[offset - key1.Length];
            }

            return key1[offset];
        }

        internal unsafe bool FindFirst(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
        {
            stack.Clear();
            var top = rootNode._root;
            if (top == IntPtr.Zero)
            {
                return false;
            }
            while (true)
            {
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                if (header._keyPrefixLength > 0)
                {
                    var prefix = new Span<byte>((top + (int)header.Size).ToPointer(), header._keyPrefixLength);
                    if (prefix.SequenceCompareTo(keyPrefix) < 0)
                    {
                        stack.Clear();
                        return false;
                    }
                    throw new NotImplementedException();
                }
                var keyRest = keyPrefix.Slice(header._keyPrefixLength);
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
                        stack.Add().Set(top, (byte)(idx >> 1));
                        return true;
                    }
                    stack.Clear();
                    return false;
                }
                idx = (idx + 1) >> 2;
                stack.Add().Set(top, (byte)idx);
                top = NodeUtils12.GetBranchValuePtr(top, idx);
            }
        }

        internal unsafe bool FindLast(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
        {
            stack.Clear();
            var top = rootNode._root;
            if (top == IntPtr.Zero)
            {
                return false;
            }
            while (true)
            {
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                if (header._keyPrefixLength > 0)
                {
                    var prefix = new Span<byte>((top + (int)header.Size).ToPointer(), header._keyPrefixLength);
                    if (prefix.SequenceCompareTo(keyPrefix) < 0)
                    {
                        stack.Clear();
                        return false;
                    }
                    throw new NotImplementedException();
                }
                var keyRest = keyPrefix.Slice(header._keyPrefixLength);
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
                        stack.Add().Set(top, (byte)(idx >> 1));
                        return true;
                    }
                    stack.Clear();
                    return false;
                }
                idx = (idx + 1) >> 2;
                stack.Add().Set(top, (byte)idx);
                top = NodeUtils12.GetBranchValuePtr(top, idx);
            }
        }

        internal unsafe bool Upsert(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> content)
        {
            CheckContent12(content);
            stack.Clear();
            var top = rootNode._root;
            if (top == IntPtr.Zero)
            {
                if (key.Length > 0xffff)
                {
                    top = AllocateLeaf(1, 0xffff, (uint)key.Length - 0xffff);
                    key.Slice(0, 0xffff).CopyTo(NodeUtils12.GetPrefixSpan(top));
                    ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                    if (header.HasLongKeys)
                    {
                        NodeUtils12.GetLongKeyPtrs(top)[0] = AllocateLongKey(key.Slice(0xffff));
                    }
                    else
                    {
                        var offsets = NodeUtils12.GetKeySpans(top, (uint)key.Length - 0xffff, out var keySufixes);
                        offsets[1] = (ushort)((uint)key.Length - 0xffff);
                        key.Slice(0xffff).CopyTo(keySufixes);
                    }
                }
                else
                {
                    top = AllocateLeaf(1, (ushort)key.Length, 0);
                    key.CopyTo(NodeUtils12.GetPrefixSpan(top));
                }
                content.CopyTo(NodeUtils12.GetLeafValues(top));
                stack.Add().Set(top, 0);
                rootNode._root = top;
                return true;
            }
            while (true)
            {
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                if (header._keyPrefixLength > 0)
                {
                    if (header._keyPrefixLength > key.Length)
                    {
                        stack.Clear();
                        return false;
                    }
                    var prefix = new Span<byte>((top + (int)header.Size).ToPointer(), header._keyPrefixLength);
                    if (!key.Slice(0, header._keyPrefixLength).SequenceEqual(prefix))
                    {
                        stack.Clear();
                        return false;
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
                        stack.Add().Set(top, (byte)(idx >> 1));
                        return true;
                    }
                    stack.Clear();
                    return false;
                }
                idx = (idx + 1) >> 2;
                stack.Add().Set(top, (byte)idx);
                top = NodeUtils12.GetBranchValuePtr(top, idx);
            }
        }

        unsafe IntPtr AllocateLongKey(in ReadOnlySpan<byte> data)
        {
            var res = _allocator.Allocate((IntPtr)(4 + data.Length));
            TreeNodeUtils.WriteInt32Aligned(res, data.Length);
            data.CopyTo(new Span<byte>((res + 4).ToPointer(), data.Length));
            return res;
        }

        unsafe int FindFirstDifference(ReadOnlySpan<byte> buf1, IntPtr buf2IntPtr, int len)
        {
            fixed (byte* buf1Ptr = &MemoryMarshal.GetReference(buf1))
            {
                var buf2Ptr = (byte*)buf2IntPtr.ToPointer();
                int i = 0;
                int n;
                if (Vector.IsHardwareAccelerated && len >= Vector<byte>.Count)
                {
                    n = len - Vector<byte>.Count;
                    while (n >= i)
                    {
                        if (Unsafe.ReadUnaligned<Vector<byte>>(buf1Ptr + i) !=
                            Unsafe.ReadUnaligned<Vector<byte>>(buf2Ptr + i))
                            break;
                        i += Vector<byte>.Count;
                    }
                }

                n = len - sizeof(long);
                while (n >= i)
                {
                    if (Unsafe.ReadUnaligned<long>(buf1Ptr + i) != Unsafe.ReadUnaligned<long>(buf2Ptr + i))
                        break;
                    i += sizeof(long);
                }

                while (len > i)
                {
                    if (*(buf1Ptr + i) != *(buf2Ptr + i))
                        break;
                    i++;
                }

                return i;
            }
        }

        void AdjustRecursiveChildCount(in Span<CursorItem> stack, long delta)
        {
            for (int i = 0; i < stack.Length; i++)
            {
                ref var stackItem = ref stack[i];
                ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
                header._recursiveChildCount = (ulong)unchecked((long)header._recursiveChildCount + delta);
            }
        }

    }
}
