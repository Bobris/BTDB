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
            internal int _pos;
            internal ushort _prefixLen;
            internal bool _hasLongKeys;
            internal BTreeImpl12 _impl;
            internal Span<byte> _prefixBytes;
            internal Span<ushort> _keyOffsets;
            internal Span<byte> _keySufixes;
            internal Span<IntPtr> _longKeys;

            internal void AddKey(in ReadOnlySpan<byte> key)
            {
                if (_hasLongKeys)
                {
                    _longKeys[_pos++] = _impl.AllocateLongKey(key.Slice(_prefixLen));
                }
                else
                {
                    var ofs = _keyOffsets[_pos];
                    var len = key.Length - _prefixLen;
                    _keyOffsets[++_pos] = (ushort)(ofs + len);
                    key.Slice(_prefixLen).CopyTo(_keySufixes.Slice(ofs, len));
                }
            }

            internal void AddKey(in ReadOnlySpan<byte> keyPrefix, in ReadOnlySpan<byte> keySufix)
            {
                if (keyPrefix.Length <= _prefixLen)
                {
                    AddKey(keySufix.Slice(_prefixLen - keyPrefix.Length));
                    return;
                }
                if (_hasLongKeys)
                {
                    _longKeys[_pos++] = _impl.AllocateLongKey(_prefixLen, keyPrefix, keySufix);
                }
                else
                {
                    var ofs = _keyOffsets[_pos];
                    var len = keyPrefix.Length + keySufix.Length - _prefixLen;
                    _keyOffsets[++_pos] = (ushort)(ofs + len);
                    var dest = _keySufixes.Slice(ofs, len);
                    keyPrefix.Slice(_prefixLen).CopyTo(dest);
                    dest = dest.Slice(keyPrefix.Length - _prefixLen);
                    keySufix.CopyTo(dest);
                }
            }
        }

        internal unsafe IntPtr AllocateLeaf(uint childCount, uint keyPrefixLength, ulong totalSuffixLength, out KeyPusher keyPusher)
        {
            Debug.Assert(keyPrefixLength <= MaxPrefixSize);
            IntPtr node;
            keyPusher = new KeyPusher();
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
                keyPusher._hasLongKeys = true;
            }
            keyPusher._prefixLen = (ushort)keyPrefixLength;
            size += 12 * childCount;
            node = _allocator.Allocate((IntPtr)size);
            ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeader(node);
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
                keyPusher._keyOffsets = NodeUtils12.GetKeySpans(node, (uint)totalSuffixLength, out keyPusher._keySufixes);
            }
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
                if (key.Length > MaxPrefixSize)
                {
                    top = AllocateLeaf(1, MaxPrefixSize, (uint)key.Length - MaxPrefixSize, out var keyPusher);
                    key.Slice(0, MaxPrefixSize).CopyTo(keyPusher._prefixBytes);
                    keyPusher.AddKey(key);
                }
                else
                {
                    top = AllocateLeaf(1, (ushort)key.Length, 0, out var keyPusher);
                    key.CopyTo(keyPusher._prefixBytes);
                }
                content.CopyTo(NodeUtils12.GetLeafValues(top));
                stack.Add().Set(top, 0);
                rootNode._root = top;
                return true;
            }
            while (true)
            {
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                var idx = BinarySearch(top, key);
                if (!header.IsNodeLeaf)
                {
                    idx = (idx + 1) / 2;
                    if (idx >= header._childCount) idx--;
                    stack.Add().Set(top, (byte)idx);
                    top = NodeUtils12.GetBranchValuePtr(top, idx);
                    continue;
                }
                if ((idx & 1) != 0)
                {
                    // Key found => Update
                    stack.Add().Set(top, (byte)(idx / 2));
                    WriteValue(rootNode, ref stack, content);
                    return false;
                }
                if (header._childCount < MaxChildren)
                {
                    var newPrefixLen = CalcCommonPrefix(top, key);
                    var newSuffixLen = NodeUtils12.GetTotalSufixLen(top) + header._keyPrefixLength * header._childCount;
                    newSuffixLen -= newPrefixLen * (header._childCount + 1) - key.Length;
                    var newNode = AllocateLeaf((uint)header._childCount + 1, (uint)newPrefixLen, (ulong)newSuffixLen, out var keyPusher);
                    key.Slice(0, newPrefixLen).CopyTo(keyPusher._prefixBytes);
                    idx = idx / 2;
                    var prefixBytes = NodeUtils12.GetPrefixSpan(top);
                    if (header.HasLongKeys)
                    {
                        var longKeys = NodeUtils12.GetLongKeyPtrs(top);
                        for (int i = 0; i < idx; i++)
                        {
                            keyPusher.AddKey(prefixBytes, LongKeyPtrToSpan(longKeys[i]));
                        }
                        keyPusher.AddKey(key);
                        for (int i = idx; i < longKeys.Length; i++)
                        {
                            keyPusher.AddKey(prefixBytes, LongKeyPtrToSpan(longKeys[i]));
                        }
                    }
                    else
                    {
                        var keyOfs = NodeUtils12.GetKeySpans(top, out var keyData);
                        for (int i = 0; i < idx; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }
                        keyPusher.AddKey(key);
                        for (int i = idx; i < keyOfs.Length - 1; i++)
                        {
                            keyPusher.AddKey(prefixBytes, GetShortKey(keyOfs, keyData, i));
                        }
                    }
                    var newValues = NodeUtils12.GetLeafValues(newNode);
                    var oldValues = NodeUtils12.GetLeafValues(top);
                    oldValues.Slice(0, 12 * idx).CopyTo(newValues);
                    newValues = newValues.Slice(12 * idx);
                    content.CopyTo(newValues);
                    newValues = newValues.Slice(12);
                    oldValues.Slice(12 * idx).CopyTo(newValues);
                    MakeUnique(rootNode, stack.AsSpan());
                    AdjustRecursiveChildCount(stack.AsSpan(), 1);
                    stack.Add().Set(top, (byte)idx);
                    OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, newNode);
                    return true;
                }
                throw new NotImplementedException();
            }
        }

        ReadOnlySpan<byte> GetShortKey(in Span<ushort> keyOfs, in Span<byte> keyData, int idx)
        {
            var start = keyOfs[idx];
            return keyData.Slice(start, keyOfs[idx + 1] - start);
        }

        int CalcCommonPrefix(IntPtr nodePtr, in ReadOnlySpan<byte> key)
        {
            var prefix = NodeUtils12.GetPrefixSpan(nodePtr);
            return TreeNodeUtils.FindFirstDifference(prefix, key);
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

        unsafe IntPtr AllocateLongKey(ushort prefixLen, ReadOnlySpan<byte> keyPrefix, ReadOnlySpan<byte> keySufix)
        {
            if (prefixLen >= keyPrefix.Length)
            {
                return AllocateLongKey(keySufix.Slice(prefixLen - keyPrefix.Length));
            }
            var totalLen = keyPrefix.Length + keySufix.Length - prefixLen;
            var res = _allocator.Allocate((IntPtr)(4 + totalLen));
            TreeNodeUtils.WriteInt32Aligned(res, totalLen);
            var dest = new Span<byte>((res + 4).ToPointer(), totalLen);
            keyPrefix.Slice(prefixLen).CopyTo(dest);
            dest = dest.Slice(keyPrefix.Length - prefixLen);
            keySufix.CopyTo(dest);
            return res;
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
