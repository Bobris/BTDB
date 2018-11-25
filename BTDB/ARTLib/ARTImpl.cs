using BTDB.KVDBLayer;
using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
{
    public class ARTImpl
    {
        internal readonly bool IsValue12;
        readonly IOffHeapAllocator _allocator;
        internal readonly int PtrSize;

        internal ARTImpl(IOffHeapAllocator allocator, bool isValue12)
        {
            _allocator = allocator;
            IsValue12 = isValue12;
            PtrSize = isValue12 ? 12 : 8;
        }

        public static IRootNode CreateEmptyRoot(IOffHeapAllocator allocator, bool isValue12)
        {
            return new RootNode(new ARTImpl(allocator, isValue12));
        }

        unsafe internal IntPtr AllocateNode(NodeType nodeType, uint keyPrefixLength, uint valueLength)
        {
            IntPtr node;
            int baseSize;
            if (IsValue12)
            {
                nodeType = nodeType | NodeType.Has12BPtrs;
                baseSize = NodeUtils.BaseSize(nodeType);
                var size = baseSize + NodeUtils.AlignUIntUpInt32(keyPrefixLength) + (nodeType.HasFlag(NodeType.IsLeaf) ? 12 : 0);
                if (keyPrefixLength >= 0xffff) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            else
            {
                baseSize = NodeUtils.BaseSize(nodeType);
                var size = baseSize + keyPrefixLength + (nodeType.HasFlag(NodeType.IsLeaf) ? valueLength : 0);
                if (keyPrefixLength >= 0xffff) size += 4;
                if (nodeType.HasFlag(NodeType.IsLeaf)) size += 4;
                node = _allocator.Allocate((IntPtr)size);
            }
            ref var nodeHeader = ref NodeUtils.Ptr2NodeHeader(node);
            nodeHeader._nodeType = nodeType;
            nodeHeader._childCount = 0;
            nodeHeader._referenceCount = 1;
            nodeHeader._recursiveChildCount = 1;
            new Span<byte>(node.ToPointer(), baseSize).Slice(16).Clear();
            if (keyPrefixLength >= 0xffffu)
            {
                nodeHeader._keyPrefixLength = (ushort)0xffffu;
                *(uint*)(node + baseSize).ToPointer() = keyPrefixLength;
                baseSize += 4;
            }
            else
            {
                nodeHeader._keyPrefixLength = (ushort)keyPrefixLength;
            }
            if (!IsValue12 && nodeType.HasFlag(NodeType.IsLeaf))
            {
                *(uint*)(node + baseSize).ToPointer() = valueLength;
            }
            if ((nodeType & NodeType.NodeSizeMask) == NodeType.Node48)
            {
                Unsafe.InitBlock((node + 16).ToPointer(), 255, 256);
            }
            if ((nodeType & NodeType.NodeSizePtrMask) == (NodeType.Node256 | NodeType.Has12BPtrs))
            {
                var p = (uint*)(node + 16).ToPointer();
                for (var i = 0; i < 256; i++)
                {
                    *p = uint.MaxValue;
                    p += 3;
                }
            }
            return node;
        }

        internal long EraseRange(RootNode rootNode, ref StructList<CursorItem> left, ref StructList<CursorItem> right)
        {
            var isUnique = true;
            var leftIndex = 0u;
            var rightIndex = 0u;
            var newNode = IntPtr.Zero;
            var children = 0L;
            while (true)
            {
                ref var leftItem = ref left[leftIndex];
                ref var rightItem = ref right[rightIndex];
                if (leftItem._posInNode == rightItem._posInNode)
                {
                    if (leftItem._posInNode == -1)
                    {
                        (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, -1, 0, IntPtr.Zero, -1, 0, IntPtr.Zero);
                        goto up;
                    }
                    if (leftIndex + 1 == left.Count)
                    {
                        (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, leftItem._posInNode, leftItem._byte, IntPtr.Zero, rightItem._posInNode, rightItem._byte, IntPtr.Zero);
                        goto up;
                    }
                    if (NodeUtils.Ptr2NodeHeader(leftItem._node)._referenceCount > 1)
                        isUnique = false;
                    leftIndex++;
                    rightIndex++;
                    continue;
                }
                var downUnique = isUnique;
                if (NodeUtils.Ptr2NodeHeader(leftItem._node)._referenceCount > 1)
                    downUnique = false;
                var leftNode = IntPtr.Zero;
                var rightNode = IntPtr.Zero;
                var childrenLeft = 0L;
                var childrenRight = 0L;
                if (leftIndex + 1 < left.Count)
                {
                    (leftNode, childrenLeft) = EraseTillEnd(downUnique, left.AsSpan((int)leftIndex + 1));
                }
                if (rightIndex + 1 < right.Count)
                {
                    (rightNode, childrenRight) = EraseFromStart(downUnique, right.AsSpan((int)rightIndex + 1));
                }
                (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, leftItem._posInNode, leftItem._byte, leftNode, rightItem._posInNode, rightItem._byte, rightNode);
                goto up;
            }
        up:
            if (newNode == IntPtr.Zero)
            {
                if (leftIndex > 0)
                {
                    leftIndex--;
                    ref var leftItem = ref left[leftIndex];
                    (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, leftItem._posInNode, leftItem._byte, IntPtr.Zero, leftItem._posInNode, leftItem._byte, IntPtr.Zero);
                }
            }
            MakeUniqueAndOverwrite(rootNode, left.AsSpan(0, (int)leftIndex), newNode);
            AdjustRecursiveChildCount(left.AsSpan(0, (int)leftIndex), -children);
            left.Clear();
            right.Clear();
            return children;
        }

        (IntPtr newNode, long children) EraseFromStart(bool downUnique, Span<CursorItem> span)
        {
            var downDownUnique = downUnique;
            var node = span[0]._node;
            if (NodeUtils.Ptr2NodeHeader(node)._referenceCount > 1)
                downDownUnique = false;
            var newNode = IntPtr.Zero;
            var children = 0L;
            if (span.Length > 1)
            {
                (newNode, children) = EraseFromStart(downDownUnique, span.Slice(1));
            }
            short startPos = -1;
            byte startByte = 0;
            (startPos, startByte) = GetStartPosAndByte(node);
            var (resNode, resChildren) = EraseRangeFromNode(downUnique, span[0]._node, startPos, startByte, IntPtr.Zero, span[0]._posInNode, span[0]._byte, newNode);
            return (resNode, resChildren + children);
        }

        (short pos, byte @byte) GetStartPosAndByte(IntPtr node)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(node);
            if (header._nodeType.HasFlag(NodeType.IsLeaf))
            {
                return (-1, 0);
            }
            switch (header._nodeType & NodeType.NodeSizeMask)
            {
                case NodeType.Node4:
                case NodeType.Node16:
                    return (0, NodeUtils.ReadByte(node + 16));
                case NodeType.Node48:
                    for (var i = 0; i < 256; i++)
                    {
                        var pos = NodeUtils.ReadByte(node + 16 + i);
                        if (pos == 255)
                            continue;
                        return (pos, (byte)i);
                    }
                    break;
                case NodeType.Node256:
                    for (var i = 0; i < 256; i++)
                    {
                        if (IsPtr(NodeUtils.PtrInNode(node, i), out var ptr))
                        {
                            if (ptr == IntPtr.Zero)
                                continue;
                        }
                        return ((short)i, (byte)i);
                    }
                    break;
            }
            throw new InvalidOperationException();
        }

        (short pos, byte @byte) GetStartPosAndByteSkipLeaf(IntPtr node)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(node);
            switch (header._nodeType & NodeType.NodeSizeMask)
            {
                case NodeType.Node4:
                case NodeType.Node16:
                    return (0, NodeUtils.ReadByte(node + 16));
                case NodeType.Node48:
                    for (var i = 0; i < 256; i++)
                    {
                        var pos = NodeUtils.ReadByte(node + 16 + i);
                        if (pos == 255)
                            continue;
                        return (pos, (byte)i);
                    }
                    break;
                case NodeType.Node256:
                    for (var i = 0; i < 256; i++)
                    {
                        if (IsPtr(NodeUtils.PtrInNode(node, i), out var ptr))
                        {
                            if (ptr == IntPtr.Zero)
                                continue;
                        }
                        return ((short)i, (byte)i);
                    }
                    break;
            }
            throw new InvalidOperationException();
        }

        (IntPtr newNode, long children) EraseTillEnd(bool downUnique, Span<CursorItem> span)
        {
            var downDownUnique = downUnique;
            var node = span[0]._node;
            if (NodeUtils.Ptr2NodeHeader(node)._referenceCount > 1)
                downDownUnique = false;
            var newNode = IntPtr.Zero;
            var children = 0L;
            if (span.Length > 1)
            {
                (newNode, children) = EraseTillEnd(downDownUnique, span.Slice(1));
            }
            short endPos = -1;
            byte endByte = 0;
            (endPos, endByte) = GetEndPosAndByte(node);
            var (resNode, resChildren) = EraseRangeFromNode(downUnique, span[0]._node, span[0]._posInNode, span[0]._byte, newNode, endPos, endByte, IntPtr.Zero);
            return (resNode, resChildren + children);
        }

        (short pos, byte @byte) GetEndPosAndByte(IntPtr node)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(node);
            switch (header._nodeType & NodeType.NodeSizeMask)
            {
                case NodeType.NodeLeaf:
                    return (-1, 0);
                case NodeType.Node4:
                case NodeType.Node16:
                    {
                        var pos = header._childCount - 1;
                        return ((short)pos, NodeUtils.ReadByte(node + 16 + pos));
                    }
                case NodeType.Node48:
                    for (var i = 255; i >= 0; i--)
                    {
                        var pos = NodeUtils.ReadByte(node + 16 + i);
                        if (pos == 255)
                            continue;
                        return (pos, (byte)i);
                    }
                    break;
                case NodeType.Node256:
                    for (var i = 255; i >= 0; i--)
                    {
                        if (IsPtr(NodeUtils.PtrInNode(node, i), out var ptr))
                        {
                            if (ptr == IntPtr.Zero)
                                continue;
                        }
                        return ((short)i, (byte)i);
                    }
                    break;
            }
            throw new InvalidOperationException();
        }

        (IntPtr newNode, long children) EraseRangeFromNode(bool canBeInplace, IntPtr node, short leftPos, byte leftByte, IntPtr leftNode, short rightPos, byte rightByte, IntPtr rightNode)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(node);
            if (header._referenceCount > 1)
                canBeInplace = false;
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
            {
                return (IntPtr.Zero, 1);
            }
            var willBeIsLeaf = header._nodeType.HasFlag(NodeType.IsLeaf) && (leftPos > -1);
            var willBeChildCount = header.ChildCount;
            var children = 0L;
            if (leftPos == -1)
            {
                children++;
                (leftPos, leftByte) = GetStartPosAndByteSkipLeaf(node);
            }
            switch (header._nodeType & NodeType.NodeSizeMask)
            {
                case NodeType.Node4:
                case NodeType.Node16:
                    for (var i = leftPos; i <= rightPos; i++)
                    {
                        willBeChildCount--;
                        if (IsPtr(NodeUtils.PtrInNode(node, i), out var ptr))
                        {
                            children += (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                        }
                        else
                        {
                            children++;
                        }
                    }
                    break;
                case NodeType.Node48:
                    unsafe
                    {
                        var span = new Span<byte>((node + 16).ToPointer(), 256);
                        for (int i = leftByte; i <= rightByte; i++)
                        {
                            if (span[i] == 255)
                                continue;
                            willBeChildCount--;
                            if (IsPtr(NodeUtils.PtrInNode(node, span[i]), out var ptr))
                            {
                                children += (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                            }
                            else
                            {
                                children++;
                            }
                        }
                    }
                    break;
                case NodeType.Node256:
                    for (int j = leftByte; j <= rightByte; j++)
                    {
                        if (IsPtr(NodeUtils.PtrInNode(node, j), out var ptr))
                        {
                            if (ptr == IntPtr.Zero)
                                continue;
                            children += (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                        }
                        else
                        {
                            children++;
                        }
                        willBeChildCount--;
                    }
                    break;
            }
            if (leftNode != IntPtr.Zero)
            {
                children -= (long)NodeUtils.Ptr2NodeHeader(leftNode)._recursiveChildCount;
                willBeChildCount++;
            }
            if (rightNode != IntPtr.Zero)
            {
                children -= (long)NodeUtils.Ptr2NodeHeader(rightNode)._recursiveChildCount;
                willBeChildCount++;
            }
            if (willBeChildCount == 0 && !willBeIsLeaf)
            {
                return (IntPtr.Zero, children);
            }
            var newNodeType = NodeType.NodeLeaf;
            if (willBeChildCount > 48)
            {
                newNodeType = NodeType.Node256;
            }
            else if (willBeChildCount > 16)
            {
                newNodeType = NodeType.Node48;
            }
            else if (willBeChildCount > 4)
            {
                newNodeType = NodeType.Node16;
            }
            else if ((willBeChildCount > 1) || (willBeChildCount == 1 && willBeIsLeaf))
            {
                newNodeType = NodeType.Node4;
            }
            if (willBeIsLeaf)
            {
                newNodeType |= NodeType.IsLeaf;
            }
            newNodeType |= header._nodeType & NodeType.Has12BPtrs;
            if (canBeInplace && header._nodeType == newNodeType)
            {
                switch (newNodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            if (leftNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils.PtrInNode(node, leftPos), leftNode);
                                leftPos++;
                            }
                            if (rightNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils.PtrInNode(node, rightPos), rightNode);
                                rightPos--;
                            }
                            if (leftPos <= rightPos)
                            {
                                for (var i = leftPos; i <= rightPos; i++)
                                {
                                    WritePtrInNode(NodeUtils.PtrInNode(node, i), IntPtr.Zero);
                                }
                                CopyMemory(node + 16 + rightPos + 1, node + 16 + leftPos, header._childCount - rightPos);
                                CopyMemory(NodeUtils.PtrInNode(node, rightPos + 1), NodeUtils.PtrInNode(node, leftPos), (header._childCount - rightPos) * PtrSize);
                            }
                            break;
                        }
                    case NodeType.Node48:
                        {
                            if (leftNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils.PtrInNode(node, leftPos), leftNode);
                                leftByte++;
                            }
                            if (rightNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils.PtrInNode(node, rightPos), rightNode);
                                rightByte--;
                            }
                            if (willBeChildCount < header._childCount)
                            {
                                unsafe
                                {
                                    Span<byte> tempItems = stackalloc byte[willBeChildCount * PtrSize]; // maximum 47*12=564
                                    var bytePtrs = new Span<byte>((node + 16).ToPointer(), 256);
                                    var outPos = 0;
                                    for (var i = 0; i < 256; i++)
                                    {
                                        var idx = bytePtrs[i];
                                        if (idx == 255) continue;
                                        if ((i >= leftByte) && (i <= rightByte))
                                        {
                                            WritePtrInNode(NodeUtils.PtrInNode(node, idx), IntPtr.Zero);
                                        }
                                        else
                                        {
                                            bytePtrs[i] = (byte)outPos;
                                            new Span<byte>((node + 16 + 256 + idx * PtrSize).ToPointer(), PtrSize).CopyTo(tempItems.Slice(outPos * PtrSize));
                                            outPos++;
                                        }
                                    }
                                    tempItems.CopyTo(new Span<byte>((node + 16 + 256).ToPointer(), tempItems.Length));
                                }
                            }
                            break;
                        }
                    case NodeType.Node256:
                        {
                            if (leftNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils.PtrInNode(node, leftPos), leftNode);
                                leftByte++;
                            }
                            if (rightNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils.PtrInNode(node, rightPos), rightNode);
                                rightByte--;
                            }
                            if (willBeChildCount < header._childCount)
                            {
                                for (int i = leftByte; i <= rightByte; i++)
                                {
                                    WritePtrInNode(NodeUtils.PtrInNode(node, i), IntPtr.Zero);
                                }
                            }
                            break;
                        }
                }
                header._childCount = (byte)willBeChildCount;
                header._recursiveChildCount -= (ulong)children;
                return (node, children);
            }
            if (willBeChildCount == 1 && !willBeIsLeaf)
            {
                byte onlyByte = 0;
                IntPtr onlyPtr = IntPtr.Zero;
                if (leftNode != IntPtr.Zero)
                {
                    onlyByte = leftByte;
                    onlyPtr = leftNode;
                }
                else if (rightNode != IntPtr.Zero)
                {
                    onlyByte = rightByte;
                    onlyPtr = rightNode;
                }
                else
                {
                    switch (header._nodeType & NodeType.NodeSizeMask)
                    {
                        case NodeType.Node4:
                        case NodeType.Node16:
                            {
                                if (leftPos > 0)
                                {
                                    onlyByte = NodeUtils.ReadByte(node + 16);
                                    onlyPtr = NodeUtils.PtrInNode(node, 0);
                                }
                                else
                                {
                                    onlyByte = NodeUtils.ReadByte(node + 16 + rightPos + 1);
                                    onlyPtr = NodeUtils.PtrInNode(node, rightPos + 1);
                                }
                                break;
                            }
                        case NodeType.Node48:
                            {
                                for (var i = 0; i < 256; i++)
                                {
                                    if (i == leftByte)
                                    {
                                        i = rightByte;
                                        continue;
                                    }
                                    var idx = NodeUtils.ReadByte(node + 16 + i);
                                    if (idx == 255) continue;
                                    onlyByte = (byte)i;
                                    onlyPtr = NodeUtils.PtrInNode(node, idx);
                                    break;
                                }
                                break;
                            }
                        case NodeType.Node256:
                            {
                                for (int i = 0; i < 256; i++)
                                {
                                    if (i == leftByte)
                                    {
                                        i = rightByte;
                                        continue;
                                    }
                                    if (IsPtr(NodeUtils.PtrInNode(node, i), out var j))
                                    {
                                        if (j == IntPtr.Zero)
                                            continue;
                                    }
                                    onlyByte = (byte)i;
                                    onlyPtr = NodeUtils.PtrInNode(node, i);
                                    break;
                                }
                                break;
                            }
                    }
                    if (IsPtr(onlyPtr, out var ptr))
                    {
                        onlyPtr = ptr;
                        NodeUtils.Reference(onlyPtr);
                    }
                    else
                    {
                        var (prefixSize, prefixPtr) = NodeUtils.GetPrefixSizeAndPtr(node);
                        var (valueSize, valuePtr) = GetValueSizeAndPtrFromPtrInNode(onlyPtr);
                        var newNode = AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf | (IsValue12 ? NodeType.Has12BPtrs : 0), prefixSize + 1, valueSize);
                        var (newPrefixSize, newPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
                        CopyMemory(prefixPtr, newPrefixPtr, (int)prefixSize);
                        NodeUtils.WriteByte(newPrefixPtr + (int)prefixSize, onlyByte);
                        if (valueSize > 0)
                        {
                            var (newValueSize, newValuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                            CopyMemory(valuePtr, newValuePtr, (int)valueSize);
                        }
                        return (newNode, children);
                    }
                }
                // scope for consistent local variable names
                {
                    var (prefixSize, prefixPtr) = NodeUtils.GetPrefixSizeAndPtr(node);
                    var newNode = CloneNodeWithKeyPrefixCut(onlyPtr, -(int)(prefixSize + 1));
                    var (newPrefixSize, newPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
                    CopyMemory(prefixPtr, newPrefixPtr, (int)prefixSize);
                    NodeUtils.WriteByte(newPrefixPtr, (int)prefixSize, onlyByte);
                    Dereference(onlyPtr);
                    return (newNode, children);
                }
            }
            // scope for consistent local variable names
            {
                var (prefixSize, prefixPtr) = NodeUtils.GetPrefixSizeAndPtr(node);
                var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(node);
                var newNode = AllocateNode(newNodeType, prefixSize, valueSize);
                if (prefixSize > 0)
                {
                    var (newPrefixSize, newPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
                    CopyMemory(prefixPtr, newPrefixPtr, (int)prefixSize);
                }
                if (willBeIsLeaf)
                {
                    var (newValueSize, newValuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                    CopyMemory(valuePtr, newValuePtr, (int)valueSize);
                }
                if (willBeChildCount == 0)
                {
                    NodeUtils.Ptr2NodeHeader(newNode)._recursiveChildCount = 1;
                    return (newNode, children);
                }
                Pusher pusher = new Pusher(newNode, newNodeType);
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            for (var i = 0; i < header._childCount; i++)
                            {
                                if (i == leftPos)
                                {
                                    if (leftNode != IntPtr.Zero)
                                        pusher.PushPtr(leftByte, leftNode);
                                    i = rightPos;
                                    if (rightNode != IntPtr.Zero)
                                    {
                                        pusher.PushPtr(rightByte, rightNode);
                                    }
                                    continue;
                                }
                                pusher.Push(NodeUtils.ReadByte(node + 16 + i), NodeUtils.PtrInNode(node, i));
                            }
                            break;
                        }
                    case NodeType.Node48:
                        {
                            unsafe
                            {
                                var bytePtrs = new Span<byte>((node + 16).ToPointer(), 256);
                                for (var i = 0; i < 256; i++)
                                {
                                    var idx = bytePtrs[i];
                                    if (idx == 255) continue;
                                    if (i == leftByte)
                                    {
                                        if (leftNode != IntPtr.Zero)
                                            pusher.PushPtr(leftByte, leftNode);
                                        i = rightByte;
                                        if (rightNode != IntPtr.Zero)
                                        {
                                            pusher.PushPtr(rightByte, rightNode);
                                        }
                                        continue;
                                    }
                                    pusher.Push((byte)i, NodeUtils.PtrInNode(node, idx));
                                }
                            }
                            break;
                        }
                    case NodeType.Node256:
                        {
                            for (var i = 0; i < 256; i++)
                            {
                                if (i == leftByte)
                                {
                                    if (leftNode != IntPtr.Zero)
                                        pusher.PushPtr(leftByte, leftNode);
                                    i = rightByte;
                                    if (rightNode != IntPtr.Zero)
                                    {
                                        pusher.PushPtr(rightByte, rightNode);
                                    }
                                    continue;
                                }
                                if (IsPtr(NodeUtils.PtrInNode(node, i), out var j))
                                {
                                    if (j == IntPtr.Zero)
                                        continue;
                                }
                                pusher.Push((byte)i, NodeUtils.PtrInNode(node, i));
                            }
                            break;
                        }
                }
                ref var newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
                newHeader._childCount = (byte)willBeChildCount;
                newHeader._recursiveChildCount = header._recursiveChildCount - (ulong)children;
                return (newNode, children);
            }
        }

        struct Pusher
        {
            IntPtr _byteDst;
            IntPtr _dst;
            readonly int _type;
            int _idx;

            public Pusher(IntPtr node, NodeType nodeType)
            {
                _idx = 0;
                switch (nodeType & NodeType.NodeSizePtrMask)
                {
                    case NodeType.Node4:
                        {
                            _type = 0;
                            _byteDst = node + 16;
                            _dst = node + 16 + 4;
                            break;
                        }
                    case NodeType.Node4 | NodeType.Has12BPtrs:
                        {
                            _type = 1;
                            _byteDst = node + 16;
                            _dst = node + 16 + 4;
                            break;
                        }
                    case NodeType.Node16:
                        {
                            _type = 0;
                            _byteDst = node + 16;
                            _dst = node + 16 + 16;
                            break;
                        }
                    case NodeType.Node16 | NodeType.Has12BPtrs:
                        {
                            _type = 1;
                            _byteDst = node + 16;
                            _dst = node + 16 + 16;
                            break;
                        }
                    case NodeType.Node48:
                        {
                            _type = 2;
                            _byteDst = node + 16;
                            _dst = node + 16 + 256;
                            break;
                        }
                    case NodeType.Node48 | NodeType.Has12BPtrs:
                        {
                            _type = 3;
                            _byteDst = node + 16;
                            _dst = node + 16 + 256;
                            break;
                        }
                    case NodeType.Node256:
                        {
                            _type = 4;
                            _byteDst = node;
                            _dst = node + 16;
                            break;
                        }
                    case NodeType.Node256 | NodeType.Has12BPtrs:
                        {
                            _type = 5;
                            _byteDst = node;
                            _dst = node + 16;
                            break;
                        }
                    default:
                        throw new InvalidOperationException();
                }
            }

            public void PushPtr(byte @byte, IntPtr ptr)
            {
                switch (_type)
                {
                    case 0:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte);
                            _byteDst += 1;
                            NodeUtils.WriteIntPtrUnalligned(_dst, ptr);
                            _dst += 8;
                            break;
                        }
                    case 1:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte);
                            _byteDst += 1;
                            NodeUtils.WriteInt32Alligned(_dst, unchecked((int)uint.MaxValue));
                            NodeUtils.WriteIntPtrUnalligned(_dst + 4, ptr);
                            _dst += 12;
                            break;
                        }
                    case 2:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte, (byte)_idx);
                            _idx++;
                            NodeUtils.WriteIntPtrUnalligned(_dst, ptr);
                            _dst += 8;
                            break;
                        }
                    case 3:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte, (byte)_idx);
                            _idx++;
                            NodeUtils.WriteInt32Alligned(_dst, unchecked((int)uint.MaxValue));
                            NodeUtils.WriteIntPtrUnalligned(_dst + 4, ptr);
                            _dst += 12;
                            break;
                        }
                    case 4:
                        {
                            NodeUtils.WriteIntPtrUnalligned(_dst + 8 * @byte, ptr);
                            break;
                        }
                    case 5:
                        {
                            NodeUtils.WriteInt32Alligned(_dst + 12 * @byte, unchecked((int)uint.MaxValue));
                            NodeUtils.WriteIntPtrUnalligned(_dst + 4 + 12 * @byte, ptr);
                            break;
                        }
                }
            }

            public void Push(byte @byte, IntPtr source)
            {
                switch (_type)
                {
                    case 0:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte);
                            _byteDst += 1;
                            var p = NodeUtils.ReadIntPtrUnalligned(source);
                            NodeUtils.WriteIntPtrUnalligned(_dst, p);
                            if (NodeUtils.IsPtrPtr(p))
                                NodeUtils.Reference(p);
                            _dst += 8;
                            break;
                        }
                    case 1:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte);
                            _byteDst += 1;
                            NodeUtils.WriteInt32Alligned(_dst, NodeUtils.ReadInt32Alligned(source));
                            NodeUtils.WriteInt32Alligned(_dst + 4, NodeUtils.ReadInt32Alligned(source + 4));
                            NodeUtils.WriteInt32Alligned(_dst + 8, NodeUtils.ReadInt32Alligned(source + 8));
                            if (NodeUtils.IsPtr12Ptr(source))
                                NodeUtils.Reference(NodeUtils.Read12Ptr(source));
                            _dst += 12;
                            break;
                        }
                    case 2:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte, (byte)_idx);
                            _idx++;
                            var p = NodeUtils.ReadIntPtrUnalligned(source);
                            NodeUtils.WriteIntPtrUnalligned(_dst, p);
                            if (NodeUtils.IsPtrPtr(p))
                                NodeUtils.Reference(p);
                            _dst += 8;
                            break;
                        }
                    case 3:
                        {
                            NodeUtils.WriteByte(_byteDst, @byte, (byte)_idx);
                            _idx++;
                            NodeUtils.WriteInt32Alligned(_dst, NodeUtils.ReadInt32Alligned(source));
                            NodeUtils.WriteInt32Alligned(_dst + 4, NodeUtils.ReadInt32Alligned(source + 4));
                            NodeUtils.WriteInt32Alligned(_dst + 8, NodeUtils.ReadInt32Alligned(source + 8));
                            if (NodeUtils.IsPtr12Ptr(source))
                                NodeUtils.Reference(NodeUtils.Read12Ptr(source));
                            _dst += 12;
                            break;
                        }
                    case 4:
                        {
                            var p = NodeUtils.ReadIntPtrUnalligned(source);
                            NodeUtils.WriteIntPtrUnalligned(_dst + 8 * @byte, p);
                            if (NodeUtils.IsPtrPtr(p))
                                NodeUtils.Reference(p);
                            break;
                        }
                    case 5:
                        {
                            var ofs = 12 * @byte;
                            NodeUtils.WriteInt32Alligned(_dst + ofs, NodeUtils.ReadInt32Alligned(source));
                            NodeUtils.WriteInt32Alligned(_dst + ofs + 4, NodeUtils.ReadInt32Alligned(source + 4));
                            NodeUtils.WriteInt32Alligned(_dst + ofs + 8, NodeUtils.ReadInt32Alligned(source + 8));
                            if (NodeUtils.IsPtr12Ptr(source))
                                NodeUtils.Reference(NodeUtils.Read12Ptr(source));
                            break;
                        }
                }
            }
        }

        internal long CalcIndex(in StructList<CursorItem> stack)
        {
            var stackCount = stack.Count;
            if (stackCount == 0)
                return -1;
            var res = 0L;
            for (var i = 0; i < stackCount; i++)
            {
                ref var stackItem = ref stack[(uint)i];
                if (stackItem._posInNode == -1)
                    return res;
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                    res++;
                switch (header._nodeType & NodeType.NodeSizePtrMask)
                {
                    case NodeType.Node4:
                        {
                            var ptrInNode = stackItem._node + 16 + 4;
                            var limit = ptrInNode + stackItem._posInNode * 8;
                            for (; ptrInNode != limit; ptrInNode += 8)
                            {
                                var child = NodeUtils.ReadPtr(ptrInNode);
                                if (NodeUtils.IsPtrPtr(child))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(child)._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node4 | NodeType.Has12BPtrs:
                        {
                            var ptrInNode = stackItem._node + 16 + 4;
                            var limit = ptrInNode + stackItem._posInNode * 12;
                            for (; ptrInNode != limit; ptrInNode += 12)
                            {
                                if (NodeUtils.IsPtr12Ptr(ptrInNode))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(NodeUtils.Read12Ptr(ptrInNode))._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node16:
                        {
                            var ptrInNode = stackItem._node + 16 + 16;
                            var limit = ptrInNode + stackItem._posInNode * 8;
                            for (; ptrInNode != limit; ptrInNode += 8)
                            {
                                var child = NodeUtils.ReadPtr(ptrInNode);
                                if (NodeUtils.IsPtrPtr(child))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(child)._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node16 | NodeType.Has12BPtrs:
                        {
                            var ptrInNode = stackItem._node + 16 + 16;
                            var limit = ptrInNode + stackItem._posInNode * 12;
                            for (; ptrInNode != limit; ptrInNode += 12)
                            {
                                if (NodeUtils.IsPtr12Ptr(ptrInNode))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(NodeUtils.Read12Ptr(ptrInNode))._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), stackItem._byte);
                            for (int j = 0; j < span.Length; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                var ptrInNode = stackItem._node + 16 + 256 + span[j] * 8;
                                var child = NodeUtils.ReadPtr(ptrInNode);
                                if (NodeUtils.IsPtrPtr(child))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(child)._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node48 | NodeType.Has12BPtrs:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), stackItem._byte);
                            for (int j = 0; j < span.Length; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                var ptrInNode = stackItem._node + 16 + 256 + span[j] * 12;
                                if (NodeUtils.IsPtr12Ptr(ptrInNode))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(NodeUtils.Read12Ptr(ptrInNode))._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node256:
                        {
                            var ptrInNode = stackItem._node + 16;
                            var limit = ptrInNode + stackItem._posInNode * 8;
                            for (; ptrInNode != limit; ptrInNode += 8)
                            {
                                var child = NodeUtils.ReadPtr(ptrInNode);
                                if (child == IntPtr.Zero)
                                    continue;
                                if (NodeUtils.IsPtrPtr(child))
                                {
                                    res += (long)NodeUtils.Ptr2NodeHeader(child)._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType.Node256 | NodeType.Has12BPtrs:
                        {
                            var ptrInNode = stackItem._node + 16;
                            var limit = ptrInNode + stackItem._posInNode * 12;
                            for (; ptrInNode != limit; ptrInNode += 12)
                            {
                                if (NodeUtils.IsPtr12Ptr(ptrInNode))
                                {
                                    var child = NodeUtils.Read12Ptr(ptrInNode);
                                    if (child != IntPtr.Zero)
                                        res += (long)NodeUtils.Ptr2NodeHeader(child)._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                }
            }
            return res;
        }

        internal bool SeekIndex(long index, IntPtr top, ref StructList<CursorItem> stack)
        {
            if (top == IntPtr.Zero)
            {
                return false;
            }
            var keyOffset = 0u;
            while (true)
            {
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                if (index >= (long)header._recursiveChildCount)
                    return false;
                keyOffset += NodeUtils.GetPrefixSize(top);
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                {
                    if (index == 0)
                    {
                        stack.Add().Set(top, keyOffset, -1, 0);
                        return true;
                    }
                    index--;
                }
                keyOffset++;
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        for (int j = 0; j < header._childCount; j++)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(top, j), out var ptr))
                            {
                                var rcc = (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                                if (index < rcc)
                                {
                                    stack.Add().Set(top, keyOffset, (short)j, NodeUtils.ReadByte(top + 16 + j));
                                    top = ptr;
                                    break;
                                }
                                index -= rcc;
                            }
                            else
                            {
                                if (index == 0)
                                {
                                    stack.Add().Set(top, keyOffset, (short)j, NodeUtils.ReadByte(top + 16 + j));
                                    return true;
                                }
                                index--;
                            }
                        }
                        break;
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 0; j < span.Length; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                if (IsPtr(NodeUtils.PtrInNode(top, span[j]), out var ptr))
                                {
                                    var rcc = (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                                    if (index < rcc)
                                    {
                                        stack.Add().Set(top, keyOffset, span[j], (byte)j);
                                        top = ptr;
                                        break;
                                    }
                                    index -= rcc;
                                }
                                else
                                {
                                    if (index == 0)
                                    {
                                        stack.Add().Set(top, keyOffset, span[j], (byte)j);
                                        return true;
                                    }
                                    index--;
                                }
                            }
                        }
                        break;
                    case NodeType.Node256:
                        for (int j = 0; j < 256; j++)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(top, j), out var ptr))
                            {
                                if (ptr == IntPtr.Zero)
                                    continue;
                                var rcc = (long)NodeUtils.Ptr2NodeHeader(ptr)._recursiveChildCount;
                                if (index < rcc)
                                {
                                    stack.Add().Set(top, keyOffset, (short)j, (byte)j);
                                    top = ptr;
                                    break;
                                }
                                index -= rcc;
                            }
                            else
                            {
                                if (index == 0)
                                {
                                    stack.Add().Set(top, keyOffset, (short)j, (byte)j);
                                    return true;
                                }
                                index--;
                            }
                        }
                        break;
                }
            }
        }

        internal bool MoveNext(ref StructList<CursorItem> stack)
        {
            while (stack.Count > 0)
            {
                ref var stackItem = ref stack[stack.Count - 1];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (stackItem._posInNode == -1) stackItem._keyOffset++;
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.NodeLeaf:
                        goto up;
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            if (stackItem._posInNode == header._childCount - 1)
                            {
                                goto up;
                            }
                            stackItem._posInNode++;
                            stackItem._byte = NodeUtils.ReadByte(stackItem._node + 16 + stackItem._posInNode);
                            goto down;
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), 256);
                            for (int j = (stackItem._posInNode == -1) ? 0 : (stackItem._byte + 1); j < 256; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                stackItem._posInNode = span[j];
                                stackItem._byte = (byte)j;
                                goto down;
                            }
                            goto up;
                        }
                    case NodeType.Node256:
                        for (int j = (stackItem._posInNode == -1) ? 0 : (stackItem._byte + 1); j < 256; j++)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(stackItem._node, j), out var ptr2))
                            {
                                if (ptr2 == IntPtr.Zero)
                                {
                                    continue;
                                }
                                stackItem._posInNode = (short)j;
                                stackItem._byte = (byte)j;
                                PushLeftMost(ptr2, (int)stackItem._keyOffset, ref stack);
                                return true;
                            }
                            stackItem._posInNode = (short)j;
                            stackItem._byte = (byte)j;
                            return true;
                        }
                        goto up;
                }
            down:
                if (IsPtr(NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode), out var ptr))
                {
                    PushLeftMost(ptr, (int)stackItem._keyOffset, ref stack);
                }
                return true;
            up:
                stack.Pop();
            }
            return false;
        }

        internal bool MovePrevious(ref StructList<CursorItem> stack)
        {
            while (stack.Count > 0)
            {
                ref var stackItem = ref stack[stack.Count - 1];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (stackItem._posInNode == -1)
                {
                    goto trullyUp;
                }
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            if (stackItem._posInNode == 0)
                            {
                                goto up;
                            }
                            stackItem._posInNode--;
                            stackItem._byte = NodeUtils.ReadByte(stackItem._node + 16 + stackItem._posInNode);
                            goto down;
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), 256);
                            for (int j = stackItem._byte - 1; j >= 0; j--)
                            {
                                if (span[j] == 255)
                                    continue;
                                stackItem._posInNode = span[j];
                                stackItem._byte = (byte)j;
                                goto down;
                            }
                            goto up;
                        }
                    case NodeType.Node256:
                        for (int j = stackItem._byte - 1; j >= 0; j--)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(stackItem._node, j), out var ptr2))
                            {
                                if (ptr2 == IntPtr.Zero)
                                {
                                    continue;
                                }
                            }
                            stackItem._posInNode = (short)j;
                            stackItem._byte = (byte)j;
                            goto down;
                        }
                        goto up;
                }
            down:
                if (IsPtr(NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode), out var ptr))
                {
                    PushRightMost(ptr, (int)stackItem._keyOffset, ref stack);
                }
                return true;
            up:
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                {
                    stackItem._posInNode = -1;
                    stackItem._keyOffset--;
                    stack[stack.Count - 1] = stackItem;
                    return true;
                }
            trullyUp:
                stack.Pop();
            }
            return false;
        }

        internal IntPtr CloneNode(IntPtr nodePtr)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils.BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                unsafe { prefixSize = *(uint*)ptr; };
                ptr += sizeof(uint);
            }
            if ((header._nodeType & NodeType.IsLeaf) == NodeType.IsLeaf)
            {
                if ((header._nodeType & (NodeType.IsLeaf | NodeType.Has12BPtrs)) == NodeType.IsLeaf)
                {
                    unsafe { ptr += *(int*)ptr; };
                    ptr += sizeof(uint);
                    ptr += (int)prefixSize;
                }
                else
                {
                    ptr += (int)prefixSize;
                    ptr = NodeUtils.AlignPtrUpInt32(ptr);
                    ptr += 12;
                }
            }
            else
            {
                ptr += (int)prefixSize;
            }
            var size = (IntPtr)(ptr.ToInt64() - nodePtr.ToInt64());
            var newNode = _allocator.Allocate(size);
            unsafe
            {
                System.Buffer.MemoryCopy(nodePtr.ToPointer(), newNode.ToPointer(), size.ToInt64(), size.ToInt64());
            }
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            newHeader._referenceCount = 1;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        unsafe void CopyMemory(IntPtr src, IntPtr dst, int size)
        {
            Unsafe.CopyBlockUnaligned(dst.ToPointer(), src.ToPointer(), (uint)size);
        }

        unsafe IntPtr ExpandNode(IntPtr nodePtr)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(nodePtr);
            var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(nodePtr);
            var newNodeType = header._nodeType + 1;
            var newNode = AllocateNode(newNodeType, keyPrefixSize, valueSize);
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
            if (newNodeType.HasFlag(NodeType.IsLeaf))
            {
                var (newValueSize, newValuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                CopyMemory(valuePtr, newValuePtr, (int)valueSize);
            }
            CopyMemory(keyPrefixPtr, newKeyPrefixPtr, (int)keyPrefixSize);
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            newHeader._childCount = header._childCount;
            newHeader._recursiveChildCount = header._recursiveChildCount;
            switch (newNodeType & NodeType.NodeSizeMask)
            {
                case NodeType.Node16:
                    {
                        CopyMemory(nodePtr + 16, newNode + 16, 4);
                        CopyMemory(NodeUtils.PtrInNode(nodePtr, 0), NodeUtils.PtrInNode(newNode, 0), 4 * PtrSize);
                        break;
                    }
                case NodeType.Node48:
                    {
                        var srcBytesPtr = (byte*)(nodePtr + 16).ToPointer();
                        var dstBytesPtr = (byte*)(newNode + 16).ToPointer();
                        for (var i = 0; i < 16; i++)
                        {
                            dstBytesPtr[srcBytesPtr[i]] = (byte)i;
                        }
                        CopyMemory(NodeUtils.PtrInNode(nodePtr, 0), NodeUtils.PtrInNode(newNode, 0), 16 * PtrSize);
                        break;
                    }
                case NodeType.Node256:
                    {
                        var srcBytesPtr = (byte*)(nodePtr + 16).ToPointer();
                        for (var i = 0; i < 256; i++)
                        {
                            var pos = srcBytesPtr[i];
                            if (pos == 255) continue;
                            CopyMemory(NodeUtils.PtrInNode(nodePtr, pos), NodeUtils.PtrInNode(newNode, i), PtrSize);
                        }
                        break;
                    }
                default:
                    throw new InvalidOperationException();
            }
            ReferenceAllChildren(newNode);
            return newNode;
        }

        IntPtr CloneNodeWithKeyPrefixCut(IntPtr nodePtr, int skipPrefix)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils.BaseSize(header._nodeType);
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(nodePtr);
            var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(nodePtr);
            var newNode = AllocateNode(header._nodeType, (uint)(keyPrefixSize - skipPrefix), valueSize);
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
            var (newValueSize, newValuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            var backupNewKeyPrefix = newHeader._keyPrefixLength;
            unsafe
            {
                new Span<byte>(nodePtr.ToPointer(), baseSize).CopyTo(new Span<byte>(newNode.ToPointer(), baseSize));
                if (skipPrefix < 0)
                {
                    new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).CopyTo(new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize).Slice(-skipPrefix));
                }
                else
                {
                    new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).Slice(skipPrefix).CopyTo(new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize));
                }
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                {
                    new Span<byte>(valuePtr.ToPointer(), (int)valueSize).CopyTo(new Span<byte>(newValuePtr.ToPointer(), (int)newValueSize));
                }
            }
            newHeader._referenceCount = 1;
            newHeader._keyPrefixLength = backupNewKeyPrefix;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        IntPtr CloneNodeWithValueResize(IntPtr nodePtr, int length)
        {
            ref NodeHeader header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils.BaseSize(header._nodeType);
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(nodePtr);
            var newNodeType = header._nodeType;
            if (length < 0)
            {
                newNodeType = newNodeType & (~NodeType.IsLeaf);
            }
            else
            {
                newNodeType = newNodeType | NodeType.IsLeaf;
            }
            var newNode = AllocateNode(newNodeType, keyPrefixSize, (uint)(length < 0 ? 0 : length));
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
            unsafe
            {
                new Span<byte>(nodePtr.ToPointer(), baseSize).CopyTo(new Span<byte>(newNode.ToPointer(), baseSize));
                new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).CopyTo(new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize));
            }
            ref NodeHeader newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
            newHeader._nodeType = newNodeType;
            newHeader._referenceCount = 1;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        void ReferenceAllChildren(IntPtr node)
        {
            ref var nodeHeader = ref NodeUtils.Ptr2NodeHeader(node);
            switch (nodeHeader._nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                case NodeType.NodeLeaf | NodeType.Has12BPtrs:
                    // does not contain any pointers
                    break;
                case NodeType.Node4:
                    {
                        var p = node + 16 + 4;
                        var limit = p + nodeHeader._childCount * 8;
                        for (; p != limit; p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                NodeUtils.Reference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node4 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 4;
                        var limit = p + nodeHeader._childCount * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node16:
                    {
                        var p = node + 16 + 16;
                        var limit = p + nodeHeader._childCount * 8;
                        for (; p != limit; p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                NodeUtils.Reference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node16 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 16;
                        var limit = p + nodeHeader._childCount * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node48:
                    {
                        var p = node + 16 + 256;
                        var limit = p + nodeHeader._childCount * 8;
                        for (; p != limit; p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                NodeUtils.Reference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node48 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 256;
                        var limit = p + nodeHeader._childCount * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node256:
                    {
                        var p = node + 16;
                        var limit = p + 256 * 8;
                        for (; p != limit; p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                NodeUtils.Reference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node256 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16;
                        var limit = p + 256 * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                NodeUtils.Reference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                default: throw new InvalidOperationException();
            }
        }

        internal void Dereference(IntPtr node)
        {
            if (node == IntPtr.Zero)
                return;
            ref var nodeHeader = ref NodeUtils.Ptr2NodeHeader(node);
            if (!nodeHeader.Dereference()) return;
            switch (nodeHeader._nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                case NodeType.NodeLeaf | NodeType.Has12BPtrs:
                    // does not contain any pointers
                    break;
                case NodeType.Node4:
                    {
                        var p = node + 16 + 4;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node4 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 4;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node16:
                    {
                        var p = node + 16 + 16;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node16 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 16;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node48:
                    {
                        var p = node + 16 + 256;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node48 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16 + 256;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType.Node256:
                    {
                        var p = node + 16;
                        for (var i = 0; i < 256; i++, p += 8)
                        {
                            var child = NodeUtils.ReadPtr(p);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                Dereference(child);
                            }
                        }
                    }
                    break;
                case NodeType.Node256 | NodeType.Has12BPtrs:
                    {
                        var p = node + 16;
                        for (var i = 0; i < 256; i++, p += 12)
                        {
                            if (NodeUtils.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                default: throw new InvalidOperationException();
            }
            _allocator.Deallocate(node);
        }

        void CheckContent12(ReadOnlySpan<byte> content)
        {
            if (content.Length != 12) throw new ArgumentOutOfRangeException(nameof(content));
            if (MemoryMarshal.Read<uint>(content) == uint.MaxValue)
            {
                throw new ArgumentException("Content cannot start with 0xFFFFFFFF when in 12 bytes mode");
            }
        }

        internal void WriteValue(RootNode rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> content)
        {
            if (IsValue12)
            {
                CheckContent12(content);
                MakeUnique(rootNode, stack.AsSpan());
                ref var stackItem = ref stack[stack.Count - 1];
                if (stackItem._posInNode == -1)
                {
                    var ptr = NodeUtils.GetValueSizeAndPtr(stackItem._node).Ptr;
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), 12)); }
                }
                else
                {
                    var ptr = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), 12)); }
                }
            }
            else
            {
                ref var stackItem = ref stack[stack.Count - 1];
                if (stackItem._posInNode == -1)
                {
                    var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                    if (size == content.Length)
                    {
                        MakeUnique(rootNode, stack.AsSpan());
                    }
                    else
                    {
                        MakeUniqueLastResize(rootNode, ref stack, content.Length);
                    }
                    stackItem = ref stack[stack.Count - 1];
                    (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                }
                else
                {
                    MakeUnique(rootNode, stack.AsSpan());
                    if (content.Length < 8)
                    {
                        WriteContentInNode(stack[stack.Count - 1], content);
                    }
                    else
                    {
                        stackItem = ref stack.Add();
                        stackItem.Set(AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf, 0, (uint)content.Length), stack[stack.Count - 1]._keyOffset, -1, 0);
                        WritePtrInNode(stack[stack.Count - 2], stackItem._node);
                        var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                        unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    }
                }
            }
        }

        void MakeUniqueLastResize(RootNode rootNode, ref StructList<CursorItem> stack, int length)
        {
            for (int i = 0; i < stack.Count; i++)
            {
                ref var stackItem = ref stack[(uint)i];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (header._referenceCount == 1 && i != stack.Count - 1)
                    continue;
                IntPtr newNode;
                if (i == stack.Count - 1)
                {
                    newNode = CloneNodeWithValueResize(stackItem._node, length);
                }
                else
                {
                    newNode = CloneNode(stackItem._node);
                }
                OverwriteNodePtrInStack(rootNode, stack.AsSpan(), i, newNode);
            }
        }

        void MakeUniqueAndOverwrite(RootNode rootNode, Span<CursorItem> stack, IntPtr newNode)
        {
            MakeUnique(rootNode, stack);
            if (stack.Length == 0)
            {
                Dereference(rootNode._root);
                rootNode._root = newNode;
            }
            else
            {
                WritePtrInNode(stack[stack.Length - 1], newNode);
            }
        }

        void MakeUnique(RootNode rootNode, Span<CursorItem> stack)
        {
            for (int i = 0; i < stack.Length; i++)
            {
                ref var stackItem = ref stack[i];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                if (header._referenceCount == 1)
                    continue;
                var newNode = CloneNode(stackItem._node);
                OverwriteNodePtrInStack(rootNode, stack, i, newNode);
            }
        }

        void OverwriteNodePtrInStack(RootNode rootNode, Span<CursorItem> stack, int i, IntPtr newNode)
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

        void WritePtrAndByteInNode(in CursorItem stackItem, IntPtr newNode)
        {
            WritePtrInNode(stackItem, newNode);
            var nodeType = NodeUtils.Ptr2NodeHeader(stackItem._node)._nodeType & NodeType.NodeSizeMask;
            if (nodeType != NodeType.Node256)
                NodeUtils.WriteByte(stackItem._node, 16 + stackItem._posInNode, stackItem._byte);
        }

        void WritePtrInNode(in CursorItem stackItem, IntPtr newNode)
        {
            var ptr = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
            WritePtrInNode(ptr, newNode);
        }

        unsafe void WritePtrInNode(IntPtr ptrInNode, IntPtr newNode)
        {
            if (IsValue12)
            {
                if (NodeUtils.IsPtr12Ptr(ptrInNode))
                {
                    Dereference(NodeUtils.Read12Ptr(ptrInNode));
                }
                Unsafe.Write(ptrInNode.ToPointer(), uint.MaxValue);
                Unsafe.Write((ptrInNode + 4).ToPointer(), newNode);
            }
            else
            {
                var child = NodeUtils.ReadPtr(ptrInNode);
                if (NodeUtils.IsPtrPtr(child))
                {
                    Dereference(child);
                }
                Unsafe.Write(ptrInNode.ToPointer(), newNode);
            }
        }

        void WriteContentAndByteInNode(in CursorItem stackItem, ReadOnlySpan<byte> content)
        {
            WriteContentInNode(stackItem, content);
            var nodeType = NodeUtils.Ptr2NodeHeader(stackItem._node)._nodeType & NodeType.NodeSizeMask;
            if (nodeType != NodeType.Node256)
                NodeUtils.WriteByte(stackItem._node, 16 + stackItem._posInNode, stackItem._byte);
        }

        void WriteContentInNode(in CursorItem stackItem, ReadOnlySpan<byte> content)
        {
            var ptr = NodeUtils.PtrInNode(stackItem._node, stackItem._posInNode);
            unsafe
            {
                if (IsValue12)
                {
                    if (NodeUtils.IsPtr12Ptr(ptr))
                    {
                        Dereference(NodeUtils.Read12Ptr(ptr));
                    }
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), 12)); }
                }
                else
                {
                    var child = NodeUtils.ReadPtr(ptr);
                    if (NodeUtils.IsPtrPtr(child))
                    {
                        Dereference(child);
                    }
                    unsafe
                    {
                        NodeUtils.AssertLittleEndian();
                        *(byte*)ptr.ToPointer() = (byte)((content.Length << 1) + 1);
                        content.CopyTo(new Span<byte>(NodeUtils.SkipLenFromPtr(ptr).ToPointer(), 7));
                    }
                }
            }
        }

        unsafe void InitializeZeroPtrValue(IntPtr ptr)
        {
            if (IsValue12)
            {
                var v = new Span<uint>(ptr.ToPointer(), 3);
                v[0] = uint.MaxValue;
                v[1] = 0;
                v[2] = 0;
            }
            else
            {
                var v = new Span<uint>(ptr.ToPointer(), 2);
                v[0] = 0;
                v[1] = 0;
            }
        }

        internal bool FindExact(RootNode rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> key)
        {
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                var keyRest = key.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    stack.Clear();
                    return false;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0 ? 0 : FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (newKeyPrefixSize < keyPrefixSize)
                {
                    stack.Clear();
                    return false;
                }
                if (keyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType.IsLeaf))
                    {
                        stack.Clear();
                        return false;
                    }
                    stack.Add().Set(top, (uint)key.Length, -1, 0);
                    return true;
                }
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Clear();
                    return false;
                }
                var b = key[keyOffset + newKeyPrefixSize];
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add().Set(top, (uint)keyOffset, (short)pos, b);
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    if (key.Length == keyOffset)
                    {
                        return true;
                    }
                }
                stack.Clear();
                return false;
            }
        }

        internal FindResult Find(RootNode rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix, ReadOnlySpan<byte> key)
        {
            stack.Clear();
            var top = rootNode._root;
            if (top == IntPtr.Zero)
            {
                return FindResult.NotFound;
            }
            var keyOffset = 0;
            var totalKeyLength = keyPrefix.Length + key.Length;
            while (true)
            {
                var keyRest = totalKeyLength - keyOffset;
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);

                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var diffPos = commonKeyAndPrefixSize == 0 ? 0 : FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (diffPos < keyPrefixSize)
                {
                    if (keyOffset + diffPos >= keyPrefix.Length)
                    {
                        PushLeftMost(top, keyOffset, ref stack);
                        if (diffPos >= keyRest) return FindResult.Next;
                        return NodeUtils.ReadByte(keyPrefixPtr + diffPos) < GetByteFromKeyPair(keyPrefix, key, keyOffset + diffPos) ? FindResult.Previous : FindResult.Next;
                    }
                    stack.Clear();
                    return FindResult.NotFound;
                }
                if (keyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType.IsLeaf))
                    {
                        PushLeftMost(top, keyOffset, ref stack);
                        return FindResult.Next;
                    }
                    stack.Add().Set(top, (uint)key.Length, -1, 0);
                    return FindResult.Exact;
                }
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    keyOffset += diffPos;
                    if (keyOffset >= keyPrefix.Length)
                    {
                        stack.Add().Set(top, (uint)keyOffset, -1, 0);
                        return FindResult.Previous;
                    }
                    stack.Clear();
                    return FindResult.NotFound;
                }
                var b = GetByteFromKeyPair(keyPrefix, key, keyOffset + diffPos);
                var pos = Find(top, b);
                keyOffset += diffPos + 1;
                if (pos >= 0)
                {
                    stack.Add().Set(top, (uint)keyOffset, (short)pos, b);
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    if (totalKeyLength == keyOffset)
                    {
                        return FindResult.Exact;
                    }
                    if (keyOffset >= keyPrefix.Length)
                    {
                        return FindResult.Previous;
                    }
                    stack.Clear();
                    return FindResult.NotFound;
                }
                if (keyOffset <= keyPrefix.Length)
                {
                    stack.Clear();
                    return FindResult.NotFound;
                }
                // scope
                {
                    var (nearPos, nearByte) = FindNearPosAndByte(top, pos, b);
                    stack.Add().Set(top, (uint)keyOffset, nearPos, nearByte);
                    if (IsPtr(NodeUtils.PtrInNode(top, nearPos), out var newTop))
                    {
                        if (nearByte < b)
                        {
                            PushRightMost(newTop, keyOffset, ref stack);
                            return FindResult.Previous;
                        }
                        else
                        {
                            PushLeftMost(newTop, keyOffset, ref stack);
                            return FindResult.Next;
                        }
                    }
                    if (nearByte < b)
                    {
                        return FindResult.Previous;
                    }
                    else
                    {
                        return FindResult.Next;
                    }
                }
            }
        }

        unsafe (short nearPos, byte nearByte) FindNearPosAndByte(IntPtr node, int pos, byte @byte)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(node);
            switch (header._nodeType & NodeType.NodeSizeMask)
            {
                case NodeType.Node4:
                case NodeType.Node16:
                    {
                        pos = ~pos;
                        if (pos >= header._childCount) pos--;
                        return ((short)pos, NodeUtils.ReadByte(node + 16 + pos));
                    }
                case NodeType.Node48:
                    {
                        var bytePtrs = new Span<byte>((node + 16).ToPointer(), 256);
                        pos = @byte + 1;
                        while (pos < 256)
                        {
                            if (bytePtrs[pos] != 255)
                            {
                                return (bytePtrs[pos], (byte)pos);
                            }
                            pos++;
                        }
                        pos = @byte - 1;
                        while (true)
                        {
                            if (bytePtrs[pos] != 255)
                            {
                                return (bytePtrs[pos], (byte)pos);
                            }
                            pos--;
                        }
                    }
                case NodeType.Node256:
                    {
                        pos = @byte + 1;
                        while (pos < 256)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(node, pos), out var ptr))
                            {
                                if (ptr == IntPtr.Zero)
                                {
                                    pos++;
                                    continue;
                                }
                            }
                            return ((short)pos, (byte)pos);
                        }
                        pos = @byte - 1;
                        while (pos >= 0)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(node, pos), out var ptr))
                            {
                                if (ptr == IntPtr.Zero)
                                {
                                    pos--;
                                    continue;
                                }
                            }
                            return ((short)pos, (byte)pos);
                        }
                        break;
                    }
            }
            throw new InvalidOperationException();
        }

        byte GetByteFromKeyPair(in ReadOnlySpan<byte> key1, in ReadOnlySpan<byte> key2, int offset)
        {
            if (offset >= key1.Length)
            {
                return key2[offset - key1.Length];
            }
            return key1[offset];
        }

        internal bool FindFirst(RootNode rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
        {
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                var keyRest = keyPrefix.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    stack.Clear();
                    return false;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0 ? 0 : FindFirstDifference(keyPrefix.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (newKeyPrefixSize < keyPrefixSize && newKeyPrefixSize < keyRest)
                {
                    stack.Clear();
                    return false;
                }
                if (newKeyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType.IsLeaf))
                    {
                        PushLeftMost(top, keyOffset, ref stack);
                        return true;
                    }
                    stack.Add().Set(top, (uint)keyOffset + keyPrefixSize, -1, 0);
                    return true;
                }
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Clear();
                    return false;
                }
                var b = keyPrefix[keyOffset + newKeyPrefixSize];
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add().Set(top, (uint)keyOffset, (short)pos, b);
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    if (keyPrefix.Length == keyOffset)
                    {
                        return true;
                    }
                }
                stack.Clear();
                return false;
            }
        }

        internal bool FindLast(RootNode rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
        {
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                var keyRest = keyPrefix.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    stack.Clear();
                    return false;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0 ? 0 : FindFirstDifference(keyPrefix.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (newKeyPrefixSize < keyPrefixSize && newKeyPrefixSize < keyRest)
                {
                    stack.Clear();
                    return false;
                }
                if (newKeyPrefixSize == keyRest)
                {
                    PushRightMost(top, keyOffset, ref stack);
                    return true;
                }
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Clear();
                    return false;
                }
                var b = keyPrefix[keyOffset + newKeyPrefixSize];
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add().Set(top, (uint)keyOffset, (short)pos, b);
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        top = newTop;
                        continue;
                    }
                    if (keyPrefix.Length == keyOffset)
                    {
                        return true;
                    }
                }
                stack.Clear();
                return false;
            }
        }

        void PushLeftMost(IntPtr top, int keyOffset, ref StructList<CursorItem> stack)
        {
            while (true)
            {
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                keyOffset += (int)NodeUtils.GetPrefixSize(top);
                if (header._nodeType.HasFlag(NodeType.IsLeaf))
                {
                    stack.Add().Set(top, (uint)keyOffset, -1, 0);
                    return;
                }
                keyOffset++;
                switch (header._nodeType & NodeType.NodeSizePtrMask)
                {
                    case NodeType.Node4:
                        {
                            stack.Add().Set(top, (uint)keyOffset, 0, NodeUtils.ReadByte(top + 16));
                            var child = NodeUtils.ReadPtr(top + 16 + 4);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                top = child;
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType.Node4 | NodeType.Has12BPtrs:
                        {
                            stack.Add().Set(top, (uint)keyOffset, 0, NodeUtils.ReadByte(top + 16));
                            var ptr = top + 16 + 4;
                            if (NodeUtils.IsPtr12Ptr(ptr))
                            {
                                top = NodeUtils.Read12Ptr(ptr);
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType.Node16:
                        {
                            stack.Add().Set(top, (uint)keyOffset, 0, NodeUtils.ReadByte(top + 16));
                            var child = NodeUtils.ReadPtr(top + 16 + 16);
                            if (NodeUtils.IsPtrPtr(child))
                            {
                                top = child;
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType.Node16 | NodeType.Has12BPtrs:
                        {
                            stack.Add().Set(top, (uint)keyOffset, 0, NodeUtils.ReadByte(top + 16));
                            var ptr = top + 16 + 16;
                            if (NodeUtils.IsPtr12Ptr(ptr))
                            {
                                top = NodeUtils.Read12Ptr(ptr);
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 0; true; j++)
                            {
                                var pos = span[j];
                                if (pos == 255)
                                    continue;
                                stack.Add().Set(top, (uint)keyOffset, pos, (byte)j);
                                var child = NodeUtils.ReadPtr(top + 16 + 256 + pos * 8);
                                if (NodeUtils.IsPtrPtr(child))
                                {
                                    top = child;
                                    break;
                                }
                                return;
                            }
                            break;
                        }
                    case NodeType.Node48 | NodeType.Has12BPtrs:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 0; true; j++)
                            {
                                var pos = span[j];
                                if (pos == 255)
                                    continue;
                                stack.Add().Set(top, (uint)keyOffset, pos, (byte)j);
                                var ptr = top + 16 + 256 + pos * 12;
                                if (NodeUtils.IsPtr12Ptr(ptr))
                                {
                                    top = NodeUtils.Read12Ptr(ptr);
                                    break;
                                }
                                return;
                            }
                            break;
                        }
                    case NodeType.Node256:
                        {
                            var p = top + 16;
                            for (var j = 0; true; j++, p += 8)
                            {
                                var child = NodeUtils.ReadPtr(p);
                                if (NodeUtils.IsPtrPtr(child))
                                {
                                    if (child != IntPtr.Zero)
                                    {
                                        stack.Add().Set(top, (uint)keyOffset, (short)j, (byte)j);
                                        top = child;
                                        break;
                                    }
                                    continue;
                                }
                                else
                                {
                                    stack.Add().Set(top, (uint)keyOffset, (short)j, (byte)j);
                                    return;
                                }
                            }
                        }
                        break;
                    case NodeType.Node256 | NodeType.Has12BPtrs:
                        {
                            var p = top + 16;
                            for (var j = 0; true; j++, p += 12)
                            {
                                if (NodeUtils.IsPtr12Ptr(p))
                                {
                                    var child = NodeUtils.Read12Ptr(p);
                                    if (child != IntPtr.Zero)
                                    {
                                        stack.Add().Set(top, (uint)keyOffset, (short)j, (byte)j);
                                        top = child;
                                        break;
                                    }
                                    continue;
                                }
                                else
                                {
                                    stack.Add().Set(top, (uint)keyOffset, (short)j, (byte)j);
                                    return;
                                }
                            }
                        }
                        break;
                }
            }
        }

        void PushRightMost(IntPtr top, int keyOffset, ref StructList<CursorItem> stack)
        {
            while (true)
            {
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                keyOffset += (int)NodeUtils.GetPrefixSize(top);
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    stack.Add().Set(top, (uint)keyOffset, -1, 0);
                    return;
                }
                keyOffset++;
                switch (header._nodeType & NodeType.NodeSizeMask)
                {
                    case NodeType.Node4:
                    case NodeType.Node16:
                        {
                            var pos = header._childCount - 1;
                            stack.Add().Set(top, (uint)keyOffset, (short)pos, NodeUtils.ReadByte(top + 16 + pos));
                            if (IsPtr(NodeUtils.PtrInNode(top, pos), out var ptr))
                            {
                                top = ptr;
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 255; true; j--)
                            {
                                if (span[j] == 255)
                                    continue;
                                stack.Add().Set(top, (uint)keyOffset, span[j], (byte)j);
                                if (IsPtr(NodeUtils.PtrInNode(top, span[j]), out var ptr))
                                {
                                    top = ptr;
                                    break;
                                }
                                return;
                            }
                            break;
                        }
                    case NodeType.Node256:
                        for (int j = 255; true; j--)
                        {
                            if (IsPtr(NodeUtils.PtrInNode(top, j), out var ptr))
                            {
                                if (ptr != IntPtr.Zero)
                                {
                                    stack.Add().Set(top, (uint)keyOffset, (short)j, (byte)j);
                                    top = ptr;
                                    break;
                                }
                                continue;
                            }
                            else
                            {
                                stack.Add().Set(top, (uint)keyOffset, (short)j, (byte)j);
                                return;
                            }
                        }
                        break;
                }
            }
        }

        internal bool Upsert(RootNode rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> key, ReadOnlySpan<byte> content)
        {
            if (IsValue12)
                CheckContent12(content);
            stack.Clear();
            var top = rootNode._root;
            var keyOffset = 0;
            while (true)
            {
                var keyRest = key.Length - keyOffset;
                if (top == IntPtr.Zero)
                {
                    // nodes on stack must be already unique
                    if (keyRest == 0 && (IsValue12 || content.Length < 8) && stack.Count > 0)
                    {
                        WriteContentInNode(stack[stack.Count - 1], content);
                        AdjustRecursiveChildCount(stack.AsSpan(), +1);
                        return true;
                    }
                    ref var stackItem = ref stack.Add();
                    stackItem.Set(AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf, (uint)keyRest, (uint)content.Length), (uint)key.Length, -1, 0);
                    var (size, ptr) = NodeUtils.GetPrefixSizeAndPtr(stackItem._node);
                    unsafe { key.Slice(keyOffset).CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, stackItem._node);
                    AdjustRecursiveChildCount(stack.AsSpan(0, (int)stack.Count - 1), +1);
                    return true;
                }
                ref var header = ref NodeUtils.Ptr2NodeHeader(top);
                var headerBackup = header;
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0 ? 0 : FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (newKeyPrefixSize < keyPrefixSize)
                {
                    MakeUnique(rootNode, stack.AsSpan());
                    var nodeType = NodeType.Node4 | (newKeyPrefixSize == keyRest ? NodeType.IsLeaf : 0);
                    var newNode = AllocateNode(nodeType, (uint)newKeyPrefixSize, (uint)content.Length);
                    try
                    {
                        ref var newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = header._recursiveChildCount;
                        var (size, ptr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
                        unsafe { key.Slice(keyOffset, newKeyPrefixSize).CopyTo(new Span<byte>(ptr.ToPointer(), newKeyPrefixSize)); }
                        if (IsValue12 && (header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf && newKeyPrefixSize + 1 == keyPrefixSize)
                        {
                            var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(top);
                            unsafe
                            {
                                WriteContentAndByteInNode(new CursorItem(newNode, 0, 0, NodeUtils.ReadByte(keyPrefixPtr + newKeyPrefixSize)), new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                            }
                        }
                        else
                        {
                            var newNode2 = CloneNodeWithKeyPrefixCut(top, newKeyPrefixSize + 1);
                            WritePtrAndByteInNode(new CursorItem(newNode, 0, 0, NodeUtils.ReadByte(keyPrefixPtr + newKeyPrefixSize)), newNode2);
                        }
                        if (nodeType.HasFlag(NodeType.IsLeaf))
                        {
                            stack.Add().Set(newNode, (uint)key.Length, -1, 0);
                            (size, ptr) = NodeUtils.GetValueSizeAndPtr(newNode);
                            unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                            keyOffset = key.Length;
                            AdjustRecursiveChildCount(stack.AsSpan(), +1);
                            OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, newNode);
                            newNode = IntPtr.Zero;
                            return true;
                        }
                        else
                        {
                            keyOffset += newKeyPrefixSize + 1;
                            var b2 = key[keyOffset - 1];
                            var pos2 = InsertChildIntoNode4(newNode, b2);
                            stack.Add().Set(newNode, (uint)keyOffset, pos2, b2);
                            top = IntPtr.Zero;
                            OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, newNode);
                            newNode = IntPtr.Zero;
                            continue;
                        }
                    }
                    finally
                    {
                        Dereference(newNode);
                    }
                }
                if (keyPrefixSize == keyRest)
                {
                    stack.Add().Set(top, (uint)key.Length, -1, 0);
                    var hadIsLeaf = header._nodeType.HasFlag(NodeType.IsLeaf);
                    if (header._nodeType.HasFlag(NodeType.IsLeaf) && (IsValue12 || NodeUtils.GetValueSizeAndPtr(top).Size == content.Length))
                    {
                        MakeUnique(rootNode, stack.AsSpan());
                    }
                    else
                    {
                        MakeUniqueLastResize(rootNode, ref stack, content.Length);
                    }
                    var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stack[stack.Count - 1]._node);
                    unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                    if (!hadIsLeaf)
                    {
                        AdjustRecursiveChildCount(stack.AsSpan(), +1);
                        return true;
                    }
                    return false;
                }
                var b = key[keyOffset + newKeyPrefixSize];
                if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                {
                    MakeUnique(rootNode, stack.AsSpan());
                    var nodeType = NodeType.Node4 | NodeType.IsLeaf;
                    var (topValueSize, topValuePtr) = NodeUtils.GetValueSizeAndPtr(top);
                    var newNode = AllocateNode(nodeType, (uint)newKeyPrefixSize, topValueSize);
                    try
                    {
                        ref var newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = 1;
                        var (size, ptr) = NodeUtils.GetPrefixSizeAndPtr(newNode);
                        unsafe { key.Slice(keyOffset, newKeyPrefixSize).CopyTo(new Span<byte>(ptr.ToPointer(), newKeyPrefixSize)); }
                        var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                        unsafe
                        {
                            new Span<byte>(topValuePtr.ToPointer(), (int)topValueSize).CopyTo(new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                        }
                        keyOffset += newKeyPrefixSize + 1;
                        NodeUtils.WriteByte(newNode, 16, b);
                        stack.Add().Set(newNode, (uint)keyOffset, 0, b);
                        top = IntPtr.Zero;
                        OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, newNode);
                        newNode = IntPtr.Zero;
                        continue;
                    }
                    finally
                    {
                        Dereference(newNode);
                    }
                }
                var pos = Find(top, b);
                if (pos >= 0)
                {
                    keyOffset += newKeyPrefixSize + 1;
                    stack.Add().Set(top, (uint)keyOffset, (short)pos, b);
                    if (IsPtr(NodeUtils.PtrInNode(top, pos), out var newTop))
                    {
                        if (key.Length == keyOffset && IsValueInlinable(content) && (NodeUtils.Ptr2NodeHeader(newTop)._nodeType & NodeType.NodeSizeMask) == NodeType.NodeLeaf)
                        {
                            MakeUnique(rootNode, stack.AsSpan());
                            WriteContentInNode(stack[stack.Count - 1], content);
                            return false;
                        }
                        top = newTop;
                        continue;
                    }
                    MakeUnique(rootNode, stack.AsSpan());
                    if (key.Length == keyOffset)
                    {
                        if (IsValueInlinable(content))
                        {
                            WriteContentInNode(stack[stack.Count - 1], content);
                        }
                        else
                        {
                            ref var stackItem = ref stack.Add();
                            stackItem.Set(AllocateNode(NodeType.NodeLeaf | NodeType.IsLeaf, 0, (uint)content.Length), (uint)key.Length, -1, 0);
                            var (size, ptr) = NodeUtils.GetValueSizeAndPtr(stackItem._node);
                            unsafe { content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size)); }
                            OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, stackItem._node);
                        }
                        return false;
                    }
                    var nodeType = NodeType.Node4 | NodeType.IsLeaf;
                    var (topValueSize, topValuePtr) = GetValueSizeAndPtrFromPtrInNode(NodeUtils.PtrInNode(top, pos));
                    var newNode = AllocateNode(nodeType, 0, topValueSize);
                    try
                    {
                        ref var newHeader = ref NodeUtils.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = 1;
                        var (valueSize, valuePtr) = NodeUtils.GetValueSizeAndPtr(newNode);
                        unsafe
                        {
                            new Span<byte>(topValuePtr.ToPointer(), (int)topValueSize).CopyTo(new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                        }
                        b = key[keyOffset++];
                        stack.Add().Set(newNode, (uint)keyOffset, 0, b);
                        top = IntPtr.Zero;
                        OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, newNode);
                        newNode = IntPtr.Zero;
                        continue;
                    }
                    finally
                    {
                        Dereference(newNode);
                    }
                }
                pos = ~pos;
                MakeUnique(rootNode, stack.AsSpan());
                bool topChanged = false;
                if (header.IsFull)
                {
                    top = ExpandNode(top);
                    topChanged = true;
                }
                else if (header._referenceCount > 1)
                {
                    top = CloneNode(top);
                    topChanged = true;
                }
                InsertChildRaw(top, ref pos, b);
                keyOffset += newKeyPrefixSize + 1;
                stack.Add().Set(top, (uint)keyOffset, (short)pos, b);
                if (topChanged)
                {
                    OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, top);
                }
                top = IntPtr.Zero;
            }
        }

        (uint, IntPtr) GetValueSizeAndPtrFromPtrInNode(IntPtr ptr)
        {
            if (IsValue12)
                return (12, ptr);
            return (NodeUtils.ReadLenFromPtr(ptr), NodeUtils.SkipLenFromPtr(ptr));
        }

        bool IsValueInlinable(ReadOnlySpan<byte> content)
        {
            if (IsValue12) return true;
            if (content.Length < 8) return true;
            return false;
        }

        bool IsPtr12(IntPtr ptr, out IntPtr pointsTo)
        {
            if (NodeUtils.IsPtr12Ptr(ptr))
            {
                pointsTo = NodeUtils.Read12Ptr(ptr);
                return true;
            }
            pointsTo = IntPtr.Zero;
            return false;
        }

        bool IsPtrX(IntPtr ptr, out IntPtr pointsTo)
        {
            var child = NodeUtils.ReadPtr(ptr);
            if (NodeUtils.IsPtrPtr(child))
            {
                pointsTo = child;
                return true;
            }
            pointsTo = IntPtr.Zero;
            return false;
        }

        bool IsPtr(IntPtr ptr, out IntPtr pointsTo)
        {
            if (IsValue12)
            {
                if (NodeUtils.IsPtr12Ptr(ptr))
                {
                    pointsTo = NodeUtils.Read12Ptr(ptr);
                    return true;
                }
            }
            else
            {
                var child = NodeUtils.ReadPtr(ptr);
                if (NodeUtils.IsPtrPtr(child))
                {
                    pointsTo = child;
                    return true;
                }
            }
            pointsTo = IntPtr.Zero;
            return false;
        }

        unsafe int Find(IntPtr nodePtr, byte b)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node256)
            {
                var ptr = NodeUtils.PtrInNode(nodePtr, b);
                if (IsValue12)
                {
                    if (NodeUtils.IsPtr12Ptr(ptr) && NodeUtils.Read12Ptr(ptr) == IntPtr.Zero)
                        return ~b;
                }
                else
                {
                    if (NodeUtils.ReadPtr(ptr) == IntPtr.Zero)
                        return ~b;
                }
                return b;
            }
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node48)
            {
                var pos = NodeUtils.ReadByte(nodePtr + 16 + b);
                if (pos == 255)
                    return ~header._childCount;
                return pos;
            }
            else
            {
                var childernBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
                return BinarySearch(childernBytes, b);
            }
        }

        int BinarySearch(ReadOnlySpan<byte> data, byte value)
        {
            var l = 0;
            ref var d = ref MemoryMarshal.GetReference(data);
            var r = data.Length;
            while (l < r)
            {
                var m = (int)(((uint)l + (uint)r) >> 1);
                var diff = Unsafe.Add(ref d, m) - value;
                if (diff == 0) return m;
                if (diff > 0)
                {
                    r = m;
                }
                else
                {
                    l = m + 1;
                }
            }
            return ~l;
        }

        unsafe short InsertChildIntoNode4(IntPtr nodePtr, byte b)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            var childernBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
            var pos = BinarySearch(childernBytes, b);
            pos = ~pos;
            if (pos < childernBytes.Length)
            {
                childernBytes.Slice(pos).CopyTo(new Span<byte>((nodePtr + 16).ToPointer(), header._childCount + 1).Slice(pos + 1));
                var chPtr = NodeUtils.PtrInNode(nodePtr, pos);
                var chSize = PtrSize * (header._childCount - pos);
                new Span<byte>(chPtr.ToPointer(), chSize).CopyTo(new Span<byte>((chPtr + PtrSize).ToPointer(), chSize));
            }
            NodeUtils.WriteByte(nodePtr, 16 + pos, b);
            header._childCount++;
            InitializeZeroPtrValue(NodeUtils.PtrInNode(nodePtr, pos));
            return (short)pos;
        }

        unsafe void InsertChildRaw(IntPtr nodePtr, ref int pos, byte b)
        {
            ref var header = ref NodeUtils.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node256)
            {
                pos = b;
                header._childCount++;
                InitializeZeroPtrValue(NodeUtils.PtrInNode(nodePtr, pos));
                return;
            }
            if ((header._nodeType & NodeType.NodeSizeMask) == NodeType.Node48)
            {
                pos = header._childCount;
                NodeUtils.WriteByte(nodePtr, 16 + b, (byte)pos);
            }
            else
            {
                var childernBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
                if (pos < childernBytes.Length)
                {
                    childernBytes.Slice(pos).CopyTo(new Span<byte>((nodePtr + 16).ToPointer(), header._childCount + 1).Slice(pos + 1));
                    var chPtr = NodeUtils.PtrInNode(nodePtr, pos);
                    var chSize = PtrSize * (header._childCount - pos);
                    new Span<byte>(chPtr.ToPointer(), chSize).CopyTo(new Span<byte>((chPtr + PtrSize).ToPointer(), chSize));
                }
                NodeUtils.WriteByte(nodePtr, 16 + pos, b);
            }
            header._childCount++;
            InitializeZeroPtrValue(NodeUtils.PtrInNode(nodePtr, pos));
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
                        if (Unsafe.ReadUnaligned<Vector<byte>>(buf1Ptr + i) != Unsafe.ReadUnaligned<Vector<byte>>(buf2Ptr + i))
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

        void AdjustRecursiveChildCount(Span<CursorItem> stack, long delta)
        {
            for (int i = 0; i < stack.Length; i++)
            {
                ref var stackItem = ref stack[i];
                ref var header = ref NodeUtils.Ptr2NodeHeader(stackItem._node);
                header._recursiveChildCount = (ulong)unchecked((long)header._recursiveChildCount + delta);
            }
        }
    }
}
