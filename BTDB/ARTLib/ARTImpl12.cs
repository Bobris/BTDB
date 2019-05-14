using BTDB.Collections;
using BTDB.KVDBLayer;
using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
{
    public class ARTImpl12
    {
        readonly IOffHeapAllocator _allocator;
        internal const int PtrSize = 12;

        internal ARTImpl12(IOffHeapAllocator allocator)
        {
            _allocator = allocator;
        }

        public static IRootNode CreateEmptyRoot(IOffHeapAllocator allocator)
        {
            return new RootNode12(new ARTImpl12(allocator));
        }

        unsafe internal IntPtr AllocateNode(NodeType12 nodeType, uint keyPrefixLength, uint valueLength)
        {
            IntPtr node;
            int baseSize;
            baseSize = NodeUtils12.BaseSize(nodeType).Base;
            var size = baseSize + ArtUtils.AlignUIntUpInt32(keyPrefixLength) +
                        (nodeType.HasFlag(NodeType12.IsLeaf) ? 12 : 0);
            if (keyPrefixLength >= 0xffff) size += 4;
            node = _allocator.Allocate((IntPtr)size);

            ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeader(node);
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

            if ((nodeType & NodeType12.NodeSizeMask) == NodeType12.Node48)
            {
                Unsafe.InitBlock((node + 16).ToPointer(), 255, 256);
            }

            if ((nodeType & NodeType12.NodeSizeMask) == NodeType12.Node256)
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

        internal long EraseRange(RootNode12 rootNode, ref StructList<CursorItem> left, ref StructList<CursorItem> right)
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
                        (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, -1, 0, IntPtr.Zero, -1, 0,
                            IntPtr.Zero);
                        goto up;
                    }

                    if (leftIndex + 1 == left.Count)
                    {
                        (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, leftItem._posInNode,
                            leftItem._byte, IntPtr.Zero, rightItem._posInNode, rightItem._byte, IntPtr.Zero);
                        goto up;
                    }

                    if (NodeUtils12.Ptr2NodeHeader(leftItem._node)._referenceCount > 1)
                        isUnique = false;
                    leftIndex++;
                    rightIndex++;
                    continue;
                }

                var downUnique = isUnique;
                if (NodeUtils12.Ptr2NodeHeader(leftItem._node)._referenceCount > 1)
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

                (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, leftItem._posInNode, leftItem._byte,
                    leftNode, rightItem._posInNode, rightItem._byte, rightNode);
                goto up;
            }

        up:
            if (newNode == IntPtr.Zero)
            {
                if (leftIndex > 0)
                {
                    leftIndex--;
                    ref var leftItem = ref left[leftIndex];
                    (newNode, children) = EraseRangeFromNode(isUnique, leftItem._node, leftItem._posInNode,
                        leftItem._byte, IntPtr.Zero, leftItem._posInNode, leftItem._byte, IntPtr.Zero);
                }
            }

            MakeUniqueAndOverwriteIfNeeded(rootNode, left.AsSpan(0, (int)leftIndex), newNode);
            AdjustRecursiveChildCount(left.AsSpan(0, (int)leftIndex), -children);
            left.Clear();
            right.Clear();
            return children;
        }

        (IntPtr newNode, long children) EraseFromStart(bool downUnique, Span<CursorItem> span)
        {
            var downDownUnique = downUnique;
            var node = span[0]._node;
            if (NodeUtils12.Ptr2NodeHeader(node)._referenceCount > 1)
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
            var (resNode, resChildren) = EraseRangeFromNode(downUnique, span[0]._node, startPos, startByte, IntPtr.Zero,
                span[0]._posInNode, span[0]._byte, newNode);
            return (resNode, resChildren + children);
        }

        (short pos, byte @byte) GetStartPosAndByte(IntPtr node)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
            if (header._nodeType.HasFlag(NodeType12.IsLeaf))
            {
                return (-1, 0);
            }

            switch (header._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.Node4:
                case NodeType12.Node16:
                    return (0, ArtUtils.ReadByte(node + 16));
                case NodeType12.Node48:
                    for (var i = 0; i < 256; i++)
                    {
                        var pos = ArtUtils.ReadByte(node + 16 + i);
                        if (pos == 255)
                            continue;
                        return (pos, (byte)i);
                    }

                    break;
                case NodeType12.Node256:
                    for (var i = 0; i < 256; i++)
                    {
                        if (IsPtr(NodeUtils12.PtrInNode(node, i), out var ptr))
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
            ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
            switch (header._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.Node4:
                case NodeType12.Node16:
                    return (0, ArtUtils.ReadByte(node + 16));
                case NodeType12.Node48:
                    for (var i = 0; i < 256; i++)
                    {
                        var pos = ArtUtils.ReadByte(node + 16 + i);
                        if (pos == 255)
                            continue;
                        return (pos, (byte)i);
                    }

                    break;
                case NodeType12.Node256:
                    for (var i = 0; i < 256; i++)
                    {
                        if (IsPtr(NodeUtils12.PtrInNode(node, i), out var ptr))
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
            if (NodeUtils12.Ptr2NodeHeader(node)._referenceCount > 1)
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
            var (resNode, resChildren) = EraseRangeFromNode(downUnique, span[0]._node, span[0]._posInNode,
                span[0]._byte, newNode, endPos, endByte, IntPtr.Zero);
            return (resNode, resChildren + children);
        }

        (short pos, byte @byte) GetEndPosAndByte(IntPtr node)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
            switch (header._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf:
                    return (-1, 0);
                case NodeType12.Node4:
                case NodeType12.Node16:
                    {
                        var pos = header._childCount - 1;
                        return ((short)pos, ArtUtils.ReadByte(node + 16 + pos));
                    }
                case NodeType12.Node48:
                    for (var i = 255; i >= 0; i--)
                    {
                        var pos = ArtUtils.ReadByte(node + 16 + i);
                        if (pos == 255)
                            continue;
                        return (pos, (byte)i);
                    }

                    break;
                case NodeType12.Node256:
                    for (var i = 255; i >= 0; i--)
                    {
                        if (IsPtr(NodeUtils12.PtrInNode(node, i), out var ptr))
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

        (IntPtr newNode, long children) EraseRangeFromNode(bool canBeInplace, IntPtr node, short leftPos, byte leftByte,
            IntPtr leftNode, short rightPos, byte rightByte, IntPtr rightNode)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
            if (header._referenceCount > 1)
                canBeInplace = false;
            if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
            {
                return (IntPtr.Zero, 1);
            }

            var willBeIsLeaf = header._nodeType.HasFlag(NodeType12.IsLeaf) && (leftPos > -1);
            var willBeChildCount = header.ChildCount;
            var children = 0L;
            if (leftPos == -1)
            {
                children++;
                (leftPos, leftByte) = GetStartPosAndByteSkipLeaf(node);
            }

            switch (header._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.Node4:
                case NodeType12.Node16:
                    for (var i = leftPos; i <= rightPos; i++)
                    {
                        willBeChildCount--;
                        if (IsPtr(NodeUtils12.PtrInNode(node, i), out var ptr))
                        {
                            children += (long)NodeUtils12.Ptr2NodeHeader(ptr)._recursiveChildCount;
                        }
                        else
                        {
                            children++;
                        }
                    }

                    break;
                case NodeType12.Node48:
                    unsafe
                    {
                        var span = new Span<byte>((node + 16).ToPointer(), 256);
                        for (int i = leftByte; i <= rightByte; i++)
                        {
                            if (span[i] == 255)
                                continue;
                            willBeChildCount--;
                            if (IsPtr(NodeUtils12.PtrInNode(node, span[i]), out var ptr))
                            {
                                children += (long)NodeUtils12.Ptr2NodeHeader(ptr)._recursiveChildCount;
                            }
                            else
                            {
                                children++;
                            }
                        }
                    }

                    break;
                case NodeType12.Node256:
                    for (int j = leftByte; j <= rightByte; j++)
                    {
                        if (IsPtr(NodeUtils12.PtrInNode(node, j), out var ptr))
                        {
                            if (ptr == IntPtr.Zero)
                                continue;
                            children += (long)NodeUtils12.Ptr2NodeHeader(ptr)._recursiveChildCount;
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
                children -= (long)NodeUtils12.Ptr2NodeHeader(leftNode)._recursiveChildCount;
                willBeChildCount++;
            }

            if (rightNode != IntPtr.Zero)
            {
                children -= (long)NodeUtils12.Ptr2NodeHeader(rightNode)._recursiveChildCount;
                willBeChildCount++;
            }

            if (willBeChildCount == 0 && !willBeIsLeaf)
            {
                return (IntPtr.Zero, children);
            }

            var newNodeType = NodeType12.NodeLeaf;
            if (willBeChildCount > 48)
            {
                newNodeType = NodeType12.Node256;
            }
            else if (willBeChildCount > 16)
            {
                newNodeType = NodeType12.Node48;
            }
            else if (willBeChildCount > 4)
            {
                newNodeType = NodeType12.Node16;
            }
            else if ((willBeChildCount > 1) || (willBeChildCount == 1 && willBeIsLeaf))
            {
                newNodeType = NodeType12.Node4;
            }

            if (willBeIsLeaf)
            {
                newNodeType |= NodeType12.IsLeaf;
            }

            if (canBeInplace && header._nodeType == newNodeType)
            {
                switch (newNodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                    case NodeType12.Node16:
                        {
                            if (leftNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils12.PtrInNode(node, leftPos), leftNode);
                                leftPos++;
                            }

                            if (rightNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils12.PtrInNode(node, rightPos), rightNode);
                                rightPos--;
                            }

                            if (leftPos <= rightPos)
                            {
                                for (var i = leftPos; i <= rightPos; i++)
                                {
                                    WritePtrInNode(NodeUtils12.PtrInNode(node, i), IntPtr.Zero);
                                }

                                ArtUtils.MoveMemory(node + 16 + rightPos + 1, node + 16 + leftPos,
                                    header._childCount - rightPos - 1);
                                ArtUtils.MoveMemory(NodeUtils12.PtrInNode(node, rightPos + 1), NodeUtils12.PtrInNode(node, leftPos),
                                    (header._childCount - rightPos - 1) * PtrSize);
                            }

                            break;
                        }
                    case NodeType12.Node48:
                        {
                            if (leftNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils12.PtrInNode(node, leftPos), leftNode);
                                leftByte++;
                            }

                            if (rightNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils12.PtrInNode(node, rightPos), rightNode);
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
                                            bytePtrs[i] = 255;
                                            // Just to decrease reference count
                                            WritePtrInNode(NodeUtils12.PtrInNode(node, idx), IntPtr.Zero);
                                        }
                                        else
                                        {
                                            bytePtrs[i] = (byte)outPos;
                                            new Span<byte>((node + 16 + 256 + idx * PtrSize).ToPointer(), PtrSize).CopyTo(
                                                tempItems.Slice(outPos * PtrSize));
                                            outPos++;
                                        }
                                    }

                                    tempItems.CopyTo(new Span<byte>((node + 16 + 256).ToPointer(), tempItems.Length));
                                }
                            }

                            break;
                        }
                    case NodeType12.Node256:
                        {
                            if (leftNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils12.PtrInNode(node, leftPos), leftNode);
                                leftByte++;
                            }

                            if (rightNode != IntPtr.Zero)
                            {
                                WritePtrInNode(NodeUtils12.PtrInNode(node, rightPos), rightNode);
                                rightByte--;
                            }

                            if (willBeChildCount < header.ChildCount)
                            {
                                for (int i = leftByte; i <= rightByte; i++)
                                {
                                    WritePtrInNode(NodeUtils12.PtrInNode(node, i), IntPtr.Zero);
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
                    switch (header._nodeType & NodeType12.NodeSizeMask)
                    {
                        case NodeType12.Node4:
                        case NodeType12.Node16:
                            {
                                if (leftPos > 0)
                                {
                                    onlyByte = ArtUtils.ReadByte(node + 16);
                                    onlyPtr = NodeUtils12.PtrInNode(node, 0);
                                }
                                else
                                {
                                    onlyByte = ArtUtils.ReadByte(node + 16 + rightPos + 1);
                                    onlyPtr = NodeUtils12.PtrInNode(node, rightPos + 1);
                                }

                                break;
                            }
                        case NodeType12.Node48:
                            {
                                for (var i = 0; i < 256; i++)
                                {
                                    if (i == leftByte)
                                    {
                                        i = rightByte;
                                        continue;
                                    }

                                    var idx = ArtUtils.ReadByte(node + 16 + i);
                                    if (idx == 255) continue;
                                    onlyByte = (byte)i;
                                    onlyPtr = NodeUtils12.PtrInNode(node, idx);
                                    break;
                                }

                                break;
                            }
                        case NodeType12.Node256:
                            {
                                for (int i = 0; i < 256; i++)
                                {
                                    if (i == leftByte)
                                    {
                                        i = rightByte;
                                        continue;
                                    }

                                    if (IsPtr(NodeUtils12.PtrInNode(node, i), out var j))
                                    {
                                        if (j == IntPtr.Zero)
                                            continue;
                                    }

                                    onlyByte = (byte)i;
                                    onlyPtr = NodeUtils12.PtrInNode(node, i);
                                    break;
                                }

                                break;
                            }
                    }

                    if (IsPtr(onlyPtr, out var ptr))
                    {
                        onlyPtr = ptr;
                        NodeUtils12.Reference(onlyPtr);
                    }
                    else
                    {
                        var (prefixSize, prefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(node);
                        var (valueSize, valuePtr) = GetValueSizeAndPtrFromPtrInNode(onlyPtr);
                        var newNode =
                            AllocateNode(NodeType12.NodeLeaf | NodeType12.IsLeaf,
                                prefixSize + 1, valueSize);
                        var (newPrefixSize, newPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
                        ArtUtils.CopyMemory(prefixPtr, newPrefixPtr, (int)prefixSize);
                        ArtUtils.WriteByte(newPrefixPtr + (int)prefixSize, onlyByte);
                        if (valueSize > 0)
                        {
                            var (newValueSize, newValuePtr) = NodeUtils12.GetValueSizeAndPtr(newNode);
                            ArtUtils.CopyMemory(valuePtr, newValuePtr, (int)valueSize);
                        }

                        return (newNode, children);
                    }
                }

                // scope for consistent local variable names
                {
                    var (prefixSize, prefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(node);
                    var newNode = CloneNodeWithKeyPrefixCut(onlyPtr, -(int)(prefixSize + 1));
                    var (newPrefixSize, newPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
                    ArtUtils.CopyMemory(prefixPtr, newPrefixPtr, (int)prefixSize);
                    ArtUtils.WriteByte(newPrefixPtr, (int)prefixSize, onlyByte);
                    Dereference(onlyPtr);
                    return (newNode, children);
                }
            }

            // scope for consistent local variable names
            {
                var (prefixSize, prefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(node);
                var (valueSize, valuePtr) = NodeUtils12.GetValueSizeAndPtr(node);
                var newNode = AllocateNode(newNodeType, prefixSize, valueSize);
                if (prefixSize > 0)
                {
                    var (newPrefixSize, newPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
                    ArtUtils.CopyMemory(prefixPtr, newPrefixPtr, (int)prefixSize);
                }

                if (willBeIsLeaf)
                {
                    var (newValueSize, newValuePtr) = NodeUtils12.GetValueSizeAndPtr(newNode);
                    ArtUtils.CopyMemory(valuePtr, newValuePtr, (int)valueSize);
                }

                if (willBeChildCount == 0)
                {
                    NodeUtils12.Ptr2NodeHeader(newNode)._recursiveChildCount = 1;
                    return (newNode, children);
                }

                Pusher pusher = new Pusher(newNode, newNodeType);
                switch (header._nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                    case NodeType12.Node16:
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

                                pusher.Push(ArtUtils.ReadByte(node + 16 + i), NodeUtils12.PtrInNode(node, i));
                            }

                            break;
                        }
                    case NodeType12.Node48:
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

                                    pusher.Push((byte)i, NodeUtils12.PtrInNode(node, idx));
                                }
                            }

                            break;
                        }
                    case NodeType12.Node256:
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

                                if (IsPtr(NodeUtils12.PtrInNode(node, i), out var j))
                                {
                                    if (j == IntPtr.Zero)
                                        continue;
                                }

                                pusher.Push((byte)i, NodeUtils12.PtrInNode(node, i));
                            }

                            break;
                        }
                }

                ref var newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
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

            public Pusher(IntPtr node, NodeType12 nodeType)
            {
                _idx = 0;
                switch (nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                        {
                            _type = 1;
                            _byteDst = node + 16;
                            _dst = node + 16 + 4;
                            break;
                        }
                    case NodeType12.Node16:
                        {
                            _type = 1;
                            _byteDst = node + 16;
                            _dst = node + 16 + 16;
                            break;
                        }
                    case NodeType12.Node48:
                        {
                            _type = 3;
                            _byteDst = node + 16;
                            _dst = node + 16 + 256;
                            break;
                        }
                    case NodeType12.Node256:
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
                    case 1:
                        {
                            ArtUtils.WriteByte(_byteDst, @byte);
                            _byteDst += 1;
                            ArtUtils.WriteInt32Aligned(_dst, unchecked((int)uint.MaxValue));
                            ArtUtils.WriteIntPtrUnaligned(_dst + 4, ptr);
                            _dst += 12;
                            break;
                        }
                    case 3:
                        {
                            ArtUtils.WriteByte(_byteDst, @byte, (byte)_idx);
                            _idx++;
                            ArtUtils.WriteInt32Aligned(_dst, unchecked((int)uint.MaxValue));
                            ArtUtils.WriteIntPtrUnaligned(_dst + 4, ptr);
                            _dst += 12;
                            break;
                        }
                    case 5:
                        {
                            ArtUtils.WriteInt32Aligned(_dst + 12 * @byte, unchecked((int)uint.MaxValue));
                            ArtUtils.WriteIntPtrUnaligned(_dst + 4 + 12 * @byte, ptr);
                            break;
                        }
                }
            }

            public void Push(byte @byte, IntPtr source)
            {
                switch (_type)
                {
                    case 1:
                        {
                            ArtUtils.WriteByte(_byteDst, @byte);
                            _byteDst += 1;
                            ArtUtils.WriteInt32Aligned(_dst, ArtUtils.ReadInt32Aligned(source));
                            ArtUtils.WriteInt32Aligned(_dst + 4, ArtUtils.ReadInt32Aligned(source + 4));
                            ArtUtils.WriteInt32Aligned(_dst + 8, ArtUtils.ReadInt32Aligned(source + 8));
                            if (NodeUtils12.IsPtr12Ptr(source))
                                NodeUtils12.Reference(NodeUtils12.Read12Ptr(source));
                            _dst += 12;
                            break;
                        }
                    case 3:
                        {
                            ArtUtils.WriteByte(_byteDst, @byte, (byte)_idx);
                            _idx++;
                            ArtUtils.WriteInt32Aligned(_dst, ArtUtils.ReadInt32Aligned(source));
                            ArtUtils.WriteInt32Aligned(_dst + 4, ArtUtils.ReadInt32Aligned(source + 4));
                            ArtUtils.WriteInt32Aligned(_dst + 8, ArtUtils.ReadInt32Aligned(source + 8));
                            if (NodeUtils12.IsPtr12Ptr(source))
                                NodeUtils12.Reference(NodeUtils12.Read12Ptr(source));
                            _dst += 12;
                            break;
                        }
                    case 5:
                        {
                            var ofs = 12 * @byte;
                            ArtUtils.WriteInt32Aligned(_dst + ofs, ArtUtils.ReadInt32Aligned(source));
                            ArtUtils.WriteInt32Aligned(_dst + ofs + 4, ArtUtils.ReadInt32Aligned(source + 4));
                            ArtUtils.WriteInt32Aligned(_dst + ofs + 8, ArtUtils.ReadInt32Aligned(source + 8));
                            if (NodeUtils12.IsPtr12Ptr(source))
                                NodeUtils12.Reference(NodeUtils12.Read12Ptr(source));
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
                ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
                if (header._nodeType.HasFlag(NodeType12.IsLeaf))
                    res++;
                switch (header._nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                        {
                            var ptrInNode = stackItem._node + 16 + 4;
                            var limit = ptrInNode + stackItem._posInNode * 12;
                            for (; ptrInNode != limit; ptrInNode += 12)
                            {
                                if (NodeUtils12.IsPtr12Ptr(ptrInNode))
                                {
                                    res += (long)NodeUtils12.Ptr2NodeHeader(NodeUtils12.Read12Ptr(ptrInNode))
                                        ._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType12.Node16:
                        {
                            var ptrInNode = stackItem._node + 16 + 16;
                            var limit = ptrInNode + stackItem._posInNode * 12;
                            for (; ptrInNode != limit; ptrInNode += 12)
                            {
                                if (NodeUtils12.IsPtr12Ptr(ptrInNode))
                                {
                                    res += (long)NodeUtils12.Ptr2NodeHeader(NodeUtils12.Read12Ptr(ptrInNode))
                                        ._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }
                        break;
                    case NodeType12.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((stackItem._node + 16).ToPointer(), stackItem._byte);
                            for (int j = 0; j < span.Length; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                var ptrInNode = stackItem._node + 16 + 256 + span[j] * 12;
                                if (NodeUtils12.IsPtr12Ptr(ptrInNode))
                                {
                                    res += (long)NodeUtils12.Ptr2NodeHeader(NodeUtils12.Read12Ptr(ptrInNode))
                                        ._recursiveChildCount;
                                }
                                else
                                {
                                    res++;
                                }
                            }
                        }

                        break;
                    case NodeType12.Node256:
                        {
                            var ptrInNode = stackItem._node + 16;
                            var limit = ptrInNode + stackItem._posInNode * 12;
                            for (; ptrInNode != limit; ptrInNode += 12)
                            {
                                if (NodeUtils12.IsPtr12Ptr(ptrInNode))
                                {
                                    var child = NodeUtils12.Read12Ptr(ptrInNode);
                                    if (child != IntPtr.Zero)
                                        res += (long)NodeUtils12.Ptr2NodeHeader(child)._recursiveChildCount;
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
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                if (index >= (long)header._recursiveChildCount)
                    return false;
                keyOffset += NodeUtils12.GetPrefixSize(top);
                if (header._nodeType.HasFlag(NodeType12.IsLeaf))
                {
                    if (index == 0)
                    {
                        stack.Add().Set(top, keyOffset, -1, 0);
                        return true;
                    }

                    index--;
                }

                keyOffset++;
                switch (header._nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                    case NodeType12.Node16:
                        for (int j = 0; j < header._childCount; j++)
                        {
                            if (IsPtr(NodeUtils12.PtrInNode(top, j), out var ptr))
                            {
                                var rcc = (long)NodeUtils12.Ptr2NodeHeader(ptr)._recursiveChildCount;
                                if (index < rcc)
                                {
                                    stack.Add().Set(top, keyOffset, (short)j, ArtUtils.ReadByte(top + 16 + j));
                                    top = ptr;
                                    break;
                                }

                                index -= rcc;
                            }
                            else
                            {
                                if (index == 0)
                                {
                                    stack.Add().Set(top, keyOffset, (short)j, ArtUtils.ReadByte(top + 16 + j));
                                    return true;
                                }

                                index--;
                            }
                        }

                        break;
                    case NodeType12.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 0; j < span.Length; j++)
                            {
                                if (span[j] == 255)
                                    continue;
                                if (IsPtr(NodeUtils12.PtrInNode(top, span[j]), out var ptr))
                                {
                                    var rcc = (long)NodeUtils12.Ptr2NodeHeader(ptr)._recursiveChildCount;
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
                    case NodeType12.Node256:
                        for (int j = 0; j < 256; j++)
                        {
                            if (IsPtr(NodeUtils12.PtrInNode(top, j), out var ptr))
                            {
                                if (ptr == IntPtr.Zero)
                                    continue;
                                var rcc = (long)NodeUtils12.Ptr2NodeHeader(ptr)._recursiveChildCount;
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
                ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
                if (stackItem._posInNode == -1) stackItem._keyOffset++;
                switch (header._nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.NodeLeaf:
                        goto up;
                    case NodeType12.Node4:
                    case NodeType12.Node16:
                        {
                            if (stackItem._posInNode == header._childCount - 1)
                            {
                                goto up;
                            }

                            stackItem._posInNode++;
                            stackItem._byte = ArtUtils.ReadByte(stackItem._node + 16 + stackItem._posInNode);
                            goto down;
                        }
                    case NodeType12.Node48:
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
                    case NodeType12.Node256:
                        for (int j = (stackItem._posInNode == -1) ? 0 : (stackItem._byte + 1); j < 256; j++)
                        {
                            if (IsPtr(NodeUtils12.PtrInNode(stackItem._node, j), out var ptr2))
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
                if (IsPtr(NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode), out var ptr))
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
                ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
                if (stackItem._posInNode == -1)
                {
                    goto trullyUp;
                }

                switch (header._nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                    case NodeType12.Node16:
                        {
                            if (stackItem._posInNode == 0)
                            {
                                goto up;
                            }

                            stackItem._posInNode--;
                            stackItem._byte = ArtUtils.ReadByte(stackItem._node + 16 + stackItem._posInNode);
                            goto down;
                        }
                    case NodeType12.Node48:
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
                    case NodeType12.Node256:
                        for (int j = stackItem._byte - 1; j >= 0; j--)
                        {
                            if (IsPtr(NodeUtils12.PtrInNode(stackItem._node, j), out var ptr2))
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
                if (IsPtr(NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode), out var ptr))
                {
                    PushRightMost(ptr, (int)stackItem._keyOffset, ref stack);
                }

                return true;
            up:
                if (header._nodeType.HasFlag(NodeType12.IsLeaf))
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

        internal unsafe IntPtr CloneNode(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils12.BaseSize(header._nodeType).Base;
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                prefixSize = *(uint*)ptr;
                ptr += sizeof(uint);
            }

            if (header._nodeType.HasFlag(NodeType12.IsLeaf))
            {
                ptr += (int)prefixSize;
                ptr = ArtUtils.AlignPtrUpInt32(ptr);
                ptr += 12;
            }
            else
            {
                ptr += (int)prefixSize;
            }

            var size = (IntPtr)(ptr.ToInt64() - nodePtr.ToInt64());
            var newNode = _allocator.Allocate(size);
            System.Buffer.MemoryCopy(nodePtr.ToPointer(), newNode.ToPointer(), size.ToInt64(), size.ToInt64());

            ref NodeHeader12 newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
            newHeader._referenceCount = 1;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        unsafe IntPtr ExpandNode(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(nodePtr);
            var (valueSize, valuePtr) = NodeUtils12.GetValueSizeAndPtr(nodePtr);
            var newNodeType = header._nodeType + 1;
            var newNode = AllocateNode(newNodeType, keyPrefixSize, valueSize);
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
            if (newNodeType.HasFlag(NodeType12.IsLeaf))
            {
                var (newValueSize, newValuePtr) = NodeUtils12.GetValueSizeAndPtr(newNode);
                ArtUtils.CopyMemory(valuePtr, newValuePtr, (int)valueSize);
            }

            ArtUtils.CopyMemory(keyPrefixPtr, newKeyPrefixPtr, (int)keyPrefixSize);
            ref NodeHeader12 newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
            newHeader._childCount = header._childCount;
            newHeader._recursiveChildCount = header._recursiveChildCount;
            switch (newNodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.Node16:
                    {
                        ArtUtils.CopyMemory(nodePtr + 16, newNode + 16, 4);
                        ArtUtils.CopyMemory(NodeUtils12.PtrInNode(nodePtr, 0), NodeUtils12.PtrInNode(newNode, 0), 4 * PtrSize);
                        break;
                    }
                case NodeType12.Node48:
                    {
                        var srcBytesPtr = (byte*)(nodePtr + 16).ToPointer();
                        var dstBytesPtr = (byte*)(newNode + 16).ToPointer();
                        for (var i = 0; i < 16; i++)
                        {
                            dstBytesPtr[srcBytesPtr[i]] = (byte)i;
                        }

                        ArtUtils.CopyMemory(NodeUtils12.PtrInNode(nodePtr, 0), NodeUtils12.PtrInNode(newNode, 0), 16 * PtrSize);
                        break;
                    }
                case NodeType12.Node256:
                    {
                        var srcBytesPtr = (byte*)(nodePtr + 16).ToPointer();
                        for (var i = 0; i < 256; i++)
                        {
                            var pos = srcBytesPtr[i];
                            if (pos == 255) continue;
                            ArtUtils.CopyMemory(NodeUtils12.PtrInNode(nodePtr, pos), NodeUtils12.PtrInNode(newNode, i), PtrSize);
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
            ref NodeHeader12 header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils12.BaseSize(header._nodeType).Base;
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(nodePtr);
            var (valueSize, valuePtr) = NodeUtils12.GetValueSizeAndPtr(nodePtr);
            var newNode = AllocateNode(header._nodeType, (uint)(keyPrefixSize - skipPrefix), valueSize);
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
            var (newValueSize, newValuePtr) = NodeUtils12.GetValueSizeAndPtr(newNode);
            ref NodeHeader12 newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
            var backupNewKeyPrefix = newHeader._keyPrefixLength;
            unsafe
            {
                new Span<byte>(nodePtr.ToPointer(), baseSize).CopyTo(new Span<byte>(newNode.ToPointer(), baseSize));
                if (skipPrefix < 0)
                {
                    new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).CopyTo(
                        new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize).Slice(-skipPrefix));
                }
                else
                {
                    new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).Slice(skipPrefix)
                        .CopyTo(new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize));
                }

                if (header._nodeType.HasFlag(NodeType12.IsLeaf))
                {
                    new Span<byte>(valuePtr.ToPointer(), (int)valueSize).CopyTo(new Span<byte>(newValuePtr.ToPointer(),
                        (int)newValueSize));
                }
            }

            newHeader._referenceCount = 1;
            newHeader._keyPrefixLength = backupNewKeyPrefix;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        IntPtr CloneNodeWithValueResize(IntPtr nodePtr, int length)
        {
            ref NodeHeader12 header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            var baseSize = NodeUtils12.BaseSize(header._nodeType).Base;
            var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(nodePtr);
            var newNodeType = header._nodeType;
            if (length < 0)
            {
                newNodeType = newNodeType & (~NodeType12.IsLeaf);
            }
            else
            {
                newNodeType = newNodeType | NodeType12.IsLeaf;
            }

            var newNode = AllocateNode(newNodeType, keyPrefixSize, (uint)(length < 0 ? 0 : length));
            var (newKeyPrefixSize, newKeyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
            unsafe
            {
                new Span<byte>(nodePtr.ToPointer(), baseSize).CopyTo(new Span<byte>(newNode.ToPointer(), baseSize));
                new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).CopyTo(
                    new Span<byte>(newKeyPrefixPtr.ToPointer(), (int)newKeyPrefixSize));
            }

            ref NodeHeader12 newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
            newHeader._nodeType = newNodeType;
            newHeader._referenceCount = 1;
            ReferenceAllChildren(newNode);
            return newNode;
        }

        void ReferenceAllChildren(IntPtr node)
        {
            ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeader(node);
            switch (nodeHeader._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf:
                    // does not contain any pointers
                    break;
                case NodeType12.Node4:
                    {
                        var p = node + 16 + 4;
                        var limit = p + nodeHeader._childCount * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                NodeUtils12.Reference(NodeUtils12.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType12.Node16:
                    {
                        var p = node + 16 + 16;
                        var limit = p + nodeHeader._childCount * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                NodeUtils12.Reference(NodeUtils12.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType12.Node48:
                    {
                        var p = node + 16 + 256;
                        var limit = p + nodeHeader._childCount * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                NodeUtils12.Reference(NodeUtils12.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType12.Node256:
                    {
                        var p = node + 16;
                        var limit = p + 256 * 12;
                        for (; p != limit; p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                NodeUtils12.Reference(NodeUtils12.Read12Ptr(p));
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
            ref var nodeHeader = ref NodeUtils12.Ptr2NodeHeader(node);
            if (!nodeHeader.Dereference()) return;
            switch (nodeHeader._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf:
                    // does not contain any pointers
                    break;
                case NodeType12.Node4:
                    {
                        var p = node + 16 + 4;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils12.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType12.Node16:
                    {
                        var p = node + 16 + 16;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils12.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType12.Node48:
                    {
                        var p = node + 16 + 256;
                        for (var i = 0; i < nodeHeader._childCount; i++, p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils12.Read12Ptr(p));
                            }
                        }
                    }
                    break;
                case NodeType12.Node256:
                    {
                        var p = node + 16;
                        for (var i = 0; i < 256; i++, p += 12)
                        {
                            if (NodeUtils12.IsPtr12Ptr(p))
                            {
                                Dereference(NodeUtils12.Read12Ptr(p));
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

        internal void WriteValue(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> content)
        {
            CheckContent12(content);
            MakeUnique(rootNode, stack.AsSpan());
            ref var stackItem = ref stack[stack.Count - 1];
            if (stackItem._posInNode == -1)
            {
                var ptr = NodeUtils12.GetValueSizeAndPtr(stackItem._node).Ptr;
                unsafe
                {
                    content.CopyTo(new Span<byte>(ptr.ToPointer(), 12));
                }
            }
            else
            {
                var ptr = NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode);
                unsafe
                {
                    content.CopyTo(new Span<byte>(ptr.ToPointer(), 12));
                }
            }
        }

        void MakeUniqueLastResize(RootNode12 rootNode, ref StructList<CursorItem> stack, int length)
        {
            for (int i = 0; i < stack.Count; i++)
            {
                ref var stackItem = ref stack[(uint)i];
                ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
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

        void MakeUniqueAndOverwriteIfNeeded(RootNode12 rootNode, in Span<CursorItem> stack, IntPtr newNode)
        {
            if (stack.Length == 0)
            {
                Dereference(rootNode._root);
                rootNode._root = newNode;
            }
            else
            {
                ref var stackItem = ref stack[stack.Length - 1];
                var ptr = NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode);
                if (IsPtr(ptr, out var ptrTo) && ptrTo == newNode)
                    return;
                MakeUnique(rootNode, stack);
                ptr = NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode);
                WritePtrInNode(ptr, newNode);
            }
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

        void WritePtrAndByteInNode(in CursorItem stackItem, IntPtr newNode)
        {
            WritePtrInNode(stackItem, newNode);
            var nodeType = NodeUtils12.Ptr2NodeHeader(stackItem._node)._nodeType & NodeType12.NodeSizeMask;
            if (nodeType != NodeType12.Node256)
                ArtUtils.WriteByte(stackItem._node, 16 + stackItem._posInNode, stackItem._byte);
        }

        void WritePtrInNode(in CursorItem stackItem, IntPtr newNode)
        {
            var ptr = NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode);
            WritePtrInNode(ptr, newNode);
        }

        unsafe void WritePtrInNode(IntPtr ptrInNode, IntPtr newNode)
        {
            if (NodeUtils12.IsPtr12Ptr(ptrInNode))
            {
                Dereference(NodeUtils12.Read12Ptr(ptrInNode));
            }

            Unsafe.Write(ptrInNode.ToPointer(), uint.MaxValue);
            ArtUtils.WriteIntPtrUnaligned(ptrInNode + 4, newNode);
        }

        void WriteContentAndByteInNode(in CursorItem stackItem, ReadOnlySpan<byte> content)
        {
            WriteContentInNode(stackItem, content);
            var nodeType = NodeUtils12.Ptr2NodeHeader(stackItem._node)._nodeType & NodeType12.NodeSizeMask;
            if (nodeType != NodeType12.Node256)
                ArtUtils.WriteByte(stackItem._node, 16 + stackItem._posInNode, stackItem._byte);
        }

        void WriteContentInNode(in CursorItem stackItem, ReadOnlySpan<byte> content)
        {
            var ptr = NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode);
            unsafe
            {
                if (NodeUtils12.IsPtr12Ptr(ptr))
                {
                    Dereference(NodeUtils12.Read12Ptr(ptr));
                }

                unsafe
                {
                    content.CopyTo(new Span<byte>(ptr.ToPointer(), 12));
                }
            }
        }

        unsafe void InitializeZeroPtrValue(IntPtr ptr)
        {
            var v = new Span<uint>(ptr.ToPointer(), 3);
            v[0] = uint.MaxValue;
            v[1] = 0;
            v[2] = 0;
        }

        internal bool FindExact(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> key)
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

                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0
                    ? 0
                    : FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (newKeyPrefixSize < keyPrefixSize)
                {
                    stack.Clear();
                    return false;
                }

                if (keyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType12.IsLeaf))
                    {
                        stack.Clear();
                        return false;
                    }

                    stack.Add().Set(top, (uint)key.Length, -1, 0);
                    return true;
                }

                if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
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
                    if (IsPtr(NodeUtils12.PtrInNode(top, pos), out var newTop))
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

        internal FindResult Find(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix,
            ReadOnlySpan<byte> key)
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
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(top);

                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var diffPos = commonKeyAndPrefixSize == 0
                    ? 0
                    : FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (diffPos < keyPrefixSize)
                {
                    if (keyOffset + diffPos >= keyPrefix.Length)
                    {
                        PushLeftMost(top, keyOffset, ref stack);
                        if (diffPos >= keyRest) return FindResult.Next;
                        return ArtUtils.ReadByte(keyPrefixPtr + diffPos) <
                               GetByteFromKeyPair(keyPrefix, key, keyOffset + diffPos)
                            ? FindResult.Previous
                            : FindResult.Next;
                    }

                    stack.Clear();
                    return FindResult.NotFound;
                }

                if (keyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType12.IsLeaf))
                    {
                        PushLeftMost(top, keyOffset, ref stack);
                        return FindResult.Next;
                    }

                    stack.Add().Set(top, (uint)key.Length, -1, 0);
                    return FindResult.Exact;
                }

                if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
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
                    if (IsPtr(NodeUtils12.PtrInNode(top, pos), out var newTop))
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
                    if (IsPtr(NodeUtils12.PtrInNode(top, nearPos), out var newTop))
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
            ref var header = ref NodeUtils12.Ptr2NodeHeader(node);
            switch (header._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.Node4:
                case NodeType12.Node16:
                    {
                        pos = ~pos;
                        if (pos >= header._childCount) pos--;
                        return ((short)pos, ArtUtils.ReadByte(node + 16 + pos));
                    }
                case NodeType12.Node48:
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
                case NodeType12.Node256:
                    {
                        pos = @byte + 1;
                        while (pos < 256)
                        {
                            if (IsPtr(NodeUtils12.PtrInNode(node, pos), out var ptr))
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
                            if (IsPtr(NodeUtils12.PtrInNode(node, pos), out var ptr))
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

        internal bool FindFirst(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
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

                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0
                    ? 0
                    : FindFirstDifference(keyPrefix.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (newKeyPrefixSize < keyPrefixSize && newKeyPrefixSize < keyRest)
                {
                    stack.Clear();
                    return false;
                }

                if (newKeyPrefixSize == keyRest)
                {
                    if (!header._nodeType.HasFlag(NodeType12.IsLeaf))
                    {
                        PushLeftMost(top, keyOffset, ref stack);
                        return true;
                    }

                    stack.Add().Set(top, (uint)keyOffset + keyPrefixSize, -1, 0);
                    return true;
                }

                if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
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
                    if (IsPtr(NodeUtils12.PtrInNode(top, pos), out var newTop))
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

        internal bool FindLast(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> keyPrefix)
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

                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0
                    ? 0
                    : FindFirstDifference(keyPrefix.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
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

                if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
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
                    if (IsPtr(NodeUtils12.PtrInNode(top, pos), out var newTop))
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
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                keyOffset += (int)NodeUtils12.GetPrefixSize(top);
                if (header._nodeType.HasFlag(NodeType12.IsLeaf))
                {
                    stack.Add().Set(top, (uint)keyOffset, -1, 0);
                    return;
                }

                keyOffset++;
                switch (header._nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                        {
                            stack.Add().Set(top, (uint)keyOffset, 0, ArtUtils.ReadByte(top + 16));
                            var ptr = top + 16 + 4;
                            if (NodeUtils12.IsPtr12Ptr(ptr))
                            {
                                top = NodeUtils12.Read12Ptr(ptr);
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType12.Node16:
                        {
                            stack.Add().Set(top, (uint)keyOffset, 0, ArtUtils.ReadByte(top + 16));
                            var ptr = top + 16 + 16;
                            if (NodeUtils12.IsPtr12Ptr(ptr))
                            {
                                top = NodeUtils12.Read12Ptr(ptr);
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType12.Node48:
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
                                if (NodeUtils12.IsPtr12Ptr(ptr))
                                {
                                    top = NodeUtils12.Read12Ptr(ptr);
                                    break;
                                }

                                return;
                            }

                            break;
                        }
                    case NodeType12.Node256:
                        {
                            var p = top + 16;
                            for (var j = 0; true; j++, p += 12)
                            {
                                if (NodeUtils12.IsPtr12Ptr(p))
                                {
                                    var child = NodeUtils12.Read12Ptr(p);
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
                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                keyOffset += (int)NodeUtils12.GetPrefixSize(top);
                if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
                {
                    stack.Add().Set(top, (uint)keyOffset, -1, 0);
                    return;
                }

                keyOffset++;
                switch (header._nodeType & NodeType12.NodeSizeMask)
                {
                    case NodeType12.Node4:
                    case NodeType12.Node16:
                        {
                            var pos = header._childCount - 1;
                            stack.Add().Set(top, (uint)keyOffset, (short)pos, ArtUtils.ReadByte(top + 16 + pos));
                            if (IsPtr(NodeUtils12.PtrInNode(top, pos), out var ptr))
                            {
                                top = ptr;
                                break;
                            }
                            else
                            {
                                return;
                            }
                        }
                    case NodeType12.Node48:
                        unsafe
                        {
                            var span = new Span<byte>((top + 16).ToPointer(), 256);
                            for (int j = 255; true; j--)
                            {
                                if (span[j] == 255)
                                    continue;
                                stack.Add().Set(top, (uint)keyOffset, span[j], (byte)j);
                                if (IsPtr(NodeUtils12.PtrInNode(top, span[j]), out var ptr))
                                {
                                    top = ptr;
                                    break;
                                }

                                return;
                            }

                            break;
                        }
                    case NodeType12.Node256:
                        for (int j = 255; true; j--)
                        {
                            if (IsPtr(NodeUtils12.PtrInNode(top, j), out var ptr))
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

        internal bool Upsert(RootNode12 rootNode, ref StructList<CursorItem> stack, ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> content)
        {
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
                    if (keyRest == 0 && stack.Count > 0)
                    {
                        WriteContentInNode(stack[stack.Count - 1], content);
                        AdjustRecursiveChildCount(stack.AsSpan(), +1);
                        return true;
                    }

                    ref var stackItem = ref stack.Add();
                    stackItem.Set(
                        AllocateNode(NodeType12.NodeLeaf | NodeType12.IsLeaf, (uint)keyRest, (uint)content.Length),
                        (uint)key.Length, -1, 0);
                    var (size, ptr) = NodeUtils12.GetPrefixSizeAndPtr(stackItem._node);
                    unsafe
                    {
                        key.Slice(keyOffset).CopyTo(new Span<byte>(ptr.ToPointer(), (int)size));
                    }

                    (size, ptr) = NodeUtils12.GetValueSizeAndPtr(stackItem._node);
                    unsafe
                    {
                        content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size));
                    }

                    OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, stackItem._node);
                    AdjustRecursiveChildCount(stack.AsSpan(0, (int)stack.Count - 1), +1);
                    return true;
                }

                ref var header = ref NodeUtils12.Ptr2NodeHeader(top);
                var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(top);
                var commonKeyAndPrefixSize = Math.Min(keyRest, (int)keyPrefixSize);
                var newKeyPrefixSize = commonKeyAndPrefixSize == 0
                    ? 0
                    : FindFirstDifference(key.Slice(keyOffset), keyPrefixPtr, commonKeyAndPrefixSize);
                if (newKeyPrefixSize < keyPrefixSize)
                {
                    MakeUnique(rootNode, stack.AsSpan());
                    var nodeType = NodeType12.Node4 | (newKeyPrefixSize == keyRest ? NodeType12.IsLeaf : 0);
                    var newNode = AllocateNode(nodeType, (uint)newKeyPrefixSize, (uint)content.Length);
                    try
                    {
                        ref var newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = header._recursiveChildCount;
                        var (size, ptr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
                        unsafe
                        {
                            key.Slice(keyOffset, newKeyPrefixSize)
                                .CopyTo(new Span<byte>(ptr.ToPointer(), newKeyPrefixSize));
                        }

                        if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf &&
                            newKeyPrefixSize + 1 == keyPrefixSize)
                        {
                            var (valueSize, valuePtr) = NodeUtils12.GetValueSizeAndPtr(top);
                            unsafe
                            {
                                WriteContentAndByteInNode(
                                    new CursorItem(newNode, 0, 0, ArtUtils.ReadByte(keyPrefixPtr + newKeyPrefixSize)),
                                    new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                            }
                        }
                        else
                        {
                            var newNode2 = CloneNodeWithKeyPrefixCut(top, newKeyPrefixSize + 1);
                            WritePtrAndByteInNode(
                                new CursorItem(newNode, 0, 0, ArtUtils.ReadByte(keyPrefixPtr + newKeyPrefixSize)),
                                newNode2);
                        }

                        if (nodeType.HasFlag(NodeType12.IsLeaf))
                        {
                            stack.Add().Set(newNode, (uint)key.Length, -1, 0);
                            (size, ptr) = NodeUtils12.GetValueSizeAndPtr(newNode);
                            unsafe
                            {
                                content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size));
                            }

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
                    var hadIsLeaf = header._nodeType.HasFlag(NodeType12.IsLeaf);
                    if (header._nodeType.HasFlag(NodeType12.IsLeaf))
                    {
                        MakeUnique(rootNode, stack.AsSpan());
                    }
                    else
                    {
                        MakeUniqueLastResize(rootNode, ref stack, content.Length);
                    }

                    var (size, ptr) = NodeUtils12.GetValueSizeAndPtr(stack[stack.Count - 1]._node);
                    unsafe
                    {
                        content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size));
                    }

                    if (!hadIsLeaf)
                    {
                        AdjustRecursiveChildCount(stack.AsSpan(), +1);
                        return true;
                    }

                    return false;
                }

                var b = key[keyOffset + newKeyPrefixSize];
                if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
                {
                    MakeUnique(rootNode, stack.AsSpan());
                    var nodeType = NodeType12.Node4 | NodeType12.IsLeaf;
                    var (topValueSize, topValuePtr) = NodeUtils12.GetValueSizeAndPtr(top);
                    var newNode = AllocateNode(nodeType, (uint)newKeyPrefixSize, topValueSize);
                    try
                    {
                        ref var newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = 1;
                        var (size, ptr) = NodeUtils12.GetPrefixSizeAndPtr(newNode);
                        unsafe
                        {
                            key.Slice(keyOffset, newKeyPrefixSize)
                                .CopyTo(new Span<byte>(ptr.ToPointer(), newKeyPrefixSize));
                        }

                        var (valueSize, valuePtr) = NodeUtils12.GetValueSizeAndPtr(newNode);
                        unsafe
                        {
                            new Span<byte>(topValuePtr.ToPointer(), (int)topValueSize).CopyTo(
                                new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
                        }

                        keyOffset += newKeyPrefixSize + 1;
                        ArtUtils.WriteByte(newNode, 16, b);
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
                    if (IsPtr(NodeUtils12.PtrInNode(top, pos), out var newTop))
                    {
                        if (key.Length == keyOffset && IsValueInlineable(content) &&
                            (NodeUtils12.Ptr2NodeHeader(newTop)._nodeType & NodeType12.NodeSizeMask) == NodeType12.NodeLeaf)
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
                        if (IsValueInlineable(content))
                        {
                            WriteContentInNode(stack[stack.Count - 1], content);
                        }
                        else
                        {
                            ref var stackItem = ref stack.Add();
                            stackItem.Set(AllocateNode(NodeType12.NodeLeaf | NodeType12.IsLeaf, 0, (uint)content.Length),
                                (uint)key.Length, -1, 0);
                            var (size, ptr) = NodeUtils12.GetValueSizeAndPtr(stackItem._node);
                            unsafe
                            {
                                content.CopyTo(new Span<byte>(ptr.ToPointer(), (int)size));
                            }

                            OverwriteNodePtrInStack(rootNode, stack.AsSpan(), (int)stack.Count - 1, stackItem._node);
                        }

                        return false;
                    }

                    var nodeType = NodeType12.Node4 | NodeType12.IsLeaf;
                    var (topValueSize, topValuePtr) = GetValueSizeAndPtrFromPtrInNode(NodeUtils12.PtrInNode(top, pos));
                    var newNode = AllocateNode(nodeType, 0, topValueSize);
                    try
                    {
                        ref var newHeader = ref NodeUtils12.Ptr2NodeHeader(newNode);
                        newHeader.ChildCount = 1;
                        newHeader._recursiveChildCount = 1;
                        var (valueSize, valuePtr) = NodeUtils12.GetValueSizeAndPtr(newNode);
                        unsafe
                        {
                            new Span<byte>(topValuePtr.ToPointer(), (int)topValueSize).CopyTo(
                                new Span<byte>(valuePtr.ToPointer(), (int)valueSize));
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
            return (12, ptr);
        }

        bool IsValueInlineable(ReadOnlySpan<byte> content)
        {
            return true;
        }

        bool IsPtr12(IntPtr ptr, out IntPtr pointsTo)
        {
            if (NodeUtils12.IsPtr12Ptr(ptr))
            {
                pointsTo = NodeUtils12.Read12Ptr(ptr);
                return true;
            }

            pointsTo = IntPtr.Zero;
            return false;
        }

        bool IsPtr(IntPtr ptr, out IntPtr pointsTo)
        {
            if (NodeUtils12.IsPtr12Ptr(ptr))
            {
                pointsTo = NodeUtils12.Read12Ptr(ptr);
                return true;
            }

            pointsTo = IntPtr.Zero;
            return false;
        }

        unsafe int Find(IntPtr nodePtr, byte b)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.Node256)
            {
                var ptr = NodeUtils12.PtrInNode(nodePtr, b);
                if (NodeUtils12.IsPtr12Ptr(ptr) && NodeUtils12.Read12Ptr(ptr) == IntPtr.Zero)
                    return ~b;

                return b;
            }

            if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.Node48)
            {
                var pos = ArtUtils.ReadByte(nodePtr + 16 + b);
                if (pos == 255)
                    return ~header._childCount;
                return pos;
            }
            else
            {
                var childrenBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
                return BinarySearch(childrenBytes, b);
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
            ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            var childernBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
            var pos = BinarySearch(childernBytes, b);
            pos = ~pos;
            if (pos < childernBytes.Length)
            {
                childernBytes.Slice(pos)
                    .CopyTo(new Span<byte>((nodePtr + 16).ToPointer(), header._childCount + 1).Slice(pos + 1));
                var chPtr = NodeUtils12.PtrInNode(nodePtr, pos);
                var chSize = PtrSize * (header._childCount - pos);
                new Span<byte>(chPtr.ToPointer(), chSize).CopyTo(new Span<byte>((chPtr + PtrSize).ToPointer(), chSize));
            }

            ArtUtils.WriteByte(nodePtr, 16 + pos, b);
            header._childCount++;
            InitializeZeroPtrValue(NodeUtils12.PtrInNode(nodePtr, pos));
            return (short)pos;
        }

        unsafe void InsertChildRaw(IntPtr nodePtr, ref int pos, byte b)
        {
            ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.Node256)
            {
                pos = b;
                header._childCount++;
                InitializeZeroPtrValue(NodeUtils12.PtrInNode(nodePtr, pos));
                return;
            }

            if ((header._nodeType & NodeType12.NodeSizeMask) == NodeType12.Node48)
            {
                pos = header._childCount;
                ArtUtils.WriteByte(nodePtr, 16 + b, (byte)pos);
            }
            else
            {
                var childrenBytes = new ReadOnlySpan<byte>((nodePtr + 16).ToPointer(), header._childCount);
                if (pos < childrenBytes.Length)
                {
                    childrenBytes.Slice(pos)
                        .CopyTo(new Span<byte>((nodePtr + 16).ToPointer(), header._childCount + 1).Slice(pos + 1));
                    var chPtr = NodeUtils12.PtrInNode(nodePtr, pos);
                    var chSize = PtrSize * (header._childCount - pos);
                    ArtUtils.MoveMemory(chPtr, chPtr + PtrSize, chSize);
                }

                ArtUtils.WriteByte(nodePtr, 16 + pos, b);
            }

            header._childCount++;
            InitializeZeroPtrValue(NodeUtils12.PtrInNode(nodePtr, pos));
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

        public ulong StructureCheck(IntPtr nodePtr)
        {
            if (nodePtr == IntPtr.Zero)
                return 0;
            ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            Debug.Assert(header._referenceCount > 0 && header._referenceCount < 2000);
            var childrenCount = 0ul;
            if (header._nodeType.HasFlag(NodeType12.IsLeaf))
                childrenCount++;
            switch (header._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf:
                    childrenCount = 1;
                    break;
                case NodeType12.Node4:
                case NodeType12.Node16:
                    {
                        for (var pos = 0; pos < header._childCount; pos++)
                        {
                            var b = ArtUtils.ReadByte(nodePtr + 16 + pos);
                            if (IsPtr(NodeUtils12.PtrInNode(nodePtr, pos), out var ptr))
                            {
                                Debug.Assert(ptr != IntPtr.Zero);
                                childrenCount += StructureCheck(ptr);
                            }
                            else
                            {
                                childrenCount++;
                            }
                        }

                        break;
                    }
                case NodeType12.Node48:
                    unsafe
                    {
                        var span = new Span<byte>((nodePtr + 16).ToPointer(), 256);
                        StructureCheck48(span, header.ChildCount);
                        for (int j = 0; j < 256; j++)
                        {
                            if (span[j] == 255)
                                continue;
                            if (IsPtr(NodeUtils12.PtrInNode(nodePtr, span[j]), out var ptr))
                            {
                                Debug.Assert(ptr != IntPtr.Zero);
                                childrenCount += StructureCheck(ptr);
                            }
                            else
                            {
                                childrenCount++;
                            }
                        }

                        break;
                    }
                case NodeType12.Node256:
                    for (int j = 0; j < 256; j++)
                    {
                        if (IsPtr(NodeUtils12.PtrInNode(nodePtr, j), out var ptr))
                        {
                            childrenCount += StructureCheck(ptr);
                        }
                        else
                        {
                            childrenCount++;
                        }
                    }

                    break;
            }

            Debug.Assert(header._recursiveChildCount == childrenCount);
            return childrenCount;
        }

        static void StructureCheck48(in Span<byte> ptr, int childCount)
        {
            Span<byte> cnts = stackalloc byte[48];
            for (var i = 0; i < 256; i++)
            {
                var p = ptr[i];
                if (p == 255) continue;
                cnts[p]++;
            }

            for (var i = 0; i < 48; i++)
            {
                Debug.Assert(cnts[i] == (i < childCount ? 1 : 0));
            }
        }

        internal void IterateNodeInfo(IntPtr nodePtr, uint deepness, Action<ArtNodeInfo> iterator)
        {
            if (nodePtr == IntPtr.Zero)
                return;
            ref var header = ref NodeUtils12.Ptr2NodeHeader(nodePtr);
            var nodeInfo = new ArtNodeInfo();
            nodeInfo.Deepness = deepness;
            nodeInfo.ChildCount = (uint)header.ChildCount;
            nodeInfo.RecursiveChildCount = header._recursiveChildCount;
            nodeInfo.HasLeafChild = header._nodeType.HasFlag(NodeType12.IsLeaf);
            var baseSize = NodeUtils12.BaseSize(header._nodeType).Base;
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                unsafe
                {
                    prefixSize = *(uint*)ptr;
                }
                ptr += sizeof(uint);
            }
            nodeInfo.PrefixKeySize = prefixSize;

            if (header._nodeType.HasFlag(NodeType12.IsLeaf))
            {
                ptr += (int)prefixSize;
                ptr = ArtUtils.AlignPtrUpInt32(ptr);
                ptr += 12;
            }
            else
            {
                ptr += (int)prefixSize;
            }

            var size = ptr.ToInt64() - nodePtr.ToInt64();
            nodeInfo.NodeByteSize = (uint)size;
            nodeInfo.MaxChildCount = (uint)NodeUtils12.MaxChildren(header._nodeType);
            iterator(nodeInfo);
            switch (header._nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf:
                    break;
                case NodeType12.Node4:
                case NodeType12.Node16:
                    {
                        for (var pos = 0; pos < header._childCount; pos++)
                        {
                            var b = ArtUtils.ReadByte(nodePtr + 16 + pos);
                            if (IsPtr(NodeUtils12.PtrInNode(nodePtr, pos), out ptr))
                            {
                                Debug.Assert(ptr != IntPtr.Zero);
                                IterateNodeInfo(ptr, deepness + 1, iterator);
                            }
                        }
                        break;
                    }
                case NodeType12.Node48:
                    unsafe
                    {
                        var span = new Span<byte>((nodePtr + 16).ToPointer(), 256);
                        for (int j = 0; j < 256; j++)
                        {
                            if (span[j] == 255)
                                continue;
                            if (IsPtr(NodeUtils12.PtrInNode(nodePtr, span[j]), out ptr))
                            {
                                Debug.Assert(ptr != IntPtr.Zero);
                                IterateNodeInfo(ptr, deepness + 1, iterator);
                            }
                        }
                        break;
                    }
                case NodeType12.Node256:
                    for (int j = 0; j < 256; j++)
                    {
                        if (IsPtr(NodeUtils12.PtrInNode(nodePtr, j), out ptr))
                        {
                            IterateNodeInfo(ptr, deepness + 1, iterator);
                        }
                    }
                    break;
            }
        }
    }
}
