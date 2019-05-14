using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BTDB.ARTLib
{
    static class NodeUtils12
    {
        internal static (int Base, int MaxChildren) BaseSize(NodeType12 nodeType)
        {
            switch (nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf: return (16, 0);
                case NodeType12.Node4: return (16 + 4 + 4 * 12, 4);
                case NodeType12.Node16: return (16 + 16 + 16 * 12, 16);
                case NodeType12.Node48: return (16 + 256 + 48 * 12, 48);
                case NodeType12.Node256: return (16 + 256 * 12, 256);
                default: throw new InvalidOperationException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr Read12Ptr(IntPtr childPtr)
        {
            return ArtUtils.ReadIntPtrUnaligned(childPtr + sizeof(uint));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsPtr12Ptr(IntPtr childPtr)
        {
            unsafe
            {
                return *(uint*)childPtr == uint.MaxValue;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref NodeHeader12 Ptr2NodeHeader(IntPtr pointerInt)
        {
            return ref *(NodeHeader12*)pointerInt;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Reference(IntPtr node)
        {
            if (node == IntPtr.Zero)
                return;
            ref var nodeHeader = ref Ptr2NodeHeader(node);
            nodeHeader.Reference();
        }

        internal static unsafe (uint Size, IntPtr Ptr) GetPrefixSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var size = (uint)header._keyPrefixLength;
            if (size == 0) return (0, IntPtr.Zero);
            var (baseSize, maxChildren) = BaseSize(header._nodeType);
            var ptr = nodePtr + baseSize;
            if (size == 0xffff)
            {
                size = (uint)ArtUtils.ReadInt32Aligned(ptr);
                ptr += sizeof(uint);
            }
            if (header._nodeType.HasFlag(NodeType12.HasSuffixes))
            {
                ptr += 2 * maxChildren;
                ptr += 2 + *(ushort*)ptr;
            }
            return (size, ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint GetPrefixSize(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var size = (uint)header._keyPrefixLength;
            if (size == 0xffff)
            {
                var (baseSize, _) = BaseSize(header._nodeType);
                var ptr = nodePtr + baseSize;
                size = (uint)ArtUtils.ReadInt32Aligned(ptr);
            }
            return size;
        }

        internal static unsafe (uint Size, IntPtr Ptr) GetSuffixSizeAndPtr(IntPtr suffixPtr, int i, int maxChildren)
        {
            if (suffixPtr == IntPtr.Zero)
            {
                return (0, IntPtr.Zero);
            }
            var ptr = suffixPtr;
            var suffixOfs = *(ushort*)(ptr + i * 2);
            var suffixSize = (uint)(*(ushort*)(ptr + i * 2 + 2) - suffixOfs);
            ptr += 2 * maxChildren + 2 + suffixOfs;
            return (suffixSize, ptr);
        }

        internal static unsafe void SetSuffix(IntPtr suffixPtr, int i, int maxChildren, (uint Size, IntPtr Ptr) src)
        {
            if (src.Ptr == IntPtr.Zero || src.Size == 0) return;
            var ptr = suffixPtr;
            var suffixOfs = *(ushort*)(ptr + i * 2);
            var suffixOfs2 = *(ushort*)(ptr + i * 2 + 2);
            Debug.Assert(suffixOfs == suffixOfs2);
            var suffixOfs3 = *(ushort*)(ptr + 2 * maxChildren);
            for (var j = i + 1; i <= maxChildren; j++)
            {
                *(ushort*)(ptr + j * 2) += (ushort)src.Size;
            }
            ptr += 2 * maxChildren + 2;
            if (suffixOfs3 > suffixOfs2) ArtUtils.MoveMemory(ptr + suffixOfs2, ptr + (int)(suffixOfs2 + src.Size), suffixOfs3 - suffixOfs2);
            ArtUtils.CopyMemory(src.Ptr, ptr + suffixOfs, (int)src.Size);
        }

        internal static unsafe (uint Size, IntPtr Ptr) GetSuffixSizeAndPtr(IntPtr nodePtr, int i)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            if (!header._nodeType.HasFlag(NodeType12.HasSuffixes))
            {
                return (0u, IntPtr.Zero);
            }
            var (baseSize, maxChildren) = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                ptr += sizeof(uint);
            }
            var suffixOfs = *(ushort*)(ptr + i * 2);
            var suffixSize = (uint)(*(ushort*)(ptr + i * 2 + 2) - suffixOfs);
            ptr += 2 * maxChildren + 2 + suffixOfs;
            return (suffixSize, ptr);
        }

        internal static unsafe (uint, IntPtr) GetSuffixTotalSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            if (!header._nodeType.HasFlag(NodeType12.HasSuffixes))
            {
                return (0u, IntPtr.Zero);
            }
            var (baseSize, maxChildren) = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                ptr += sizeof(uint);
            }
            var suffixSize = *(ushort*)(ptr + maxChildren * 2);
            return (suffixSize, ptr);
        }

        internal static unsafe (uint Size, IntPtr Ptr) GetValueSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var (baseSize, maxChildren) = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                prefixSize = *(uint*)ptr;
                ptr += sizeof(uint);
            }
            if (header._nodeType.HasFlag(NodeType12.HasSuffixes))
            {
                ptr += 2 * maxChildren;
                ptr += 2 + *(ushort*)ptr;
            }
            ptr += (int)prefixSize;
            ptr = ArtUtils.AlignPtrUpInt32(ptr);
            return (12, ptr);
        }

        internal static unsafe (uint prefixSize, IntPtr prefixPtr, uint totalSuffixSize, IntPtr suffixPtr, uint valueSize, IntPtr valuePtr) GetAllSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var (baseSize, maxChildren) = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                prefixSize = *(uint*)ptr;
                ptr += sizeof(uint);
            }
            var suffixPtr = IntPtr.Zero;
            var totalSuffixSize = 0u;
            if (header._nodeType.HasFlag(NodeType12.HasSuffixes))
            {
                suffixPtr = ptr;
                ptr += 2 * maxChildren;
                totalSuffixSize = *(ushort*)ptr;
                ptr += 2 + (int)totalSuffixSize;
            }
            var prefixPtr = ptr;
            if (header._nodeType.HasFlag(NodeType12.IsLeaf))
            {
                ptr += (int)prefixSize;
                ptr = ArtUtils.AlignPtrUpInt32(ptr);
                return (prefixSize, prefixPtr, totalSuffixSize, suffixPtr, 12, ptr);
            }
            return (prefixSize, prefixPtr, totalSuffixSize, suffixPtr, 0, IntPtr.Zero);
        }

        internal static unsafe (uint prefixSize, IntPtr prefixPtr, IntPtr suffixPtr, uint valueSize, IntPtr valuePtr) GetAllSizeAndPtr(IntPtr nodePtr, uint totalSuffixSize)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var (baseSize, maxChildren) = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                prefixSize = *(uint*)ptr;
                ptr += sizeof(uint);
            }
            var suffixPtr = IntPtr.Zero;
            if (header._nodeType.HasFlag(NodeType12.HasSuffixes))
            {
                suffixPtr = ptr;
                ptr += 2 * maxChildren;
                ptr += 2 + (int)totalSuffixSize;
            }
            var prefixPtr = ptr;
            if (header._nodeType.HasFlag(NodeType12.IsLeaf))
            {
                ptr += (int)prefixSize;
                ptr = ArtUtils.AlignPtrUpInt32(ptr);
                return (prefixSize, prefixPtr, suffixPtr, 12, ptr);
            }
            return (prefixSize, prefixPtr, suffixPtr, 0, IntPtr.Zero);
        }

        internal static IntPtr PtrInNode(IntPtr node, int posInNode)
        {
            var nodeType = Ptr2NodeHeader(node)._nodeType;
            switch (nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf: return node + 16;
                case NodeType12.Node4: return node + 16 + 4 + posInNode * 12;
                case NodeType12.Node16: return node + 16 + 16 + posInNode * 12;
                case NodeType12.Node48: return node + 16 + 256 + posInNode * 12;
                case NodeType12.Node256: return node + 16 + posInNode * 12;
                default: throw new InvalidOperationException();
            }
        }

        internal static int MaxChildren(NodeType12 nodeType)
        {
            switch (nodeType & NodeType12.NodeSizeMask)
            {
                case NodeType12.NodeLeaf: return 0;
                case NodeType12.Node4: return 4;
                case NodeType12.Node16: return 16;
                case NodeType12.Node48: return 48;
                case NodeType12.Node256: return 256;
                default: throw new InvalidOperationException();
            }
        }
    }
}
