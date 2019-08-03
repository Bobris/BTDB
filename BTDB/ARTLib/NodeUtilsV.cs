using BTDB.Buffer;
using System;
using System.Runtime.CompilerServices;

namespace BTDB.ARTLib
{
    static class NodeUtilsV
    {
        internal static int BaseSize(NodeTypeV nodeType)
        {
            switch (nodeType & NodeTypeV.NodeSizeMask)
            {
                case NodeTypeV.NodeLeaf: return 16;
                case NodeTypeV.Node4: return 16 + 4 + 4 * 8;
                case NodeTypeV.Node16: return 16 + 16 + 16 * 8;
                case NodeTypeV.Node48: return 16 + 256 + 48 * 8;
                case NodeTypeV.Node256: return 16 + 256 * 8;
                default: throw new InvalidOperationException();
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsPtrPtr(IntPtr child)
        {
            return ((long)child & 1) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr ReadPtr(IntPtr ptr)
        {
            return TreeNodeUtils.ReadIntPtrUnaligned(ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref NodeHeaderV Ptr2NodeHeader(IntPtr pointerInt)
        {
            return ref *(NodeHeaderV*)pointerInt;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Reference(IntPtr node)
        {
            if (node == IntPtr.Zero)
                return;
            ref var nodeHeader = ref Ptr2NodeHeader(node);
            nodeHeader.Reference();
        }

        internal static (uint Size, IntPtr Ptr) GetPrefixSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeaderV header = ref Ptr2NodeHeader(nodePtr);
            var size = (uint)header._keyPrefixLength;
            if (size == 0) return (0, IntPtr.Zero);
            var baseSize = BaseSize(header._nodeType);
            var ptr = nodePtr + baseSize;
            if (size == 0xffff)
            {
                size = (uint)TreeNodeUtils.ReadInt32Aligned(ptr);
                ptr += sizeof(uint);
            }
            if (header._nodeType.HasFlag(NodeTypeV.IsLeaf))
            {
                ptr += sizeof(uint);
            }
            return (size, ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint GetPrefixSize(IntPtr nodePtr)
        {
            ref NodeHeaderV header = ref Ptr2NodeHeader(nodePtr);
            var size = (uint)header._keyPrefixLength;
            if (size == 0xffff)
            {
                var baseSize = BaseSize(header._nodeType);
                var ptr = nodePtr + baseSize;
                size = (uint)TreeNodeUtils.ReadInt32Aligned(ptr);
            }
            return size;
        }

        internal static unsafe (uint Size, IntPtr Ptr) GetValueSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeaderV header = ref Ptr2NodeHeader(nodePtr);
            var baseSize = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                prefixSize = *(uint*)ptr;
                ptr += sizeof(uint);
            }
            uint size;
            size = *(uint*)ptr;
            ptr += sizeof(uint);
            ptr += (int)prefixSize;
            return (size, ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint ReadLenFromPtr(IntPtr ptr)
        {
            TreeNodeUtils.AssertLittleEndian();
            unsafe { return ((uint)*(byte*)ptr.ToPointer()) >> 1; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr SkipLenFromPtr(IntPtr ptr)
        {
            return ptr + (BitConverter.IsLittleEndian ? 1 : 0);
        }

        internal static IntPtr PtrInNode(IntPtr node, int posInNode)
        {
            var nodeType = Ptr2NodeHeader(node)._nodeType;
            switch (nodeType & NodeTypeV.NodeSizeMask)
            {
                case NodeTypeV.NodeLeaf: return node + 16;
                case NodeTypeV.Node4: return node + 16 + 4 + posInNode * 8;
                case NodeTypeV.Node16: return node + 16 + 16 + posInNode * 8;
                case NodeTypeV.Node48: return node + 16 + 256 + posInNode * 8;
                case NodeTypeV.Node256: return node + 16 + posInNode * 8;
                default: throw new InvalidOperationException();
            }
        }

        internal static int MaxChildren(NodeTypeV nodeType)
        {
            switch (nodeType & NodeTypeV.NodeSizeMask)
            {
                case NodeTypeV.NodeLeaf: return 0;
                case NodeTypeV.Node4: return 4;
                case NodeTypeV.Node16: return 16;
                case NodeTypeV.Node48: return 48;
                case NodeTypeV.Node256: return 256;
                default: throw new InvalidOperationException();
            }
        }
    }
}
