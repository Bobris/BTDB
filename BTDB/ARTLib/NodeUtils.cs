using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
{
    static class NodeUtils
    {
        internal static int BaseSize(NodeType nodeType)
        {
            switch (nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                case NodeType.NodeLeaf | NodeType.Has12BPtrs:
                    return 16;
                case NodeType.Node4: return 16 + 4 + 4 * 8;
                case NodeType.Node4 | NodeType.Has12BPtrs: return 16 + 4 + 4 * 12;
                case NodeType.Node16: return 16 + 16 + 16 * 8;
                case NodeType.Node16 | NodeType.Has12BPtrs: return 16 + 16 + 16 * 12;
                case NodeType.Node48: return 16 + 256 + 48 * 8;
                case NodeType.Node48 | NodeType.Has12BPtrs: return 16 + 256 + 48 * 12;
                case NodeType.Node256: return 16 + 256 * 8;
                case NodeType.Node256 | NodeType.Has12BPtrs: return 16 + 256 * 12;
                default: throw new InvalidOperationException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr Read12Ptr(IntPtr childPtr)
        {
            return Marshal.ReadIntPtr(childPtr + sizeof(uint));
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
        internal static bool IsPtrPtr(IntPtr child)
        {
            return ((long)child & 1) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr ReadPtr(IntPtr ptr)
        {
            return Marshal.ReadIntPtr(ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref NodeHeader Ptr2NodeHeader(IntPtr pointerInt)
        {
            unsafe
            {
                return ref *(NodeHeader*)pointerInt;
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr AlignPtrUpInt32(IntPtr ptr)
        {
            return ptr + (((~(int)ptr.ToInt64()) + 1) & 3);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint AlignUIntUpInt32(uint ptr)
        {
            return ptr + (((~ptr) + 1) & 3);
        }

        internal static void Reference(IntPtr node)
        {
            if (node == IntPtr.Zero)
                return;
            ref var nodeHeader = ref Ptr2NodeHeader(node);
            nodeHeader.Reference();
        }

        internal static (uint Size, IntPtr Ptr) GetPrefixSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeader header = ref Ptr2NodeHeader(nodePtr);
            var size = (uint)header._keyPrefixLength;
            if (size == 0) return (0, IntPtr.Zero);
            var baseSize = BaseSize(header._nodeType);
            var ptr = nodePtr + baseSize;
            if (size == 0xffff)
            {
                size = (uint)Marshal.ReadInt32(ptr);
                ptr += sizeof(uint);
            }
            if ((header._nodeType & (NodeType.IsLeaf | NodeType.Has12BPtrs)) == NodeType.IsLeaf)
            {
                ptr += sizeof(uint);
            }
            return (size, ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint GetPrefixSize(IntPtr nodePtr)
        {
            ref NodeHeader header = ref Ptr2NodeHeader(nodePtr);
            var size = (uint)header._keyPrefixLength;
            if (size == 0xffff)
            {
                var baseSize = BaseSize(header._nodeType);
                var ptr = nodePtr + baseSize;
                size = (uint)Marshal.ReadInt32(ptr);
            }
            return size;
        }

        internal static (uint Size, IntPtr Ptr) GetValueSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeader header = ref Ptr2NodeHeader(nodePtr);
            var baseSize = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                unsafe { prefixSize = *(uint*)ptr; };
                ptr += sizeof(uint);
            }
            uint size;
            if ((header._nodeType & (NodeType.IsLeaf | NodeType.Has12BPtrs)) == NodeType.IsLeaf)
            {
                unsafe { size = *(uint*)ptr; };
                ptr += sizeof(uint);
                ptr += (int)prefixSize;
            }
            else
            {
                size = 12;
                ptr += (int)prefixSize;
                ptr = AlignPtrUpInt32(ptr);
            }
            return (size, ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint ReadLenFromPtr(IntPtr ptr)
        {
            AssertLittleEndian();
            unsafe { return ((uint)*(byte*)ptr.ToPointer()) >> 1; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr SkipLenFromPtr(IntPtr ptr)
        {
            AssertLittleEndian();
            return ptr + 1;
        }

        internal static IntPtr PtrInNode(IntPtr node, int posInNode)
        {
            var nodeType = Ptr2NodeHeader(node)._nodeType;
            switch (nodeType & NodeType.NodeSizePtrMask)
            {
                case NodeType.NodeLeaf:
                case NodeType.NodeLeaf | NodeType.Has12BPtrs:
                    return node + 16;
                case NodeType.Node4: return node + 16 + 4 + posInNode * 8;
                case NodeType.Node4 | NodeType.Has12BPtrs: return node + 16 + 4 + posInNode * 12;
                case NodeType.Node16: return node + 16 + 16 + posInNode * 8;
                case NodeType.Node16 | NodeType.Has12BPtrs: return node + 16 + 16 + posInNode * 12;
                case NodeType.Node48: return node + 16 + 256 + posInNode * 8;
                case NodeType.Node48 | NodeType.Has12BPtrs: return node + 16 + 256 + posInNode * 12;
                case NodeType.Node256: return node + 16 + posInNode * 8;
                case NodeType.Node256 | NodeType.Has12BPtrs: return node + 16 + posInNode * 12;
                default: throw new InvalidOperationException();
            }
        }

        internal static int MaxChildren(NodeType nodeType)
        {
            switch (nodeType & NodeType.NodeSizeMask)
            {
                case NodeType.NodeLeaf: return 0;
                case NodeType.Node4: return 4;
                case NodeType.Node16: return 16;
                case NodeType.Node48: return 48;
                case NodeType.Node256: return 256;
                default: throw new InvalidOperationException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void AssertLittleEndian()
        {
            if (!BitConverter.IsLittleEndian)
            {
                throw new NotSupportedException("Only Little Endian platform supported");
            }
        }
    }
}
