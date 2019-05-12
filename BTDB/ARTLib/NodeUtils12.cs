using System;
using System.Runtime.CompilerServices;

namespace BTDB.ARTLib
{
    static class NodeUtils12
    {
        internal static int BaseSize(NodeType12 nodeType)
        {
            switch (nodeType & NodeType12.NodeSizePtrMask)
            {
                case NodeType12.NodeLeaf:
                case NodeType12.NodeLeaf | NodeType12.Has12BPtrs:
                    return 16;
                case NodeType12.Node4: return 16 + 4 + 4 * 8;
                case NodeType12.Node4 | NodeType12.Has12BPtrs: return 16 + 4 + 4 * 12;
                case NodeType12.Node16: return 16 + 16 + 16 * 8;
                case NodeType12.Node16 | NodeType12.Has12BPtrs: return 16 + 16 + 16 * 12;
                case NodeType12.Node48: return 16 + 256 + 48 * 8;
                case NodeType12.Node48 | NodeType12.Has12BPtrs: return 16 + 256 + 48 * 12;
                case NodeType12.Node256: return 16 + 256 * 8;
                case NodeType12.Node256 | NodeType12.Has12BPtrs: return 16 + 256 * 12;
                default: throw new InvalidOperationException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe byte ReadByte(IntPtr ptr)
        {
            return *(byte*)ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void WriteByte(IntPtr ptr, byte value)
        {
            *(byte*)ptr = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void WriteByte(IntPtr ptr, int offset, byte value)
        {
            *(byte*)(ptr + offset) = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void WriteInt32Alligned(IntPtr ptr, int value)
        {
            *(int*)ptr = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe int ReadInt32Alligned(IntPtr ptr)
        {
            return *(int*)ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe IntPtr ReadIntPtrUnalligned(IntPtr ptr)
        {
            return *(IntPtr*)ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void WriteIntPtrUnalligned(IntPtr ptr, IntPtr value)
        {
            *(IntPtr*)ptr = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr Read12Ptr(IntPtr childPtr)
        {
            return ReadIntPtrUnalligned(childPtr + sizeof(uint));
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
            return ReadIntPtrUnalligned(ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref NodeHeader12 Ptr2NodeHeader(IntPtr pointerInt)
        {
            return ref *(NodeHeader12*)pointerInt;
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
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var size = (uint)header._keyPrefixLength;
            if (size == 0) return (0, IntPtr.Zero);
            var baseSize = BaseSize(header._nodeType);
            var ptr = nodePtr + baseSize;
            if (size == 0xffff)
            {
                size = (uint)ReadInt32Alligned(ptr);
                ptr += sizeof(uint);
            }
            if ((header._nodeType & (NodeType12.IsLeaf | NodeType12.Has12BPtrs)) == NodeType12.IsLeaf)
            {
                ptr += sizeof(uint);
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
                var baseSize = BaseSize(header._nodeType);
                var ptr = nodePtr + baseSize;
                size = (uint)ReadInt32Alligned(ptr);
            }
            return size;
        }

        internal static (uint Size, IntPtr Ptr) GetValueSizeAndPtr(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var baseSize = BaseSize(header._nodeType);
            var prefixSize = (uint)header._keyPrefixLength;
            var ptr = nodePtr + baseSize;
            if (prefixSize == 0xffff)
            {
                unsafe { prefixSize = *(uint*)ptr; };
                ptr += sizeof(uint);
            }
            uint size;
            if ((header._nodeType & (NodeType12.IsLeaf | NodeType12.Has12BPtrs)) == NodeType12.IsLeaf)
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
            switch (nodeType & NodeType12.NodeSizePtrMask)
            {
                case NodeType12.NodeLeaf:
                case NodeType12.NodeLeaf | NodeType12.Has12BPtrs:
                    return node + 16;
                case NodeType12.Node4: return node + 16 + 4 + posInNode * 8;
                case NodeType12.Node4 | NodeType12.Has12BPtrs: return node + 16 + 4 + posInNode * 12;
                case NodeType12.Node16: return node + 16 + 16 + posInNode * 8;
                case NodeType12.Node16 | NodeType12.Has12BPtrs: return node + 16 + 16 + posInNode * 12;
                case NodeType12.Node48: return node + 16 + 256 + posInNode * 8;
                case NodeType12.Node48 | NodeType12.Has12BPtrs: return node + 16 + 256 + posInNode * 12;
                case NodeType12.Node256: return node + 16 + posInNode * 8;
                case NodeType12.Node256 | NodeType12.Has12BPtrs: return node + 16 + posInNode * 12;
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
