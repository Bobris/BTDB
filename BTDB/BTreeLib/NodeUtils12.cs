using BTDB.Buffer;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BTDB.BTreeLib
{
    static class NodeUtils12
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref NodeHeader12 Ptr2NodeHeader(IntPtr pointerInt)
        {
            Debug.Assert((uint)(*(NodeHeader12*)pointerInt)._nodeType < 4);
            return ref *(NodeHeader12*)pointerInt;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref NodeHeader12 Ptr2NodeHeaderInit(IntPtr pointerInt)
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

        internal static unsafe Span<byte> GetPrefixSpan(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var size = header._keyPrefixLength;
            if (size == 0) return new Span<byte>();
            return new Span<byte>((nodePtr + (int)header.Size).ToPointer(), size);
        }

        internal static unsafe Span<ushort> GetKeySpans(IntPtr nodePtr, out Span<byte> keys)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var ptr = nodePtr + (int)header.Size;
            ptr += header._keyPrefixLength;
            Debug.Assert(!header.HasLongKeys);
            ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
            var offsetsPtr = ptr;
            var offsetsCount = header._childCount + (header.IsNodeLeaf ? 1 : 0);
            ptr += 2 * offsetsCount;
            var keysLen = *(ushort*)(ptr - 2);
            keys = new Span<byte>(ptr.ToPointer(), keysLen);
            return new Span<ushort>(offsetsPtr.ToPointer(), offsetsCount);
        }

        internal static unsafe Span<ushort> GetKeySpans(IntPtr nodePtr, uint totalSufixLength, out Span<byte> keySufixes)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var ptr = nodePtr + (int)header.Size;
            ptr += header._keyPrefixLength;
            Debug.Assert(!header.HasLongKeys);
            ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
            var offsetsPtr = ptr;
            var offsetsCount = header._childCount + (header.IsNodeLeaf ? 1 : 0);
            ptr += 2 * offsetsCount;
            keySufixes = new Span<byte>(ptr.ToPointer(), (int)totalSufixLength);
            return new Span<ushort>(offsetsPtr.ToPointer(), offsetsCount);
        }

        internal static unsafe Span<IntPtr> GetLongKeyPtrs(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            Debug.Assert(header.HasLongKeys);
            var ptr = nodePtr + (int)header.Size;
            ptr += header._keyPrefixLength;
            ptr = TreeNodeUtils.AlignPtrUpInt64(ptr);
            return new Span<IntPtr>(ptr.ToPointer(), 8 * (header._childCount - (header.IsNodeLeaf ? 0 : 1)));
        }

        internal static unsafe Span<byte> GetLeafValues(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            Debug.Assert(header.IsNodeLeaf);
            var ptr = nodePtr + 8;
            ptr += header._keyPrefixLength;
            if (header.HasLongKeys)
            {
                ptr = TreeNodeUtils.AlignPtrUpInt64(ptr);
                ptr += 8 * header._childCount;
            }
            else
            {
                ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
                ptr += 2 * header._childCount;
                ptr += 2 + *(ushort*)ptr;
                ptr = TreeNodeUtils.AlignPtrUpInt32(ptr);
            }
            return new Span<byte>(ptr.ToPointer(), 12 * header._childCount);
        }

        internal static unsafe Span<IntPtr> GetBranchValuePtrs(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            Debug.Assert(!header.IsNodeLeaf);
            var ptr = nodePtr + 16;
            ptr += header._keyPrefixLength;
            if (header.HasLongKeys)
            {
                ptr = TreeNodeUtils.AlignPtrUpInt64(ptr);
                ptr += 8 * (header._childCount - 1);
            }
            else
            {
                ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
                ptr += 2 * (header._childCount - 1);
                ptr += 2 + *(ushort*)ptr;
                ptr = TreeNodeUtils.AlignPtrUpInt64(ptr);
            }
            return new Span<IntPtr>(ptr.ToPointer(), header._childCount);
        }

        internal static IntPtr GetBranchValuePtr(IntPtr nodePtr, int index)
        {
            return GetBranchValuePtrs(nodePtr)[index];
        }

        internal static void SetBranchValuePtr(IntPtr nodePtr, int index, IntPtr childNodePtr)
        {
            GetBranchValuePtrs(nodePtr)[index] = childNodePtr;
        }

        internal static unsafe int NodeSize(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            var ptr = nodePtr + (int)header.Size;
            ptr += header._keyPrefixLength;
            if (header.HasLongKeys)
            {
                ptr = TreeNodeUtils.AlignPtrUpInt64(ptr);
                ptr += 8 * header._childCount;
            }
            else
            {
                ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
                ptr += 2 * header.KeyCount;
                ptr += 2 + *(ushort*)ptr;
                if (header.IsNodeLeaf)
                    ptr = TreeNodeUtils.AlignPtrUpInt32(ptr);
                else
                    ptr = TreeNodeUtils.AlignPtrUpInt64(ptr);
            }
            if (header.IsNodeLeaf)
                return (int)(ptr.ToInt64() - nodePtr.ToInt64() + 12 * header._childCount);
            else
                return (int)(ptr.ToInt64() - nodePtr.ToInt64() + 8 * header._childCount);
        }

        internal static unsafe long GetTotalSufixLen(IntPtr nodePtr)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            long len = 0;
            if (header.HasLongKeys)
            {
                var keys = GetLongKeyPtrs(nodePtr);
                for (int i = 0; i < keys.Length; i++)
                {
                    len += TreeNodeUtils.ReadInt32Aligned(keys[i]);
                }
            }
            else
            {
                var ptr = nodePtr + (int)header.Size;
                ptr += header._keyPrefixLength;
                ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
                var offsetsPtr = ptr;
                var offsetsCount = header._childCount + (header.IsNodeLeaf ? 1 : 0);
                ptr += 2 * offsetsCount;
                len = *(ushort*)(ptr - 2);
            }
            return len;
        }

        internal static unsafe long GetTotalSufixLen(IntPtr nodePtr, int start, int end)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            long len = 0;
            if (header.HasLongKeys)
            {
                var keys = GetLongKeyPtrs(nodePtr);
                for (int i = start; i < end; i++)
                {
                    len += TreeNodeUtils.ReadInt32Aligned(keys[i]);
                }
            }
            else
            {
                var ptr = nodePtr + (int)header.Size;
                ptr += header._keyPrefixLength;
                ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
                var offsetsPtr = (ushort*)ptr;
                return offsetsPtr[end] - offsetsPtr[start];
            }
            return len;
        }

        internal static unsafe long GetTotalSufixLenExcept(IntPtr nodePtr, int start, int end)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            long len = 0;
            if (header.HasLongKeys)
            {
                var keys = GetLongKeyPtrs(nodePtr);
                for (int i = 0; i < keys.Length; i++)
                {
                    if (i == start)
                    {
                        i = end;
                        continue;
                    }
                    len += TreeNodeUtils.ReadInt32Aligned(keys[i]);
                }
            }
            else
            {
                var ptr = nodePtr + (int)header.Size;
                ptr += header._keyPrefixLength;
                ptr = TreeNodeUtils.AlignPtrUpInt16(ptr);
                var offsetsPtr = (ushort*)ptr;
                return offsetsPtr[header.KeyCount] - (offsetsPtr[end + 1] - offsetsPtr[start]);
            }
            return len;
        }

        internal unsafe static Span<byte> LongKeyPtrToSpan(IntPtr ptr)
        {
            var size = TreeNodeUtils.ReadInt32Aligned(ptr);
            return new Span<byte>((ptr + 4).ToPointer(), size);
        }

        internal static Span<byte> GetLeftestKey(IntPtr nodePtr, out Span<byte> keySufix)
        {
            ref NodeHeader12 header = ref Ptr2NodeHeader(nodePtr);
            while (!header.IsNodeLeaf)
            {
                nodePtr = GetBranchValuePtr(nodePtr, 0);
                header = ref Ptr2NodeHeader(nodePtr);
            }
            if (header.HasLongKeys)
            {
                keySufix = LongKeyPtrToSpan(GetLongKeyPtrs(nodePtr)[0]);
            }
            else
            {
                var keyOfs = GetKeySpans(nodePtr, out var keySufixes);
                keySufix = keySufixes.Slice(0, keyOfs[1]);
            }
            return GetPrefixSpan(nodePtr);
        }

        internal static long RecalcRecursiveChildrenCount(IntPtr nodePtr)
        {
            var children = GetBranchValuePtrs(nodePtr);
            var res = 0UL;
            for (var i = 0; i < children.Length; i++)
            {
                res += Ptr2NodeHeader(children[i]).RecursiveChildCount;
            }
            Ptr2NodeHeader(nodePtr)._recursiveChildCount = res;
            return (long)res;
        }

        internal static void CopyAndReferenceBranchValues(Span<IntPtr> from, Span<IntPtr> to)
        {
            for (var i = 0; i < from.Length; i++)
            {
                var node = from[i];
                Reference(node);
                to[i] = node;
            }
        }
    }
}
