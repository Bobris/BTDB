using System;
using System.Runtime.CompilerServices;

namespace BTDB.ARTLib
{
    static class ArtUtils
    {
        internal static void ThrowCursorHaveToBeValid()
        {
            throw new InvalidOperationException("Cursor must be valid for this operation");
        }

        internal static void ThrowCursorNotWrittable()
        {
            throw new InvalidOperationException("Cursor not writtable");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void AssertLittleEndian()
        {
            if (!BitConverter.IsLittleEndian)
            {
                throw new NotSupportedException("Only Little Endian platform supported");
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
        internal static unsafe void WriteInt32Aligned(IntPtr ptr, int value)
        {
            *(int*)ptr = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe int ReadInt32Aligned(IntPtr ptr)
        {
            return *(int*)ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe IntPtr ReadIntPtrUnaligned(IntPtr ptr)
        {
            return *(IntPtr*)ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void WriteIntPtrUnaligned(IntPtr ptr, IntPtr value)
        {
            *(IntPtr*)ptr = value;
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

        internal static unsafe void CopyMemory(IntPtr src, IntPtr dst, int size)
        {
            Unsafe.CopyBlockUnaligned(dst.ToPointer(), src.ToPointer(), (uint)size);
        }

        internal static unsafe void MoveMemory(IntPtr src, IntPtr dst, int size)
        {
            new Span<byte>(src.ToPointer(), size).CopyTo(new Span<byte>(dst.ToPointer(), size));
        }
    }
}
