using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.Buffer
{
    static class TreeNodeUtils
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
        internal static IntPtr AlignPtrUpInt16(IntPtr ptr)
        {
            return ptr + ((int)ptr.ToInt64() & 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint AlignUIntUpInt16(uint ptr)
        {
            return ptr + (ptr & 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr AlignPtrUpInt32(IntPtr ptr)
        {
            return ptr + (~(int)ptr.ToInt64() + 1 & 3);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint AlignUIntUpInt32(uint ptr)
        {
            return ptr + (~ptr + 1 & 3);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr AlignPtrUpInt64(IntPtr ptr)
        {
            return ptr + (~(int)ptr.ToInt64() + 1 & 7);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static uint AlignUIntUpInt64(uint ptr)
        {
            return ptr + (~ptr + 1 & 7);
        }

        internal static unsafe void CopyMemory(IntPtr src, IntPtr dst, int size)
        {
            Unsafe.CopyBlockUnaligned(dst.ToPointer(), src.ToPointer(), (uint)size);
        }

        internal static unsafe void MoveMemory(IntPtr src, IntPtr dst, int size)
        {
            new Span<byte>(src.ToPointer(), size).CopyTo(new Span<byte>(dst.ToPointer(), size));
        }

        internal static bool IsPrefix(ReadOnlySpan<byte> data, ReadOnlySpan<byte> prefix)
        {
            if (data.Length < prefix.Length)
                return false;
            return data.Slice(0, prefix.Length).SequenceEqual(prefix);
        }

        internal static unsafe int FindFirstDifference(ReadOnlySpan<byte> buf1, IntPtr buf2IntPtr, int len)
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

        internal static unsafe int FindFirstDifference(ReadOnlySpan<byte> buf1, ReadOnlySpan<byte> buf2)
        {
            var len = Math.Min(buf1.Length, buf2.Length);
            if (len == 0) return 0;
            fixed (byte* buf1Ptr = &MemoryMarshal.GetReference(buf1))
            fixed (byte* buf2Ptr = &MemoryMarshal.GetReference(buf2))
            {
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

        internal static unsafe int FindFirstDifference(in ReadOnlySpan<byte> buf1, in ReadOnlySpan<byte> buf2a, in ReadOnlySpan<byte> buf2b)
        {
            var pos = FindFirstDifference(buf1, buf2a);
            if (pos < buf2a.Length || pos == buf1.Length)
            {
                return pos;
            }
            return buf2a.Length + FindFirstDifference(buf1.Slice(buf2a.Length), buf2b);
        }

        internal static uint CalcCommonPrefix(ByteBuffer[] keys)
        {
            var first = keys[0].AsSyncReadOnlySpan();
            var res = first.Length;
            for (var i = 1; i < keys.Length; i++)
            {
                res = FindFirstDifference(first, keys[i].AsSyncReadOnlySpan());
                if (res == 0) return 0;
                first = first.Slice(0, res);
            }
            return (uint)res;
        }
    }
}
