using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;

namespace BTDB.Buffer;

public static class PackUnpack
{
    public static void PackUInt16LE(byte[] data, int offset, ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(data.AsSpan(offset), value);
    }

    public static ushort UnpackUInt16LE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
    }

    public static void PackInt16LE(byte[] data, int offset, short value)
    {
        BinaryPrimitives.WriteInt16LittleEndian(data.AsSpan(offset), value);
    }

    public static short UnpackInt16LE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadInt16LittleEndian(data.AsSpan(offset));
    }

    public static void PackUInt32LE(byte[] data, int offset, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(data.AsSpan(offset), value);
    }

    public static uint UnpackUInt32LE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset));
    }

    public static void PackInt32LE(byte[] data, int offset, int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(offset), value);
    }

    public static void PackInt32BE(byte[] data, int offset, int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(offset), value);
    }

    public static int UnpackInt32LE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
    }

    public static int UnpackInt32BE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset));
    }

    public static void PackUInt64LE(byte[] data, int offset, ulong value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(data.AsSpan(offset), value);
    }

    public static ulong UnpackUInt64LE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(offset));
    }

    public static void PackInt64LE(byte[] data, int offset, long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(offset), value);
    }

    public static void PackInt64BE(byte[] data, int offset, long value)
    {
        BinaryPrimitives.WriteInt64BigEndian(data.AsSpan(offset), value);
    }

    public static long UnpackInt64LE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(offset));
    }

    public static long UnpackInt64BE(byte[] data, int offset)
    {
        return BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset));
    }

    public static void IncrementInt64LE(byte[] data, int offset)
    {
        var span = data.AsSpan(offset);
        var v = BinaryPrimitives.ReadInt64LittleEndian(span) + 1;
        BinaryPrimitives.WriteInt64LittleEndian(span, v);
    }

    public static uint LengthVUInt(uint value)
    {
        /* Logically doing commented code, but branch less => much faster
        if (value < 0x80) return 1;
        if (value < 0x4000) return 2;
        if (value < 0x200000) return 3;
        if (value < 0x10000000) return 4;
        return 5;
        */
        return (uint)(352 - BitOperations.LeadingZeroCount(value) * 9) >> 6;
    }

    public static uint LengthVUInt(ulong value)
    {
        /* Logically doing commented code, but branch less and memory access less => much faster
        if (value < 0x80) return 1;
        if (value < 0x4000) return 2;
        if (value < 0x200000) return 3;
        if (value < 0x10000000) return 4;
        if (value < 0x0800000000) return 5;
        if (value < 0x040000000000) return 6;
        if (value < 0x02000000000000) return 7;
        if (value < 0x0100000000000000) return 8;
        return 9;
        */
        return (uint)(20441 - BitOperations.LeadingZeroCount(value) * 287) >> 11;
    }

    public static uint LengthVUInt(byte[] data, int ofs)
    {
        var first = data[ofs];
        return LengthVUIntByFirstByte(first);
    }

    public static uint LengthVUIntByFirstByte(byte first)
    {
        /* Logically doing commented code, but branch less => much faster
        if (first < 0x80) return 1;
        if (first < 0xC0) return 2;
        if (first < 0xE0) return 3;
        if (first < 0xF0) return 4;
        if (first < 0xF8) return 5;
        if (first < 0xFC) return 6;
        if (first < 0xFE) return 7;
        return first == 0xFE ? 8 : 9;
        */
        return (uint)BitOperations.LeadingZeroCount(first ^ 0xffu) + 9 - 32;
    }

    public static void UnsafePackVUInt(ref byte data, ulong value, uint len)
    {
        Debug.Assert(LengthVUInt(value) == len);
        switch (len)
        {
            case 1:
            {
                data = (byte)value;
                return;
            }
            case 2:
            {
                value = 0x8000u + value;
                Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort)value));
                return;
            }
            case 3:
            {
                data = (byte)(0xC0 + (value >> 16));
                data = ref Unsafe.AddByteOffset(ref data, 1);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort)value));
                return;
            }
            case 4:
            {
                value = 0xE0000000u + value;
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            case 5:
            {
                data = (byte)(0xF0 + (value >> 32));
                data = ref Unsafe.AddByteOffset(ref data, 1);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            case 6:
            {
                var hiValue = (ushort)(0xF800u + (value >> 32));
                Unsafe.WriteUnaligned(ref data, AsBigEndian(hiValue));
                data = ref Unsafe.AddByteOffset(ref data, 2);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            case 7:
            {
                data = (byte)(0xFC + (value >> 48));
                data = ref Unsafe.AddByteOffset(ref data, 1);
                var hiValue = (ushort)(value >> 32);
                Unsafe.WriteUnaligned(ref data, AsBigEndian(hiValue));
                data = ref Unsafe.AddByteOffset(ref data, 2);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            case 8:
            {
                value += 0xFE00_0000_0000_0000ul;
                Unsafe.WriteUnaligned(ref data, AsBigEndian(value));
                return;
            }
            default:
            {
                data = 0xFF;
                data = ref Unsafe.AddByteOffset(ref data, 1);
                Unsafe.WriteUnaligned(ref data, AsBigEndian(value));
                return;
            }
        }
    }

    public static void PackVUInt(byte[] data, ref int ofs, uint value)
    {
        PackVUInt(data, ref ofs, (ulong)value);
    }

    public static void PackVUInt(byte[] data, ref int ofs, ulong value)
    {
        var len = LengthVUInt(value);
        if (data.Length < ofs + len) throw new IndexOutOfRangeException();
        UnsafePackVUInt(ref data[ofs], value, len);
        ofs += (int)len;
    }

    public static ushort AsBigEndian(ushort value)
    {
        return BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
    }

    public static uint AsBigEndian(uint value)
    {
        return BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
    }

    public static ulong AsBigEndian(ulong value)
    {
        return BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
    }

    public static ushort AsLittleEndian(ushort value)
    {
        return !BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
    }

    public static uint AsLittleEndian(uint value)
    {
        return !BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
    }

    public static ulong AsLittleEndian(ulong value)
    {
        return !BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
    }

    public static unsafe ulong UnsafeUnpackVUInt(nuint ptr)
    {
        ref var r = ref Unsafe.AsRef<byte>(ptr.ToPointer());
        var l = LengthVUIntByFirstByte(r);
        return UnsafeUnpackVUInt(ref r, l);
    }

    public static ulong UnsafeUnpackVUInt(ref byte data, uint len)
    {
        switch (len)
        {
            default:
                return data;
            case 2:
                return 0x3fffu & AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data));
            case 3:
            {
                var res = (data & 0x1Fu) << 16;
                data = ref Unsafe.AddByteOffset(ref data, 1);
                return res + AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data));
            }
            case 4:
                return 0x0fff_ffffu & AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
            case 5:
            {
                var res = (ulong)(data & 0x07u) << 32;
                data = ref Unsafe.AddByteOffset(ref data, 1);
                return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
            }
            case 6:
            {
                var res = (0x03fful & AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data))) << 32;
                data = ref Unsafe.AddByteOffset(ref data, 2);
                return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
            }
            case 7:
            {
                var res = (ulong)(data & 0x01u) << 48;
                data = ref Unsafe.AddByteOffset(ref data, 1);
                res += (ulong)AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data)) << 32;
                data = ref Unsafe.AddByteOffset(ref data, 2);
                return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
            }
            case 8:
                return 0x00ff_ffff_ffff_fffful & AsBigEndian(Unsafe.ReadUnaligned<ulong>(ref data));
            case 9:
                data = ref Unsafe.AddByteOffset(ref data, 1);
                return AsBigEndian(Unsafe.ReadUnaligned<ulong>(ref data));
        }
    }

    public static ulong UnpackVUInt(byte[] data, ref int ofs)
    {
        var first = data[ofs];
        var len = LengthVUIntByFirstByte(first);
        if ((uint)data.Length < (uint)ofs + len) throw new IndexOutOfRangeException();
        // All range checks were done already before, so now do it without them for speed
        var res = UnsafeUnpackVUInt(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(data.AsSpan()), ofs), len);
        ofs += (int)len;
        return res;
    }

    public static ulong UnpackVUInt(in ReadOnlySpan<byte> data)
    {
        var len = LengthVUIntByFirstByte(data[0]);
        if ((uint)data.Length < len) ThrowEndOfStreamException();
        // All range checks were done already before, so now do it without them for speed
        return UnsafeUnpackVUInt(ref MemoryMarshal.GetReference(data), len);
    }

    static ReadOnlySpan<byte> LzcToVIntLen => new byte[65]
    {
        9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5,
        5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1
    };

    public static uint LengthVInt(int value)
    {
        /* Logically doing commented code, but branch less => much faster
        if (-0x40 <= value && value < 0x40) return 1;
        if (-0x2000 <= value && value < 0x2000) return 2;
        if (-0x100000 <= value && value < 0x100000) return 3;
        if (-0x08000000 <= value && value < 0x08000000) return 4;
        return 5;
        */
        value ^= value >> 31; // Convert negative value to -value-1 and don't touch zero or positive
        return ((uint)(40 - BitOperations.LeadingZeroCount((uint)value)) * 9) >> 6;
    }

    public static uint LengthVInt(long value)
    {
        /* Logically doing commented code, but branch less => 4x-10x faster
        if (-0x40 <= value && value < 0x40) return 1;
        if (-0x2000 <= value && value < 0x2000) return 2;
        if (-0x100000 <= value && value < 0x100000) return 3;
        if (-0x08000000 <= value && value < 0x08000000) return 4;
        if (-0x0400000000 <= value && value < 0x0400000000) return 5;
        if (-0x020000000000 <= value && value < 0x020000000000) return 6;
        if (-0x01000000000000 <= value && value < 0x01000000000000) return 7;
        return 9;
        */
        value ^= value >> 63; // Convert negative value to -value-1 and don't touch zero or positive
        return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVIntLen),
            BitOperations.LeadingZeroCount((ulong)value));
    }

    public static uint LengthVInt(byte[] data, int ofs)
    {
        var first = data[ofs];
        return LengthVIntByFirstByte(first);
    }

    public static uint LengthVIntByFirstByte(uint first)
    {
        /* Logically doing commented code, but branch less => much faster
        if (0x40 <= first && first < 0xC0) return 1;
        if (0x20 <= first && first < 0xE0) return 2;
        if (0x10 <= first && first < 0xF0) return 3;
        if (0x08 <= first && first < 0xF8) return 4;
        if (0x04 <= first && first < 0xFC) return 5;
        if (0x02 <= first && first < 0xFE) return 6;
        if (0x01 <= first && first < 0xFF) return 7;
        return 9;
        */
        first ^= (uint)((sbyte)first >> 7) & 0xff;
        var res = BitOperations.LeadingZeroCount(first) + 8 - 32;
        return (uint)(0x976543211UL >> (res * 4)) & 0xf;
    }

    public static void UnsafePackVInt(ref byte data, long value, uint len)
    {
        Debug.Assert(LengthVInt(value) == len);
        var sign = value >> 63;
        switch (len)
        {
            case 1:
            {
                data = (byte)(value + 0x80);
                return;
            }
            case 2:
            {
                value = 0xC000u + value;
                value ^= sign & 0x8000;
                Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort)value));
                return;
            }
            case 3:
            {
                value = 0xE00000u + value;
                value ^= sign & 0xC00000u;
                data = (byte)((ulong)value >> 16);
                data = ref Unsafe.AddByteOffset(ref data, 1);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort)value));
                return;
            }
            case 4:
            {
                value = 0xF000_0000u + value;
                value ^= sign & 0xE000_0000;
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            case 5:
            {
                value = 0xF8_0000_0000L + value;
                value ^= sign & 0xF0_0000_0000;
                data = (byte)((ulong)value >> 32);
                data = ref Unsafe.AddByteOffset(ref data, 1);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            case 6:
            {
                value = 0xFC00_0000_0000L + value;
                value ^= sign & 0xF800_0000_0000;
                Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort)((ulong)value >> 32)));
                data = ref Unsafe.AddByteOffset(ref data, 2);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            case 7:
            {
                value = 0xFE_0000_0000_0000L + value;
                value ^= sign & 0xFC_0000_0000_0000;
                data = (byte)((ulong)value >> 48);
                data = ref Unsafe.AddByteOffset(ref data, 1);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort)((ulong)value >> 32)));
                data = ref Unsafe.AddByteOffset(ref data, 2);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((uint)value));
                return;
            }
            default: // It always 9
            {
                data = (byte)((sign & 0xFF) ^ 0xFF);
                data = ref Unsafe.AddByteOffset(ref data, 1);
                Unsafe.WriteUnaligned(ref data, AsBigEndian((ulong)value));
                return;
            }
        }
    }

    public static void PackVInt(byte[] data, ref int ofs, long value)
    {
        var len = LengthVInt(value);
        if (data.Length < ofs + len) throw new IndexOutOfRangeException();
        UnsafePackVInt(ref data[ofs], value, len);
        ofs += (int)len;
    }

    public static long UnsafeUnpackVInt(ref byte data, uint len)
    {
        switch (len)
        {
            case 1:
                return (long)data - 0x80;
            case 2:
            {
                var sign = (long)((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                return (0x1fffu & AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data))) - (0x2000 & sign);
            }
            case 3:
            {
                var sign = (long)((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                var res = (data & 0x0F) << 16;
                data = ref Unsafe.AddByteOffset(ref data, 1);
                return res + AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data)) - (0x10_0000 & sign);
            }
            case 4:
            {
                var sign = (long)((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                return (0x07ff_ffff & AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data))) - (0x0800_0000 & sign);
            }
            case 5:
            {
                var sign = (long)((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                var res = (long)(data & 0x03u) << 32;
                data = ref Unsafe.AddByteOffset(ref data, 1);
                return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data)) - (0x04_0000_0000 & sign);
            }
            case 6:
            {
                var sign = (long)((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                var res = (0x01ffL & AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data))) << 32;
                data = ref Unsafe.AddByteOffset(ref data, 2);
                return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data)) - (0x0200_0000_0000 & sign);
            }
            case 7:
            {
                var sign = (long)((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                data = ref Unsafe.AddByteOffset(ref data, 1);
                var res = (long)AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data)) << 32;
                data = ref Unsafe.AddByteOffset(ref data, 2);
                return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data)) - (0x01_0000_0000_0000 & sign);
            }
            default:
                data = ref Unsafe.AddByteOffset(ref data, 1);
                return (long)AsBigEndian(Unsafe.ReadUnaligned<ulong>(ref data));
        }
    }

    public static long UnpackVInt(byte[] data, ref int ofs)
    {
        var first = data[ofs];
        var len = LengthVIntByFirstByte(first);
        if ((uint)data.Length < (uint)ofs + len) throw new IndexOutOfRangeException();
        // All range checks were done already before, so now do it without them for speed
        var res = UnsafeUnpackVInt(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(data.AsSpan()), ofs), len);
        ofs += (int)len;
        return res;
    }

    public static void ThrowEndOfStreamException()
    {
        throw new EndOfStreamException();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref byte UnsafeGetAndAdvance(ref ReadOnlySpan<byte> p, int delta)
    {
        ref var res = ref MemoryMarshal.GetReference(p);
        p = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AddByteOffset(ref res, delta), p.Length - delta);
        return ref res;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref byte UnsafeGetAndAdvance(ref Span<byte> p, int delta)
    {
        ref var res = ref MemoryMarshal.GetReference(p);
        p = MemoryMarshal.CreateSpan(ref Unsafe.AddByteOffset(ref res, delta), p.Length - delta);
        return ref res;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe ref T UnsafeGetAndAdvance<T>(ref Span<byte> p) where T : unmanaged
    {
        ref var res = ref Unsafe.AsRef<T>(Unsafe.AsPointer(ref MemoryMarshal.GetReference(p)));
        p = MemoryMarshal.CreateSpan(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(p), sizeof(T)),
            p.Length - sizeof(T));
        return ref res;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe T UnsafeGet<T>(in ReadOnlySpan<byte> p) where T : unmanaged
    {
        return Unsafe.ReadUnaligned<T>(Unsafe.AsPointer(ref MemoryMarshal.GetReference(p)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void UnsafeAdvance(ref ReadOnlySpan<byte> p, int delta)
    {
        p = MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(p), delta), p.Length - delta);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void UnsafeAdvance(ref Span<byte> p, int delta)
    {
        p = MemoryMarshal.CreateSpan(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(p), delta), p.Length - delta);
    }

    public static uint SequenceEqualUpTo(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        return (uint)left.CommonPrefixLength(right);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static Vector<byte> LoadVector(ref byte start, nuint offset)
        => Unsafe.ReadUnaligned<Vector<byte>>(ref Unsafe.AddByteOffset(ref start, (nint)offset));

    // Next methods are heavily inspired by dotnet source code

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AllCharsInUInt32AreAscii(uint value)
    {
        return (value & ~0x007F007Fu) == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AllCharsInUInt64AreAscii(ulong value)
    {
        return (value & ~0x007F007F_007F007Ful) == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AllCharsInUInt32AreAsciiM1(ref uint value)
    {
        if ((value & ~0x007F007Fu) != 0) return false;
        var v = value + 0x00010001u;
        if ((v & ~0x007F007Fu) != 0) return false;
        value = v;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AllCharsInUInt64AreAsciiM1(ref ulong value)
    {
        if ((value & ~0x007F007F_007F007Ful) != 0) return false;
        var v = value + 0x00010001_00010001ul;
        if ((v & ~0x007F007F_007F007Ful) != 0) return false;
        value = v;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AllCharsInVectorAreAscii(Vector128<ushort> utf16Vector)
    {
        // prefer architecture specific intrinsic as they offer better perf
        if (Sse2.IsSupported)
        {
            if (Sse41.IsSupported)
            {
                var asciiMaskForTestZ = Vector128.Create((ushort)0xFF80);
                // If a non-ASCII bit is set in any WORD of the vector, we have seen non-ASCII data.
                return Sse41.TestZ(utf16Vector.AsInt16(), asciiMaskForTestZ.AsInt16());
            }
            else
            {
                var asciiMaskForAddSaturate = Vector128.Create((ushort)0x7F80);
                // The operation below forces the 0x8000 bit of each WORD to be set iff the WORD element
                // has value >= 0x0800 (non-ASCII). Then we'll treat the vector as a BYTE vector in order
                // to extract the mask. Reminder: the 0x0080 bit of each WORD should be ignored.
                return (Sse2.MoveMask(Sse2.AddSaturate(utf16Vector, asciiMaskForAddSaturate).AsByte()) &
                        0b_1010_1010_1010_1010) == 0;
            }
        }
        else if (AdvSimd.Arm64.IsSupported)
        {
            // First we pick four chars, a larger one from all four pairs of adjacent chars in the vector.
            // If any of those four chars has a non-ASCII bit set, we have seen non-ASCII data.
            var maxChars = AdvSimd.Arm64.MaxPairwise(utf16Vector, utf16Vector);
            return (maxChars.AsUInt64().ToScalar() & 0xFF80FF80FF80FF80) == 0;
        }
        else
        {
            const ushort asciiMask = ushort.MaxValue - 127; // 0x7F80
            var zeroIsAscii = utf16Vector & Vector128.Create(asciiMask);
            // If a non-ASCII bit is set in any WORD of the vector, we have seen non-ASCII data.
            return zeroIsAscii == Vector128<ushort>.Zero;
        }
    }

    /// Takes 4 ASCII chars in ulong and returns 4 ASCII values
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint NarrowFourUtf16CharsToAscii(ulong value)
    {
        Debug.Assert(AllCharsInUInt64AreAscii(value));

        if (Sse2.X64.IsSupported)
        {
            // Narrows a vector of words [ w0 w1 w2 w3 ] to a vector of bytes
            // [ b0 b1 b2 b3 * * * * ], then returns 4 bytes (32 bits).

            var vecWide = Sse2.X64.ConvertScalarToVector128UInt64(value).AsInt16();
            var vecNarrow = Sse2.PackUnsignedSaturate(vecWide, vecWide).AsUInt32();
            return Sse2.ConvertToUInt32(vecNarrow);
        }
        else if (AdvSimd.IsSupported)
        {
            // Narrows a vector of words [ w0 w1 w2 w3 ] to a vector of bytes
            // [ b0 b1 b2 b3 * * * * ], then returns 4 bytes (32 bits).

            var vecWide = Vector128.CreateScalarUnsafe(value).AsInt16();
            var lower = AdvSimd.ExtractNarrowingSaturateUnsignedLower(vecWide);
            return lower.AsUInt32().ToScalar();
        }
        else
        {
            return (uint)((value & 0x00ff_0000_0000_0000ul) >> 24)
                   | (uint)((value & 0x0000_00ff_0000_0000ul) >> 16)
                   | (uint)((value & 0x0000_0000_00ff_0000ul) >> 8)
                   | (uint)(value & 0x0000_0000_0000_00fful);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong NarrowEightUtf16CharsToAscii(Vector128<ushort> value)
    {
        return NarrowSixteenUtf16CharsToAscii(value, value).AsUInt64().ToScalar();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Vector128<byte> NarrowSixteenUtf16CharsToAscii(Vector128<ushort> vectorFirst,
        Vector128<ushort> vectorSecond)
    {
        // Narrows two vectors of words [ w7 w6 w5 w4 w3 w2 w1 w0 ] and [ w7' w6' w5' w4' w3' w2' w1' w0' ]
        // to a vector of bytes [ b7 ... b0 b7' ... b0'].

        // prefer architecture specific intrinsic as they don't perform additional AND like Vector128.Narrow does
        if (Sse2.IsSupported)
        {
            return Sse2.PackUnsignedSaturate(vectorFirst.AsInt16(), vectorSecond.AsInt16());
        }
        else if (AdvSimd.Arm64.IsSupported)
        {
            return AdvSimd.Arm64.UnzipEven(vectorFirst.AsByte(), vectorSecond.AsByte());
        }
        else
        {
            return Vector128.Narrow(vectorFirst, vectorSecond);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AllBytesInUInt32AreAscii(uint value)
    {
        return (value & 0x80808080u) == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void WidenFourAsciiBytesToUtf16(char* outputBuffer, uint value)
    {
        Debug.Assert(AllBytesInUInt32AreAscii(value));

        if (Sse2.X64.IsSupported)
        {
            Debug.Assert(BitConverter.IsLittleEndian, "SSE2 widening assumes little-endian.");
            var vecNarrow = Sse2.ConvertScalarToVector128UInt32(value).AsByte();
            var vecWide = Sse2.UnpackLow(vecNarrow, Vector128<byte>.Zero).AsUInt64();
            Unsafe.WriteUnaligned(outputBuffer, Sse2.X64.ConvertToUInt64(vecWide));
        }
        else if (AdvSimd.Arm64.IsSupported)
        {
            var vecNarrow = AdvSimd.DuplicateToVector128(value).AsByte();
            var vecWide = AdvSimd.Arm64.ZipLow(vecNarrow, Vector128<byte>.Zero).AsUInt64();
            Unsafe.WriteUnaligned(outputBuffer, vecWide.ToScalar());
        }
        else
        {
            if (BitConverter.IsLittleEndian)
            {
                outputBuffer[0] = (char)(byte)value;
                value >>= 8;
                outputBuffer[1] = (char)(byte)value;
                value >>= 8;
                outputBuffer[2] = (char)(byte)value;
                value >>= 8;
                outputBuffer[3] = (char)value;
            }
            else
            {
                outputBuffer[3] = (char)(byte)value;
                value >>= 8;
                outputBuffer[2] = (char)(byte)value;
                value >>= 8;
                outputBuffer[1] = (char)(byte)value;
                value >>= 8;
                outputBuffer[0] = (char)value;
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static bool ContainsNonAsciiByte(Vector128<byte> value)
    {
        if (!AdvSimd.Arm64.IsSupported)
        {
            throw new PlatformNotSupportedException();
        }

        value = AdvSimd.Arm64.MaxPairwise(value, value);
        return (value.AsUInt64().ToScalar() & 0x8080808080808080) != 0;
    }

    public static unsafe nuint WidenAsciiToUtf16Simd(byte* pAsciiBuffer, char* pUtf16Buffer, nuint elementCount)
    {
        var SizeOfVector128 = (uint)Unsafe.SizeOf<Vector128<byte>>();
        nuint MaskOfAllBitsInVector128 = SizeOfVector128 - 1;

        // This method is written such that control generally flows top-to-bottom, avoiding
        // jumps as much as possible in the optimistic case of "all ASCII". If we see non-ASCII
        // data, we jump out of the hot paths to targets at the end of the method.

        Debug.Assert(Sse2.IsSupported || AdvSimd.Arm64.IsSupported);
        Debug.Assert(BitConverter.IsLittleEndian);
        Debug.Assert(elementCount >= 2 * SizeOfVector128);

        // We're going to get the best performance when we have aligned writes, so we'll take the
        // hit of potentially unaligned reads in order to hit this sweet spot.

        Vector128<byte> asciiVector;
        Vector128<byte> utf16FirstHalfVector;
        bool containsNonAsciiBytes;

        // First, perform an unaligned read of the first part of the input buffer.

        if (Sse2.IsSupported)
        {
            asciiVector = Sse2.LoadVector128(pAsciiBuffer); // unaligned load
            containsNonAsciiBytes = (uint)Sse2.MoveMask(asciiVector) != 0;
        }
        else if (AdvSimd.Arm64.IsSupported)
        {
            asciiVector = AdvSimd.LoadVector128(pAsciiBuffer);
            containsNonAsciiBytes = ContainsNonAsciiByte(asciiVector);
        }
        else
        {
            throw new PlatformNotSupportedException();
        }

        // If there's non-ASCII data in the first 8 elements of the vector, there's nothing we can do.

        if (containsNonAsciiBytes)
        {
            return 0;
        }

        // Then perform an unaligned write of the first part of the input buffer.

        var zeroVector = Vector128<byte>.Zero;

        if (Sse2.IsSupported)
        {
            utf16FirstHalfVector = Sse2.UnpackLow(asciiVector, zeroVector);
            Sse2.Store((byte*)pUtf16Buffer, utf16FirstHalfVector); // unaligned
        }
        else if (AdvSimd.IsSupported)
        {
            utf16FirstHalfVector = AdvSimd.ZeroExtendWideningLower(asciiVector.GetLower()).AsByte();
            AdvSimd.Store((byte*)pUtf16Buffer, utf16FirstHalfVector); // unaligned
        }
        else
        {
            throw new PlatformNotSupportedException();
        }

        // Calculate how many elements we wrote in order to get pOutputBuffer to its next alignment
        // point, then use that as the base offset going forward. Remember the >> 1 to account for
        // that we wrote chars, not bytes. This means we may re-read data in the next iteration of
        // the loop, but this is ok.

        var currentOffset = (SizeOfVector128 >> 1) - (((nuint)pUtf16Buffer >> 1) & (MaskOfAllBitsInVector128 >> 1));
        Debug.Assert(0 < currentOffset && currentOffset <= SizeOfVector128 / sizeof(char));

        var finalOffsetWhereCanRunLoop = elementCount - SizeOfVector128;

        // Calculating the destination address outside the loop results in significant
        // perf wins vs. relying on the JIT to fold memory addressing logic into the
        // write instructions. See: https://github.com/dotnet/runtime/issues/33002

        var pCurrentWriteAddress = pUtf16Buffer + currentOffset;

        do
        {
            // In a loop, perform an unaligned read, widen to two vectors, then aligned write the two vectors.

            if (Sse2.IsSupported)
            {
                asciiVector = Sse2.LoadVector128(pAsciiBuffer + currentOffset); // unaligned load
                containsNonAsciiBytes = (uint)Sse2.MoveMask(asciiVector) != 0;
            }
            else if (AdvSimd.Arm64.IsSupported)
            {
                asciiVector = AdvSimd.LoadVector128(pAsciiBuffer + currentOffset);
                containsNonAsciiBytes = ContainsNonAsciiByte(asciiVector);
            }
            else
            {
                throw new PlatformNotSupportedException();
            }

            if (containsNonAsciiBytes)
            {
                break;
            }

            if (Sse2.IsSupported)
            {
                var low = Sse2.UnpackLow(asciiVector, zeroVector);
                Sse2.StoreAligned((byte*)pCurrentWriteAddress, low);

                var high = Sse2.UnpackHigh(asciiVector, zeroVector);
                Sse2.StoreAligned((byte*)pCurrentWriteAddress + SizeOfVector128, high);
            }
            else if (AdvSimd.Arm64.IsSupported)
            {
                var low = AdvSimd.ZeroExtendWideningLower(asciiVector.GetLower());
                var high = AdvSimd.ZeroExtendWideningUpper(asciiVector);
                AdvSimd.Arm64.StorePair((ushort*)pCurrentWriteAddress, low, high);
            }
            else
            {
                throw new PlatformNotSupportedException();
            }

            currentOffset += SizeOfVector128;
            pCurrentWriteAddress += SizeOfVector128;
        } while (currentOffset <= finalOffsetWhereCanRunLoop);

        return currentOffset;
    }

    public static unsafe nuint SkipAsciiToUtf16Simd(byte* pAsciiBuffer, nuint elementCount)
    {
        var SizeOfVector128 = (uint)Unsafe.SizeOf<Vector128<byte>>();

        Debug.Assert(Sse2.IsSupported || AdvSimd.Arm64.IsSupported);
        Debug.Assert(BitConverter.IsLittleEndian);
        Debug.Assert(elementCount >= SizeOfVector128);

        nuint processed = 0;
        while (processed + SizeOfVector128 <= elementCount)
        {
            Vector128<byte> asciiVector;
            bool containsNonAsciiBytes;
            if (Sse2.IsSupported)
            {
                asciiVector = Sse2.LoadVector128(pAsciiBuffer); // unaligned load
                containsNonAsciiBytes = (uint)Sse2.MoveMask(asciiVector) != 0;
            }
            else if (AdvSimd.Arm64.IsSupported)
            {
                asciiVector = AdvSimd.LoadVector128(pAsciiBuffer);
                containsNonAsciiBytes = ContainsNonAsciiByte(asciiVector);
            }
            else
            {
                throw new PlatformNotSupportedException();
            }

            // If there's non-ASCII data in the first 16 elements of the vector, there's nothing we can do.

            if (containsNonAsciiBytes)
            {
                return processed;
            }

            pAsciiBuffer += 16;
            processed += 16;
        }

        return processed;
    }

    public static Vector128<ushort> Add1Saturate(Vector128<ushort> value)
    {
        if (Sse2.IsSupported)
        {
            return Sse2.AddSaturate(value, Vector128.Create((ushort)1));
        }
        else if (AdvSimd.IsSupported)
        {
            return AdvSimd.AddSaturate(value, Vector128.Create((ushort)1));
        }
        else
        {
            throw new PlatformNotSupportedException();
        }
    }

    public static bool AreAllBytes1to127(uint value)
    {
        return (value & 0x80808080) == 0 && ((value + 0x7f7f7f7f) & 0x80808080) == 0x80808080;
    }

    public static (nuint Count, bool WasEnd) DetectLengthOfSimpleCharacters(uint value)
    {
        Debug.Assert(BitConverter.IsLittleEndian);
        var hb = value & 0x80808080;
        if (hb == 0)
        {
            var dz = (value + 0x7f7f7f7f) & 0x80808080;
            if (dz == 0x80808080) return (4, false);
            dz ^= 0x80808080;
            return (((uint)BitOperations.TrailingZeroCount(dz) >> 3) + 1, true);
        }

        if ((value & 0x80) == 0x80) return (0, false);
        var hbc = (uint)BitOperations.TrailingZeroCount(hb) >> 3;
        var dzhb = ((value + 0x7f7f7f7f) & 0x80808080 | hb) ^ 0x80808080;
        var dzc = ((uint)BitOperations.TrailingZeroCount(dzhb) >> 3) + 1;
        if (dzc > hbc) return (hbc, false);
        return (dzc, true);
    }

    public static bool AreAllBytes1to127(ulong value)
    {
        return (value & 0x80808080_80808080ul) == 0 &&
               ((value + 0x7f7f7f7f_7f7f7f7ful) & 0x80808080_80808080ul) == 0x80808080_80808080ul;
    }

    public static (nuint Count, bool WasEnd) DetectLengthOfSimpleCharacters(ulong value)
    {
        Debug.Assert(BitConverter.IsLittleEndian);
        var hb = value & 0x80808080_80808080ul;
        if (hb == 0)
        {
            var dz = (value + 0x7f7f7f7f_7f7f7f7ful) & 0x80808080_80808080ul;
            if (dz == 0x80808080_80808080ul) return (8, false);
            dz ^= 0x80808080_80808080ul;
            return (((uint)BitOperations.TrailingZeroCount(dz) >> 3) + 1, true);
        }

        if ((value & 0x80) == 0x80) return (0, false);
        var hbc = (uint)BitOperations.TrailingZeroCount(hb) >> 3;
        var dzhb = ((value + 0x7f7f7f7f_7f7f7f7ful) & 0x80808080_80808080ul | hb) ^ 0x80808080_80808080ul;
        var dzc = ((uint)BitOperations.TrailingZeroCount(dzhb) >> 3) + 1;
        if (dzc > hbc) return (hbc, false);
        return (dzc, true);
    }

    public static bool AreAllBytes1to127(Vector128<byte> value)
    {
        var all80 = Vector128.Create((byte)0x80);
        return (value & all80) == Vector128<byte>.Zero && ((value + Vector128.Create((byte)0x7f)) & all80) == all80;
    }

    public static (nuint Count, bool WasEnd) DetectLengthOfSimpleCharacters(Vector128<byte> value)
    {
        Debug.Assert(BitConverter.IsLittleEndian);
        var all80 = Vector128.Create((byte)0x80);
        var hb = value & all80;
        if (hb == Vector128<byte>.Zero)
        {
            var dz = (value + Vector128.Create((byte)0x7f)) & all80;
            if (dz == all80) return (16, false);
            dz ^= all80;
            return ((nuint)BitOperations.TrailingZeroCount(dz.ExtractMostSignificantBits()) + 1, true);
        }

        var hbc = (uint)BitOperations.TrailingZeroCount(hb.ExtractMostSignificantBits());
        if (hbc == 0) return (0, false);
        var dzhb = ((value + Vector128.Create((byte)0x7f)) & all80 | hb) ^ all80;
        var dzc = (uint)BitOperations.TrailingZeroCount(dzhb.ExtractMostSignificantBits()) + 1;
        if (dzc > hbc) return (hbc, false);
        return (dzc, true);
    }

    public static (nuint Count, bool WasEnd) DetectLengthOfSimpleCharacters(ReadOnlySpan<byte> span)
    {
        nuint count = 0;
        var len = (nuint)span.Length;
        ref var ptr = ref MemoryMarshal.GetReference(span);
        if (BitConverter.IsLittleEndian) goto last;
        lastBytes:
        while (len > 0)
        {
            if (ptr == 0) return (count + 1, true);
            if (ptr >= 0x80) return (count, false);
            count++;
            len--;
            ptr = ref Unsafe.Add(ref ptr, 1);
        }

        return (count, false);
        last:
        if (len < 4) goto lastBytes;
        if (!AreAllBytes1to127(Unsafe.ReadUnaligned<uint>(ref ptr)) || len < 8)
        {
            var skip = DetectLengthOfSimpleCharacters(Unsafe.ReadUnaligned<uint>(ref ptr));
            count += skip.Count;
            if (skip.WasEnd) return (count, true);
            len -= skip.Count;
            ptr = ref Unsafe.Add(ref ptr, skip.Count);
            goto lastBytes;
        }

        if (len < 16)
        {
            var skip = DetectLengthOfSimpleCharacters(Unsafe.ReadUnaligned<ulong>(ref ptr));
            count += skip.Count;
            if (skip.WasEnd) return (count, true);
            len -= skip.Count;
            ptr = ref Unsafe.Add(ref ptr, skip.Count);
            goto lastBytes;
        }

        if (Vector128.IsHardwareAccelerated)
        {
            var all80 = Vector128.Create((byte)0x80);
            var all7f = Vector128.Create((byte)0x7f);
            while (len >= 16)
            {
                var value = Vector128.LoadUnsafe(ref ptr);
                var hb = value & all80;
                if (hb == Vector128<byte>.Zero)
                {
                    var dz = (value + all7f) & all80;
                    if (dz == all80)
                    {
                        ptr = ref Unsafe.Add(ref ptr, 16);
                        count += 16;
                        len -= 16;
                        continue;
                    }

                    dz ^= all80;
                    return (count + (nuint)BitOperations.TrailingZeroCount(dz.ExtractMostSignificantBits()) + 1, true);
                }

                var hbc = (uint)BitOperations.TrailingZeroCount(hb.ExtractMostSignificantBits());
                if (hbc == 0) return (count, false);
                var dzhb = ((value + all7f) & all80 | hb) ^ all80;
                var dzc = (uint)BitOperations.TrailingZeroCount(dzhb.ExtractMostSignificantBits()) + 1;
                if (dzc > hbc) return (count + hbc, false);
                return (count + dzc, true);
            }
        }

        while (len >= 8)
        {
            if (!AreAllBytes1to127(Unsafe.ReadUnaligned<ulong>(ref ptr))) break;

            ptr = ref Unsafe.Add(ref ptr, 8);
            count += 8;
            len -= 8;
        }

        goto last;
    }

    public static unsafe (nuint Count, bool WasEnd) ReadAndExpandSimpleCharacters(ReadOnlySpan<byte> span,
        ref Span<char> targetBuf, ref uint targetOfs)
    {
        nuint count = 0;
        var len = (nuint)span.Length;
        ref var ptr = ref MemoryMarshal.GetReference(span);
        if (BitConverter.IsLittleEndian || !Sse2.IsSupported) goto last;
        lastBytes:
        while (len > 0)
        {
            if (ptr == 0) return (count + 1, true);
            if (ptr >= 0x80) return (count, false);
            if (targetBuf.Length <= targetOfs)
            {
                var newCharBuf = (Span<char>)new char[targetBuf.Length * 2];
                targetBuf.CopyTo(newCharBuf);
                targetBuf = newCharBuf;
            }

            targetBuf[(int)targetOfs++] = (char)(ptr - 1);
            count++;
            len--;
            ptr = ref Unsafe.Add(ref ptr, 1);
        }

        return (count, false);
        last:
        if (len < 4) goto lastBytes;
        if (!AreAllBytes1to127(Unsafe.ReadUnaligned<uint>(ref ptr)) || len < 8)
        {
            goto lastBytes;
        }


        if (Vector128.IsHardwareAccelerated && Sse2.IsSupported)
        {
            var all80 = Vector128.Create((byte)0x80);
            var all7f = Vector128.Create((byte)0x7f);
            var all01 = Vector128.Create((byte)0x01);
            var all00 = Vector128<byte>.Zero;

            while (len >= 16)
            {
                if (targetBuf.Length < targetOfs + 16)
                {
                    var newCharBuf = (Span<char>)new char[targetBuf.Length * 2];
                    targetBuf.CopyTo(newCharBuf);
                    targetBuf = newCharBuf;
                }

                var value = Vector128.LoadUnsafe(ref ptr);

                // It is ok to do more work than needed => buffer will be cropped by actual used size
                var finalValue = Sse2.SubtractSaturate(value, all01);
                var low = Sse2.UnpackLow(finalValue, all00);
                Sse2.Store(
                    (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(targetBuf)) + 2 * targetOfs,
                    low);

                var high = Sse2.UnpackHigh(finalValue, all00);
                Sse2.Store(
                    (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(targetBuf)) + 2 * targetOfs + 16,
                    high);

                var hb = value & all80;
                if (hb == Vector128<byte>.Zero)
                {
                    var dz = (value + all7f) & all80;
                    if (dz == all80)
                    {
                        targetOfs += 16;
                        ptr = ref Unsafe.Add(ref ptr, 16);
                        count += 16;
                        len -= 16;
                        continue;
                    }

                    dz ^= all80;
                    var bytesUntilZero = (nuint)BitOperations.TrailingZeroCount(dz.ExtractMostSignificantBits());
                    targetOfs += (uint)bytesUntilZero;
                    return (count + bytesUntilZero + 1, true);
                }

                var hbc = (uint)BitOperations.TrailingZeroCount(hb.ExtractMostSignificantBits());
                if (hbc == 0) return (count, false);
                var dzhb = ((value + all7f) & all80 | hb) ^ all80;
                var dzc = (uint)BitOperations.TrailingZeroCount(dzhb.ExtractMostSignificantBits()) + 1;
                if (dzc > hbc)
                {
                    targetOfs += hbc;
                    return (count + hbc, false);
                }

                targetOfs += dzc - 1;
                return (count + dzc, true);
            }

            if (len >= 8)
            {
                if (targetBuf.Length < targetOfs + 8)
                {
                    var newCharBuf = (Span<char>)new char[targetBuf.Length * 2];
                    targetBuf.CopyTo(newCharBuf);
                    targetBuf = newCharBuf;
                }

                var value = Unsafe.ReadUnaligned<ulong>(ref ptr);

                // It is ok to do more work than needed => buffer will be cropped by actual used size
                var finalValue = Sse2.SubtractSaturate(Vector128.Create<ulong>(value).AsByte(), all01);
                var low = Sse2.UnpackLow(finalValue, all00);
                Sse2.Store(
                    (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(targetBuf)) + 2 * targetOfs,
                    low);

                var skip = DetectLengthOfSimpleCharacters(value);
                count += skip.Count;
                if (skip.WasEnd)
                {
                    targetOfs += (uint)(skip.Count - 1);
                    return (count, true);
                }

                targetOfs += (uint)skip.Count;
                len -= skip.Count;
                ptr = ref Unsafe.Add(ref ptr, skip.Count);
            }
        }

        goto lastBytes;
    }
}
