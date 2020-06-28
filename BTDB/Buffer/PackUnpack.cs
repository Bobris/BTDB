using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.Buffer
{
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

        // This pattern is optimized by Roslyn that it place these data into constant data segment in executable
        static ReadOnlySpan<byte> LzcToVUintLen => new byte[ /*65*/]
        {
            9, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5,
            4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1
        };

        public static int LengthVUInt(uint value)
        {
            /* Logically doing commented code, but branch less => much faster
            if (value < 0x80) return 1;
            if (value < 0x4000) return 2;
            if (value < 0x200000) return 3;
            if (value < 0x10000000) return 4;
            return 5;
            */
            return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVUintLen),
                (IntPtr) (32 + BitOperations.LeadingZeroCount(value)));
        }

        public static int LengthVUInt(ulong value)
        {
            /* Logically doing commented code, but branch less => much faster
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
            return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVUintLen),
                (IntPtr) BitOperations.LeadingZeroCount(value));
        }

        public static int LengthVUInt(byte[] data, int ofs)
        {
            var first = data[ofs];
            return LengthVUIntByFirstByte(first);
        }

        public static int LengthVUIntByFirstByte(byte first)
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
            return BitOperations.LeadingZeroCount(first ^ 0xffu) + 9 - 32;
        }

        public static void UnsafePackVUInt(ref byte data, ulong value, int len)
        {
            Debug.Assert(LengthVUInt(value) == len);
            switch (len)
            {
                case 1:
                {
                    data = (byte) value;
                    return;
                }
                case 2:
                {
                    value = 0x8000u + value;
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort) value));
                    return;
                }
                case 3:
                {
                    data = (byte) (0xC0 + (value >> 16));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort) value));
                    return;
                }
                case 4:
                {
                    value = 0xE0000000u + value;
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
                    return;
                }
                case 5:
                {
                    data = (byte) (0xF0 + (value >> 32));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
                    return;
                }
                case 6:
                {
                    var hiValue = (ushort) (0xF800u + (value >> 32));
                    Unsafe.WriteUnaligned(ref data, AsBigEndian(hiValue));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
                    return;
                }
                case 7:
                {
                    data = (byte) (0xFC + (value >> 48));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    var hiValue = (ushort) (value >> 32);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian(hiValue));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
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
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian(value));
                    return;
                }
            }
        }

        public static void PackVUInt(byte[] data, ref int ofs, uint value)
        {
            PackVUInt(data, ref ofs, (ulong) value);
        }

        public static void PackVUInt(byte[] data, ref int ofs, ulong value)
        {
            var len = LengthVUInt(value);
            if (data.Length < ofs + len) throw new IndexOutOfRangeException();
            UnsafePackVUInt(ref data[ofs], value, len);
            ofs += len;
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

        public static ulong UnsafeUnpackVUInt(ref byte data, int len)
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
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data));
                }
                case 4:
                    return 0x0fff_ffffu & AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
                case 5:
                {
                    var res = (ulong) (data & 0x07u) << 32;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
                }
                case 6:
                {
                    var res = (0x03fful & AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data))) << 32;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
                }
                case 7:
                {
                    var res = (ulong) (data & 0x01u) << 48;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    res += (ulong) AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data)) << 32;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data));
                }
                case 8:
                    return 0x00ff_ffff_ffff_fffful & AsBigEndian(Unsafe.ReadUnaligned<ulong>(ref data));
                case 9:
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    return AsBigEndian(Unsafe.ReadUnaligned<ulong>(ref data));
            }
        }

        public static ulong UnpackVUInt(byte[] data, ref int ofs)
        {
            var first = data[ofs];
            var len = LengthVUIntByFirstByte(first);
            if (data.Length < ofs + len) throw new IndexOutOfRangeException();
            // All range checks were done already before, so now do it without them for speed
            var res = UnsafeUnpackVUInt(
                ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(data.AsSpan()), (IntPtr) ofs), len);
            ofs += len;
            return res;
        }

        static ReadOnlySpan<byte> LzcToVIntLen => new byte[65]
        {
            9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5,
            5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1
        };

        public static int LengthVInt(int value)
        {
            /* Logically doing commented code, but branch less => much faster
            if (-0x40 <= value && value < 0x40) return 1;
            if (-0x2000 <= value && value < 0x2000) return 2;
            if (-0x100000 <= value && value < 0x100000) return 3;
            if (-0x08000000 <= value && value < 0x08000000) return 4;
            return 5;
            */
            value ^= value >> 31; // Convert negative value to -value-1 and don't touch zero or positive
            return Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(LzcToVIntLen),
                (IntPtr) 32 + BitOperations.LeadingZeroCount((uint) value));
        }

        public static int LengthVInt(long value)
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
                (IntPtr) BitOperations.LeadingZeroCount((ulong) value));
        }

        public static int LengthVInt(byte[] data, int ofs)
        {
            var first = data[ofs];
            return LengthVIntByFirstByte(first);
        }

        public static int LengthVIntByFirstByte(uint first)
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
            first ^= (uint) ((sbyte) first >> 7) & 0xff;
            var res = BitOperations.LeadingZeroCount(first) + 8 - 32;
            return (int) (0x976543211UL >> (res * 4)) & 0xf;
        }

        public static void UnsafePackVInt(ref byte data, long value, int len)
        {
            Debug.Assert(LengthVInt(value) == len);
            var sign = value >> 63;
            switch (len)
            {
                case 1:
                {
                    data = (byte) (value + 0x80);
                    return;
                }
                case 2:
                {
                    value = 0xC000u + value;
                    value ^= sign & 0x8000;
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort) value));
                    return;
                }
                case 3:
                {
                    value = 0xE00000u + value;
                    value ^= sign & 0xC00000u;
                    data = (byte) ((ulong) value >> 16);
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort) value));
                    return;
                }
                case 4:
                {
                    value = 0xF000_0000u + value;
                    value ^= sign & 0xE000_0000;
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
                    return;
                }
                case 5:
                {
                    value = 0xF8_0000_0000L + value;
                    value ^= sign & 0xF0_0000_0000;
                    data = (byte) ((ulong) value >> 32);
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
                    return;
                }
                case 6:
                {
                    value = 0xFC00_0000_0000L + value;
                    value ^= sign & 0xF800_0000_0000;
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort) ((ulong) value >> 32)));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
                    return;
                }
                case 7:
                {
                    value = 0xFE_0000_0000_0000L + value;
                    value ^= sign & 0xFC_0000_0000_0000;
                    data = (byte) ((ulong) value >> 48);
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((ushort) ((ulong) value >> 32)));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((uint) value));
                    return;
                }
                default: // It always 9
                {
                    data = (byte) ((sign & 0xFF) ^ 0xFF);
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data, AsBigEndian((ulong) value));
                    return;
                }
            }
        }

        public static void PackVInt(byte[] data, ref int ofs, long value)
        {
            var len = LengthVInt(value);
            if (data.Length < ofs + len) throw new IndexOutOfRangeException();
            UnsafePackVInt(ref data[ofs], value, len);
            ofs += len;
        }

        public static long UnsafeUnpackVInt(ref byte data, int len)
        {
            switch (len)
            {
                case 1:
                    return (long) data - 0x80;
                case 2:
                {
                    var sign = (long) ((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                    return (0x1fffu & AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data))) - (0x2000 & sign);
                }
                case 3:
                {
                    var sign = (long) ((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                    var res = (data & 0x0F) << 16;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data)) - (0x10_0000 & sign);
                }
                case 4:
                {
                    var sign = (long) ((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                    return (0x07ff_ffff & AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data))) - (0x0800_0000 & sign);
                }
                case 5:
                {
                    var sign = (long) ((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                    var res = (long) (data & 0x03u) << 32;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data)) - (0x04_0000_0000 & sign);
                }
                case 6:
                {
                    var sign = (long) ((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                    var res = (0x01ffL & AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data))) << 32;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data)) - (0x0200_0000_0000 & sign);
                }
                case 7:
                {
                    var sign = (long) ((data & 0x80) >> 7) - 1; // -1 for negative, 0 for positive
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    var res = (long) AsBigEndian(Unsafe.ReadUnaligned<ushort>(ref data)) << 32;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    return res + AsBigEndian(Unsafe.ReadUnaligned<uint>(ref data)) - (0x01_0000_0000_0000 & sign);
                }
                default:
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    return (long) AsBigEndian(Unsafe.ReadUnaligned<ulong>(ref data));
            }
        }

        public static long UnpackVInt(byte[] data, ref int ofs)
        {
            var first = data[ofs];
            var len = LengthVIntByFirstByte(first);
            if (data.Length < ofs + len) throw new IndexOutOfRangeException();
            // All range checks were done already before, so now do it without them for speed
            var res = UnsafeUnpackVInt(
                ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(data.AsSpan()), (IntPtr) ofs), len);
            ofs += len;
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
            p = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AddByteOffset(ref res, (IntPtr) delta), p.Length - delta);
            return ref res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref byte UnsafeGetAndAdvance(ref Span<byte> p, int delta)
        {
            ref var res = ref MemoryMarshal.GetReference(p);
            p = MemoryMarshal.CreateSpan(ref Unsafe.AddByteOffset(ref res, (IntPtr) delta), p.Length - delta);
            return ref res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeAdvance(ref ReadOnlySpan<byte> p, int delta)
        {
            p = MemoryMarshal.CreateReadOnlySpan(
                ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(p), (IntPtr) delta), p.Length - delta);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnsafeAdvance(ref Span<byte> p, int delta)
        {
            p = MemoryMarshal.CreateSpan(
                ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(p), (IntPtr) delta), p.Length - delta);
        }
    }
}
