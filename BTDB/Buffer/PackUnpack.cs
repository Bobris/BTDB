using System;
using System.Buffers.Binary;
using System.Diagnostics;
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
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness((ushort) value)
                            : (ushort) value);
                    return;
                }
                case 3:
                {
                    data = (byte) (0xC0 + (value >> 16));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness((ushort) value)
                            : (ushort) value);
                    return;
                }
                case 4:
                {
                    value = 0xE0000000u + value;
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness((uint) value)
                            : (uint) value);
                    return;
                }
                case 5:
                {
                    data = (byte) (0xF0 + (value >> 32));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness((uint) value)
                            : (uint) value);
                    return;
                }
                case 6:
                {
                    var hiValue = (ushort) (0xF800u + (value >> 32));
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness(hiValue)
                            : hiValue);
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness((uint) value)
                            : (uint) value);
                    return;
                }
                case 7:
                {
                    data = (byte) (0xFC + (value >> 48));
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    var hiValue = (ushort) (value >> 32);
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness(hiValue)
                            : hiValue);
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 2);
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness((uint) value)
                            : (uint) value);
                    return;
                }
                case 8:
                {
                    value += 0xFE00_0000_0000_0000ul;
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness(value)
                            : value);
                    return;
                }
                default:
                {
                    data = 0xFF;
                    data = ref Unsafe.AddByteOffset(ref data, (IntPtr) 1);
                    Unsafe.WriteUnaligned(ref data,
                        BitConverter.IsLittleEndian
                            ? BinaryPrimitives.ReverseEndianness(value)
                            : value);
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
            if (data.Length < ofs + len) throw new AccessViolationException();
            UnsafePackVUInt(ref data[ofs], value, len);
            ofs += len;
        }

        public static ulong UnpackVUInt(byte[] data, ref int ofs)
        {
            uint first = data[ofs];
            ofs++;
            if (first < 0x80) return first;
            ulong result;
            if (first < 0xC0)
            {
                result = ((first & 0x3F) << 8) + data[ofs];
                ofs++;
                return result;
            }

            if (first < 0xE0)
            {
                result = ((first & 0x1Fu) << 16) + ((uint) data[ofs] << 8) + data[ofs + 1];
                ofs += 2;
                return result;
            }

            if (first < 0xF0)
            {
                result = ((first & 0x0Fu) << 24) + ((uint) data[ofs] << 16) + ((uint) data[ofs + 1] << 8) +
                         data[ofs + 2];
                ofs += 3;
                return result;
            }

            if (first < 0xF8)
            {
                result = ((ulong) (first & 0x07u) << 32) + ((uint) data[ofs] << 24) + ((uint) data[ofs + 1] << 16)
                         + ((uint) data[ofs + 2] << 8) + data[ofs + 3];
                ofs += 4;
                return result;
            }

            if (first < 0xFC)
            {
                result = ((ulong) (first & 0x03u) << 40) + ((ulong) data[ofs] << 32) + ((uint) data[ofs + 1] << 24)
                         + ((uint) data[ofs + 2] << 16) + ((uint) data[ofs + 3] << 8) + data[ofs + 4];
                ofs += 5;
                return result;
            }

            if (first < 0xFE)
            {
                result = ((ulong) (first & 0x01u) << 48) + ((ulong) data[ofs] << 40) + ((ulong) data[ofs + 1] << 32)
                         + ((uint) data[ofs + 2] << 24) + ((uint) data[ofs + 3] << 16) + ((uint) data[ofs + 4] << 8) +
                         data[ofs + 5];
                ofs += 6;
                return result;
            }

            if (first == 0xFE)
            {
                result = ((ulong) data[ofs] << 48) + ((ulong) data[ofs + 1] << 40) + ((ulong) data[ofs + 2] << 32)
                         + ((uint) data[ofs + 3] << 24) + ((uint) data[ofs + 4] << 16) + ((uint) data[ofs + 5] << 8) +
                         data[ofs + 6];
                ofs += 7;
                return result;
            }

            result = ((ulong) data[ofs] << 56) + ((ulong) data[ofs + 1] << 48) + ((ulong) data[ofs + 2] << 40) +
                     ((ulong) data[ofs + 3] << 32)
                     + ((uint) data[ofs + 4] << 24) + ((uint) data[ofs + 5] << 16) + ((uint) data[ofs + 6] << 8) +
                     data[ofs + 7];
            ofs += 8;
            return result;
        }

        public static int LengthVInt(int value)
        {
            if (-0x40 <= value && value < 0x40) return 1;
            if (-0x2000 <= value && value < 0x2000) return 2;
            if (-0x100000 <= value && value < 0x100000) return 3;
            if (-0x08000000 <= value && value < 0x08000000) return 4;
            return 5;
        }

        public static int LengthVInt(long value)
        {
            if (-0x40 <= value && value < 0x40) return 1;
            if (-0x2000 <= value && value < 0x2000) return 2;
            if (-0x100000 <= value && value < 0x100000) return 3;
            if (-0x08000000 <= value && value < 0x08000000) return 4;
            if (-0x0400000000 <= value && value < 0x0400000000) return 5;
            if (-0x020000000000 <= value && value < 0x020000000000) return 6;
            if (-0x01000000000000 <= value && value < 0x01000000000000) return 7;
            return 9;
        }

        public static int LengthVInt(byte[] data, int ofs)
        {
            uint first = data[ofs];
            if (0x40 <= first && first < 0xC0) return 1;
            if (0x20 <= first && first < 0xE0) return 2;
            if (0x10 <= first && first < 0xF0) return 3;
            if (0x08 <= first && first < 0xF8) return 4;
            if (0x04 <= first && first < 0xFC) return 5;
            if (0x02 <= first && first < 0xFE) return 6;
            if (0x01 <= first && first < 0xFF) return 7;
            return 9;
        }

        public static void PackVInt(byte[] data, ref int ofs, long value)
        {
            if (-0x40 <= value && value < 0x40)
            {
                data[ofs] = (byte) (value + 0x80);
                ofs++;
                return;
            }

            if (value >= 0)
            {
                if (value < 0x2000)
                {
                    data[ofs] = (byte) (0xC0 + (value >> 8));
                    data[ofs + 1] = unchecked((byte) value);
                    ofs += 2;
                    return;
                }

                if (value < 0x100000)
                {
                    data[ofs] = (byte) (0xE0 + (value >> 16));
                    data[ofs + 1] = unchecked((byte) (value >> 8));
                    data[ofs + 2] = unchecked((byte) value);
                    ofs += 3;
                    return;
                }

                if (value < 0x08000000)
                {
                    data[ofs] = (byte) (0xF0 + (value >> 24));
                    data[ofs + 1] = unchecked((byte) (value >> 16));
                    data[ofs + 2] = unchecked((byte) (value >> 8));
                    data[ofs + 3] = unchecked((byte) value);
                    ofs += 4;
                    return;
                }

                if (value < 0x0400000000)
                {
                    data[ofs] = (byte) (0xF8 + (value >> 32));
                    data[ofs + 1] = unchecked((byte) (value >> 24));
                    data[ofs + 2] = unchecked((byte) (value >> 16));
                    data[ofs + 3] = unchecked((byte) (value >> 8));
                    data[ofs + 4] = unchecked((byte) value);
                    ofs += 5;
                    return;
                }

                if (value < 0x020000000000)
                {
                    data[ofs] = (byte) (0xFC + (value >> 40));
                    data[ofs + 1] = unchecked((byte) (value >> 32));
                    data[ofs + 2] = unchecked((byte) (value >> 24));
                    data[ofs + 3] = unchecked((byte) (value >> 16));
                    data[ofs + 4] = unchecked((byte) (value >> 8));
                    data[ofs + 5] = unchecked((byte) value);
                    ofs += 6;
                    return;
                }

                if (value < 0x01000000000000)
                {
                    data[ofs] = 0xFE;
                    data[ofs + 1] = unchecked((byte) (value >> 40));
                    data[ofs + 2] = unchecked((byte) (value >> 32));
                    data[ofs + 3] = unchecked((byte) (value >> 24));
                    data[ofs + 4] = unchecked((byte) (value >> 16));
                    data[ofs + 5] = unchecked((byte) (value >> 8));
                    data[ofs + 6] = unchecked((byte) value);
                    ofs += 7;
                    return;
                }

                data[ofs] = 0xFF;
            }
            else
            {
                if (value >= -0x2000)
                {
                    data[ofs] = (byte) (0x40 + (value >> 8));
                    data[ofs + 1] = unchecked((byte) value);
                    ofs += 2;
                    return;
                }

                if (value >= -0x100000)
                {
                    data[ofs] = (byte) (0x20 + (value >> 16));
                    data[ofs + 1] = unchecked((byte) (value >> 8));
                    data[ofs + 2] = unchecked((byte) value);
                    ofs += 3;
                    return;
                }

                if (value >= -0x08000000)
                {
                    data[ofs] = (byte) (0x10 + (value >> 24));
                    data[ofs + 1] = unchecked((byte) (value >> 16));
                    data[ofs + 2] = unchecked((byte) (value >> 8));
                    data[ofs + 3] = unchecked((byte) value);
                    ofs += 4;
                    return;
                }

                if (value >= -0x0400000000)
                {
                    data[ofs] = (byte) (0x08 + (value >> 32));
                    data[ofs + 1] = unchecked((byte) (value >> 24));
                    data[ofs + 2] = unchecked((byte) (value >> 16));
                    data[ofs + 3] = unchecked((byte) (value >> 8));
                    data[ofs + 4] = unchecked((byte) value);
                    ofs += 5;
                    return;
                }

                if (value >= -0x020000000000)
                {
                    data[ofs] = (byte) (0x04 + (value >> 40));
                    data[ofs + 1] = unchecked((byte) (value >> 32));
                    data[ofs + 2] = unchecked((byte) (value >> 24));
                    data[ofs + 3] = unchecked((byte) (value >> 16));
                    data[ofs + 4] = unchecked((byte) (value >> 8));
                    data[ofs + 5] = unchecked((byte) value);
                    ofs += 6;
                    return;
                }

                if (value >= -0x01000000000000)
                {
                    data[ofs] = 0x01;
                    data[ofs + 1] = unchecked((byte) (value >> 40));
                    data[ofs + 2] = unchecked((byte) (value >> 32));
                    data[ofs + 3] = unchecked((byte) (value >> 24));
                    data[ofs + 4] = unchecked((byte) (value >> 16));
                    data[ofs + 5] = unchecked((byte) (value >> 8));
                    data[ofs + 6] = unchecked((byte) value);
                    ofs += 7;
                    return;
                }

                data[ofs] = 0;
            }

            data[ofs + 1] = unchecked((byte) (value >> 56));
            data[ofs + 2] = unchecked((byte) (value >> 48));
            data[ofs + 3] = unchecked((byte) (value >> 40));
            data[ofs + 4] = unchecked((byte) (value >> 32));
            data[ofs + 5] = unchecked((byte) (value >> 24));
            data[ofs + 6] = unchecked((byte) (value >> 16));
            data[ofs + 7] = unchecked((byte) (value >> 8));
            data[ofs + 8] = unchecked((byte) value);
            ofs += 9;
        }

        public static long UnpackVInt(byte[] data, ref int ofs)
        {
            uint first = data[ofs];
            ofs++;
            if (0x40 <= first && first < 0xC0) return (long) first - 0x80;
            long result;
            if (first >= 0x80)
            {
                if (first < 0xE0)
                {
                    result = ((first & 0x1F) << 8) + data[ofs];
                    ofs++;
                    return result;
                }

                if (first < 0xF0)
                {
                    result = ((first & 0x0F) << 16) + (data[ofs] << 8) + data[ofs + 1];
                    ofs += 2;
                    return result;
                }

                if (first < 0xF8)
                {
                    result = ((first & 0x07) << 24) + (data[ofs] << 16) + (data[ofs + 1] << 8) + data[ofs + 2];
                    ofs += 3;
                    return result;
                }

                if (first < 0xFC)
                {
                    result = ((long) (first & 0x03) << 32) + ((long) data[ofs] << 24) + (data[ofs + 1] << 16) +
                             (data[ofs + 2] << 8) + data[ofs + 3];
                    ofs += 4;
                    return result;
                }

                if (first < 0xFE)
                {
                    result = ((long) (first & 0x01) << 40) + ((long) data[ofs] << 32) + ((long) data[ofs + 1] << 24) +
                             (data[ofs + 2] << 16) + (data[ofs + 3] << 8) + data[ofs + 4];
                    ofs += 5;
                    return result;
                }

                if (first < 0xFF)
                {
                    result = ((long) data[ofs] << 40) + ((long) data[ofs + 1] << 32) + ((long) data[ofs + 2] << 24) +
                             (data[ofs + 3] << 16) + (data[ofs + 4] << 8) + data[ofs + 5];
                    ofs += 6;
                    return result;
                }

                result = ((long) data[ofs] << 56) + ((long) data[ofs + 1] << 48) + ((long) data[ofs + 2] << 40) +
                         ((long) data[ofs + 3] << 32)
                         + ((long) data[ofs + 4] << 24) + (data[ofs + 5] << 16) + (data[ofs + 6] << 8) + data[ofs + 7];
                ofs += 8;
                return result;
            }

            if (first >= 0x20)
            {
                result = ((int) (first - 0x40) << 8) + data[ofs];
                ofs++;
                return result;
            }

            if (first >= 0x10)
            {
                result = ((int) (first - 0x20) << 16) + (data[ofs] << 8) + data[ofs + 1];
                ofs += 2;
                return result;
            }

            if (first >= 0x08)
            {
                result = ((int) (first - 0x10) << 24) + (data[ofs] << 16) + (data[ofs + 1] << 8) + data[ofs + 2];
                ofs += 3;
                return result;
            }

            if (first >= 0x04)
            {
                result = ((long) (first - 0x08) << 32) + ((long) data[ofs] << 24) + (data[ofs + 1] << 16) +
                         (data[ofs + 2] << 8) + data[ofs + 3];
                ofs += 4;
                return result;
            }

            if (first >= 0x02)
            {
                result = ((long) (first - 0x04) << 40) + ((long) data[ofs] << 32) + ((long) data[ofs + 1] << 24) +
                         (data[ofs + 2] << 16) + (data[ofs + 3] << 8) + data[ofs + 4];
                ofs += 5;
                return result;
            }

            if (first >= 0x01)
            {
                result = (-1L << 48) + ((long) data[ofs] << 40) + ((long) data[ofs + 1] << 32) +
                         ((long) data[ofs + 2] << 24) + (data[ofs + 3] << 16) + (data[ofs + 4] << 8) + data[ofs + 5];
                ofs += 6;
                return result;
            }

            result = ((long) data[ofs] << 56) + ((long) data[ofs + 1] << 48) + ((long) data[ofs + 2] << 40) +
                     ((long) data[ofs + 3] << 32)
                     + ((long) data[ofs + 4] << 24) + (data[ofs + 5] << 16) + (data[ofs + 6] << 8) + data[ofs + 7];
            ofs += 8;
            return result;
        }
    }
}
