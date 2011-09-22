namespace BTDB.StreamLayer
{
    public static class PackUnpack
    {
        public static void PackUInt16LE(byte[] data, int offset, ushort value)
        {
            data[offset] = unchecked((byte)value);
            data[offset + 1] = unchecked((byte)(value >> 8));
        }

        public static ushort UnpackUInt16LE(byte[] data, int offset)
        {
            return (ushort)(data[offset] | (data[offset + 1] << 8));
        }

        public static void PackInt16LE(byte[] data, int offset, short value)
        {
            data[offset] = unchecked((byte)value);
            data[offset + 1] = unchecked((byte)(value >> 8));
        }

        public static short UnpackInt16LE(byte[] data, int offset)
        {
            return (short)(data[offset] | (data[offset + 1] << 8));
        }

        public static void PackUInt32LE(byte[] data, int offset, uint value)
        {
            data[offset] = unchecked((byte)value);
            data[offset + 1] = unchecked((byte)(value >> 8));
            data[offset + 2] = unchecked((byte)(value >> 16));
            data[offset + 3] = unchecked((byte)(value >> 24));
        }

        public static uint UnpackUInt32LE(byte[] data, int offset)
        {
            return (uint)(data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24));
        }

        public static void PackInt32LE(byte[] data, int offset, int value)
        {
            data[offset] = unchecked((byte)value);
            data[offset + 1] = unchecked((byte)(value >> 8));
            data[offset + 2] = unchecked((byte)(value >> 16));
            data[offset + 3] = unchecked((byte)(value >> 24));
        }

        public static void PackInt32BE(byte[] data, int offset, int value)
        {
            data[offset + 3] = unchecked((byte)value);
            data[offset + 2] = unchecked((byte)(value >> 8));
            data[offset + 1] = unchecked((byte)(value >> 16));
            data[offset] = unchecked((byte)(value >> 24));
        }

        public static int UnpackInt32LE(byte[] data, int offset)
        {
            return data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24);
        }

        public static int UnpackInt32BE(byte[] data, int offset)
        {
            return data[offset + 3] | (data[offset + 2] << 8) | (data[offset + 1] << 16) | (data[offset] << 24);
        }

        public static void PackUInt64LE(byte[] data, int offset, ulong value)
        {
            data[offset] = unchecked((byte)value);
            data[offset + 1] = unchecked((byte)(value >> 8));
            data[offset + 2] = unchecked((byte)(value >> 16));
            data[offset + 3] = unchecked((byte)(value >> 24));
            data[offset + 4] = unchecked((byte)(value >> 32));
            data[offset + 5] = unchecked((byte)(value >> 40));
            data[offset + 6] = unchecked((byte)(value >> 48));
            data[offset + 7] = unchecked((byte)(value >> 56));
        }

        public static ulong UnpackUInt64LE(byte[] data, int offset)
        {
            return data[offset] | ((ulong)data[offset + 1] << 8) |
                   ((ulong)data[offset + 2] << 16) | ((ulong)data[offset + 3] << 24) |
                   ((ulong)data[offset + 4] << 32) | ((ulong)data[offset + 5] << 40) |
                   ((ulong)data[offset + 6] << 48) | ((ulong)data[offset + 7] << 56);
        }

        public static void PackInt64LE(byte[] data, int offset, long value)
        {
            data[offset] = unchecked((byte)value);
            data[offset + 1] = unchecked((byte)(value >> 8));
            data[offset + 2] = unchecked((byte)(value >> 16));
            data[offset + 3] = unchecked((byte)(value >> 24));
            data[offset + 4] = unchecked((byte)(value >> 32));
            data[offset + 5] = unchecked((byte)(value >> 40));
            data[offset + 6] = unchecked((byte)(value >> 48));
            data[offset + 7] = unchecked((byte)(value >> 56));
        }

        public static void PackInt64BE(byte[] data, int offset, long value)
        {
            data[offset + 7] = unchecked((byte)value);
            data[offset + 6] = unchecked((byte)(value >> 8));
            data[offset + 5] = unchecked((byte)(value >> 16));
            data[offset + 4] = unchecked((byte)(value >> 24));
            data[offset + 3] = unchecked((byte)(value >> 32));
            data[offset + 2] = unchecked((byte)(value >> 40));
            data[offset + 1] = unchecked((byte)(value >> 48));
            data[offset] = unchecked((byte)(value >> 56));
        }

        public static long UnpackInt64LE(byte[] data, int offset)
        {
            return data[offset] | ((long)data[offset + 1] << 8) |
                   ((long)data[offset + 2] << 16) | ((long)data[offset + 3] << 24) |
                   ((long)data[offset + 4] << 32) | ((long)data[offset + 5] << 40) |
                   ((long)data[offset + 6] << 48) | ((long)data[offset + 7] << 56);
        }

        public static long UnpackInt64BE(byte[] data, int offset)
        {
            return data[offset + 7] | ((long)data[offset + 6] << 8) |
                   ((long)data[offset + 5] << 16) | ((long)data[offset + 4] << 24) |
                   ((long)data[offset + 3] << 32) | ((long)data[offset + 2] << 40) |
                   ((long)data[offset + 1] << 48) | ((long)data[offset] << 56);
        }

        public static void IncrementInt64LE(byte[] data, int offset)
        {
            var b = (byte)(data[offset] + 1);
            data[offset] = b;
            if (b != 0) return;
            offset++;
            b = (byte)(data[offset] + 1);
            data[offset] = b;
            if (b != 0) return;
            offset++;
            b = (byte)(data[offset] + 1);
            data[offset] = b;
            if (b != 0) return;
            offset++;
            b = (byte)(data[offset] + 1);
            data[offset] = b;
            if (b != 0) return;
            offset++;
            b = (byte)(data[offset] + 1);
            data[offset] = b;
            if (b != 0) return;
            offset++;
            b = (byte)(data[offset] + 1);
            data[offset] = b;
            if (b != 0) return;
            offset++;
            b = (byte)(data[offset] + 1);
            data[offset] = b;
            if (b != 0) return;
            offset++;
            b = (byte)(data[offset] + 1);
            data[offset] = b;
        }

        public static int LengthVUInt(uint value)
        {
            if (value < 0x80) return 1;
            if (value < 0x4000) return 2;
            if (value < 0x200000) return 3;
            if (value < 0x10000000) return 4;
            return 5;
        }

        public static int LengthVUInt(ulong value)
        {
            if (value < 0x80) return 1;
            if (value < 0x4000) return 2;
            if (value < 0x200000) return 3;
            if (value < 0x10000000) return 4;
            if (value < 0x0800000000) return 5;
            if (value < 0x040000000000) return 6;
            if (value < 0x02000000000000) return 7;
            if (value < 0x0100000000000000) return 8;
            return 9;
        }

        public static int LengthVUInt(byte[] data, int ofs)
        {
            uint first = data[ofs];
            if (first < 0x80) return 1;
            if (first < 0xC0) return 2;
            if (first < 0xE0) return 3;
            if (first < 0xF0) return 4;
            if (first < 0xF8) return 5;
            if (first < 0xFC) return 6;
            if (first < 0xFE) return 7;
            return first == 0xFE ? 8 : 9;
        }

        public static void PackVUInt(byte[] data, ref int ofs, uint value)
        {
            if (value < 0x80)
            {
                data[ofs] = (byte)value;
                ofs++;
                return;
            }
            if (value < 0x4000)
            {
                data[ofs] = (byte)(0x80 + (value >> 8));
                data[ofs + 1] = unchecked((byte)value);
                ofs += 2;
                return;
            }
            if (value < 0x200000)
            {
                data[ofs] = (byte)(0xC0 + (value >> 16));
                data[ofs + 1] = unchecked((byte)(value >> 8));
                data[ofs + 2] = unchecked((byte)value);
                ofs += 3;
                return;
            }
            if (value < 0x10000000)
            {
                data[ofs] = (byte)(0xE0 + (value >> 24));
                data[ofs + 1] = unchecked((byte)(value >> 16));
                data[ofs + 2] = unchecked((byte)(value >> 8));
                data[ofs + 3] = unchecked((byte)value);
                ofs += 4;
                return;
            }
            data[ofs] = 0xF0;
            data[ofs + 1] = unchecked((byte)(value >> 24));
            data[ofs + 2] = unchecked((byte)(value >> 16));
            data[ofs + 3] = unchecked((byte)(value >> 8));
            data[ofs + 4] = unchecked((byte)value);
            ofs += 5;
        }

        public static void PackVUInt(byte[] data, ref int ofs, ulong value)
        {
            if (value < 0x80)
            {
                data[ofs] = (byte)value;
                ofs++;
                return;
            }
            if (value < 0x4000)
            {
                data[ofs] = (byte)(0x80 + (value >> 8));
                data[ofs + 1] = unchecked((byte)value);
                ofs += 2;
                return;
            }
            if (value < 0x200000)
            {
                data[ofs] = (byte)(0xC0 + (value >> 16));
                data[ofs + 1] = unchecked((byte)(value >> 8));
                data[ofs + 2] = unchecked((byte)value);
                ofs += 3;
                return;
            }
            if (value < 0x10000000)
            {
                data[ofs] = (byte)(0xE0 + (value >> 24));
                data[ofs + 1] = unchecked((byte)(value >> 16));
                data[ofs + 2] = unchecked((byte)(value >> 8));
                data[ofs + 3] = unchecked((byte)value);
                ofs += 4;
                return;
            }
            if (value < 0x0800000000)
            {
                data[ofs] = (byte)(0xF0 + (value >> 32));
                data[ofs + 1] = unchecked((byte)(value >> 24));
                data[ofs + 2] = unchecked((byte)(value >> 16));
                data[ofs + 3] = unchecked((byte)(value >> 8));
                data[ofs + 4] = unchecked((byte)value);
                ofs += 5;
                return;
            }
            if (value < 0x040000000000)
            {
                data[ofs] = (byte)(0xF8 + (value >> 40));
                data[ofs + 1] = unchecked((byte)(value >> 32));
                data[ofs + 2] = unchecked((byte)(value >> 24));
                data[ofs + 3] = unchecked((byte)(value >> 16));
                data[ofs + 4] = unchecked((byte)(value >> 8));
                data[ofs + 5] = unchecked((byte)value);
                ofs += 6;
                return;
            }
            if (value < 0x02000000000000)
            {
                data[ofs] = (byte)(0xFC + (value >> 48));
                data[ofs + 1] = unchecked((byte)(value >> 40));
                data[ofs + 2] = unchecked((byte)(value >> 32));
                data[ofs + 3] = unchecked((byte)(value >> 24));
                data[ofs + 4] = unchecked((byte)(value >> 16));
                data[ofs + 5] = unchecked((byte)(value >> 8));
                data[ofs + 6] = unchecked((byte)value);
                ofs += 7;
                return;
            }
            if (value < 0x0100000000000000)
            {
                data[ofs] = (byte)(0xFE + (value >> 56));
                data[ofs + 1] = unchecked((byte)(value >> 48));
                data[ofs + 2] = unchecked((byte)(value >> 40));
                data[ofs + 3] = unchecked((byte)(value >> 32));
                data[ofs + 4] = unchecked((byte)(value >> 24));
                data[ofs + 5] = unchecked((byte)(value >> 16));
                data[ofs + 6] = unchecked((byte)(value >> 8));
                data[ofs + 7] = unchecked((byte)value);
                ofs += 8;
                return;
            }
            data[ofs] = 0xFF;
            data[ofs + 1] = unchecked((byte)(value >> 56));
            data[ofs + 2] = unchecked((byte)(value >> 48));
            data[ofs + 3] = unchecked((byte)(value >> 40));
            data[ofs + 4] = unchecked((byte)(value >> 32));
            data[ofs + 5] = unchecked((byte)(value >> 24));
            data[ofs + 6] = unchecked((byte)(value >> 16));
            data[ofs + 7] = unchecked((byte)(value >> 8));
            data[ofs + 8] = unchecked((byte)value);
            ofs += 9;
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
                result = ((first & 0x1Fu) << 16) + ((uint)data[ofs] << 8) + data[ofs + 1];
                ofs += 2;
                return result;
            }
            if (first < 0xF0)
            {
                result = ((first & 0x0Fu) << 24) + ((uint)data[ofs] << 16) + ((uint)data[ofs + 1] << 8) + data[ofs + 2];
                ofs += 3;
                return result;
            }
            if (first < 0xF8)
            {
                result = ((ulong)(first & 0x07u) << 32) + ((uint)data[ofs] << 24) + ((uint)data[ofs + 1] << 16)
                    + ((uint)data[ofs + 2] << 8) + data[ofs + 3];
                ofs += 4;
                return result;
            }
            if (first < 0xFC)
            {
                result = ((ulong)(first & 0x03u) << 40) + ((ulong)data[ofs] << 32) + ((uint)data[ofs + 1] << 24)
                    + ((uint)data[ofs + 2] << 16) + ((uint)data[ofs + 3] << 8) + data[ofs + 4];
                ofs += 5;
                return result;
            }
            if (first < 0xFE)
            {
                result = ((ulong)(first & 0x01u) << 48) + ((ulong)data[ofs] << 40) + ((ulong)data[ofs + 1] << 32)
                    + ((uint)data[ofs + 2] << 24) + ((uint)data[ofs + 3] << 16) + ((uint)data[ofs + 4] << 8) + data[ofs + 5];
                ofs += 6;
                return result;
            }
            if (first == 0xFE)
            {
                result = ((ulong)data[ofs] << 48) + ((ulong)data[ofs + 1] << 40) + ((ulong)data[ofs + 2] << 32)
                    + ((uint)data[ofs + 3] << 24) + ((uint)data[ofs + 4] << 16) + ((uint)data[ofs + 5] << 8) + data[ofs + 6];
                ofs += 7;
                return result;
            }
            result = ((ulong)data[ofs] << 56) + ((ulong)data[ofs + 1] << 48) + ((ulong)data[ofs + 2] << 40) + ((ulong)data[ofs + 3] << 32)
                + ((uint)data[ofs + 4] << 24) + ((uint)data[ofs + 5] << 16) + ((uint)data[ofs + 6] << 8) + data[ofs + 7];
            ofs += 8;
            return result;
        }
    }
}
