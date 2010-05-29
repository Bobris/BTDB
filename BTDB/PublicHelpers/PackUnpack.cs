namespace BTDB
{
    public static class PackUnpack
    {
        public static void PackUInt16(byte[] data, int offset, ushort value)
        {
            data[offset] = (byte)value;
            data[offset + 1] = (byte)(value >> 8);
        }

        public static ushort UnpackUInt16(byte[] data, int offset)
        {
            return (ushort)(data[offset] | (data[offset + 1] << 8));
        }

        public static void PackInt16(byte[] data, int offset, short value)
        {
            data[offset] = (byte)value;
            data[offset + 1] = (byte)(value >> 8);
        }

        public static short UnpackInt16(byte[] data, int offset)
        {
            return (short)(data[offset] | ((sbyte)data[offset + 1] << 8));
        }

        public static void PackUInt32(byte[] data, int offset, uint value)
        {
            data[offset] = (byte)value;
            data[offset + 1] = (byte)(value >> 8);
            data[offset + 2] = (byte)(value >> 16);
            data[offset + 3] = (byte)(value >> 24);
        }

        public static uint UnpackUInt32(byte[] data, int offset)
        {
            return (uint)(data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24));
        }

        public static void PackInt32(byte[] data, int offset, int value)
        {
            data[offset] = (byte)value;
            data[offset + 1] = (byte)(value >> 8);
            data[offset + 2] = (byte)(value >> 16);
            data[offset + 3] = (byte)(value >> 24);
        }

        public static int UnpackInt32(byte[] data, int offset)
        {
            return data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24);
        }

        public static void PackUInt64(byte[] data, int offset, ulong value)
        {
            PackUInt32(data, offset, (uint)value);
            PackUInt32(data, offset + 4, (uint)(value >> 32));
        }

        public static ulong UnpackUInt64(byte[] data, int offset)
        {
            return UnpackUInt32(data, offset) | ((ulong)UnpackUInt32(data, offset + 4) << 32);
        }

        public static void PackInt64(byte[] data, int offset, long value)
        {
            PackUInt32(data, offset, (uint)value);
            PackInt32(data, offset + 4, (int)(value >> 32));
        }

        public static long UnpackInt64(byte[] data, int offset)
        {
            return UnpackUInt32(data, offset) | ((long)UnpackInt32(data, offset + 4) << 32);
        }
    }
}
