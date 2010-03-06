namespace BTDB
{
    static public class PackUnpack
    {
        static public void PackUInt16(byte[] data, int offset, ushort value)
        {
            data[offset]=(byte)value;
            data[offset+1]=(byte)(value>>8);
        }
        static public ushort UnpackUInt16(byte[] data, int offset)
        {
            return (ushort)(data[offset] | (data[offset + 1] << 8));
        }
        static public void PackInt16(byte[] data, int offset, short value)
        {
            data[offset] = (byte)value;
            data[offset + 1] = (byte)(value >> 8);
        }
        static public short UnpackInt16(byte[] data, int offset)
        {
            return (short)(data[offset] | ((sbyte)data[offset + 1] << 8));
        }
        static public void PackUInt32(byte[] data, int offset, uint value)
        {
            data[offset] = (byte)value;
            data[offset + 1] = (byte)(value >> 8);
            data[offset + 2] = (byte)(value >> 16);
            data[offset + 3] = (byte)(value >> 24);
        }
        static public uint UnpackUInt32(byte[] data, int offset)
        {
            return (uint)(data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24));
        }
        static public void PackInt32(byte[] data, int offset, int value)
        {
            data[offset] = (byte)value;
            data[offset + 1] = (byte)(value >> 8);
            data[offset + 2] = (byte)(value >> 16);
            data[offset + 3] = (byte)(value >> 24);
        }
        static public int UnpackInt32(byte[] data, int offset)
        {
            return data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24);
        }
        static public void PackUInt64(byte[] data, int offset, ulong value)
        {
            PackUInt32(data, offset, (uint)value);
            PackUInt32(data, offset + 4, (uint)(value>>32));
        }
        static public ulong UnpackUInt64(byte[] data, int offset)
        {
            return (UnpackUInt32(data, offset) | ((ulong)UnpackUInt32(data, offset + 4) << 32));
        }
        static public void PackInt64(byte[] data, int offset, long value)
        {
            PackUInt32(data, offset, (uint)value);
            PackInt32(data, offset + 4, (int)(value >> 32));
        }
        static public long UnpackInt64(byte[] data, int offset)
        {
            return UnpackUInt32(data, offset) | ((long)UnpackInt32(data, offset + 4) << 32);
        }
    }
}
