namespace BTDB
{
    internal struct SectorPtr
    {
        internal long Ptr;
        internal uint Checksum;

        internal static void Pack(byte[] data, int offset, SectorPtr value)
        {
            PackUnpack.PackInt64(data, offset, value.Ptr);
            PackUnpack.PackUInt32(data, offset + 8, value.Checksum);
        }

        internal static SectorPtr Unpack(byte[] data, int offset)
        {
            SectorPtr result;
            result.Ptr = PackUnpack.UnpackInt64(data, offset);
            result.Checksum = PackUnpack.UnpackUInt32(data, offset + 8);
            return result;
        }
    }
}