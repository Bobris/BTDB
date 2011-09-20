namespace BTDB.KVDBLayer
{
    internal struct SectorPtr
    {
        internal long Ptr;
        internal uint Checksum;

        internal static void Pack(byte[] data, int offset, SectorPtr value)
        {
            PackUnpack.PackInt64LE(data, offset, value.Ptr);
            PackUnpack.PackUInt32LE(data, offset + 8, value.Checksum);
        }

        internal static SectorPtr Unpack(byte[] data, int offset)
        {
            SectorPtr result;
            result.Ptr = PackUnpack.UnpackInt64LE(data, offset);
            result.Checksum = PackUnpack.UnpackUInt32LE(data, offset + 8);
            return result;
        }
    }
}