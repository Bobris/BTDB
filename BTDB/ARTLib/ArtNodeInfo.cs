namespace BTDB.ARTLib
{
    public class ArtNodeInfo
    {
        public uint MaxChildCount;
        public uint ChildCount;
        public ulong RecursiveChildCount;
        public uint NodeByteSize;
        public uint PrefixKeySize;
        public uint Deepness;
        public bool HasLeafChild;
    }
}
