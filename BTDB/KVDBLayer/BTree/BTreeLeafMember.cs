namespace BTDB.KVDBLayer.BTree
{
    internal struct BTreeLeafMember
    {
        internal byte[] Key;
        internal uint ValueFileId;
        internal uint ValueOfs;
        internal int ValueSize; // Negative length means compressed
    }
}