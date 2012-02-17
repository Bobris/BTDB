namespace BTDB.KV2DBLayer.BTree
{
    internal struct BTreeLeafMember
    {
        internal byte[] Key;
        internal int ValueFileId;
        internal int ValueOfs;
        internal int ValueSize;
    }
}