namespace BTDB.KV2DBLayer.BTree
{
    internal struct BTreeValue
    {
        internal uint ValueFileId;
        internal uint ValueOfs;
        internal int ValueSize; // Negative length means compressed
    }
}