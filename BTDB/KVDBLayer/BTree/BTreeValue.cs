namespace BTDB.KVDBLayer.BTree
{
    internal struct BTreeValue
    {
        internal uint ValueFileId;
        internal uint ValueOfs;
        internal int ValueSize; // Negative length means compressed
    }
}