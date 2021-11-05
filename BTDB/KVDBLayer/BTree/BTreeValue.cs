namespace BTDB.KVDBLayer.BTree;

struct BTreeValue
{
    internal uint ValueFileId;
    internal uint ValueOfs;
    internal int ValueSize; // Negative length means compressed
}
