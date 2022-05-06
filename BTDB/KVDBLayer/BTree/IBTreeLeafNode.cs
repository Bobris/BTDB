using System;

namespace BTDB.KVDBLayer.BTree;

interface IBTreeLeafNode
{
    ReadOnlySpan<byte> GetKey(int idx);
    BTreeValue GetMemberValue(int idx);
    void SetMemberValue(int idx, in BTreeValue value);
}
