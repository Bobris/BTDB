using System;

namespace BTDB.KVDBLayer.BTreeMem;

interface IBTreeLeafNode
{
    ReadOnlyMemory<byte> GetKey(int idx);
    ReadOnlyMemory<byte> GetMemberValue(int idx);
    void SetMemberValue(int idx, in ReadOnlySpan<byte> value);
}
