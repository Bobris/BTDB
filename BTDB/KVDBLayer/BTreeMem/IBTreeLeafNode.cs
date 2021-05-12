using System;

namespace BTDB.KVDBLayer.BTreeMem
{
    interface IBTreeLeafNode
    {
        ReadOnlySpan<byte> GetKey(int idx);
        ReadOnlySpan<byte> GetMemberValue(int idx);
        void SetMemberValue(int idx, in ReadOnlySpan<byte> value);
    }
}
