using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTreeMem
{
    interface IBTreeLeafNode
    {
        ByteBuffer GetKey(int idx);
        ByteBuffer GetMemberValue(int idx);
        void SetMemberValue(int idx, ByteBuffer value);
    }
}