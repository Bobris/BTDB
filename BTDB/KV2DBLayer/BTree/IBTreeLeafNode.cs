using BTDB.Buffer;

namespace BTDB.KV2DBLayer.BTree
{
    internal interface IBTreeLeafNode
    {
        ByteBuffer GetKey(int idx);
        BTreeValue GetMemberValue(int idx);
        void SetMemberValue(int idx, BTreeValue value);
    }
}