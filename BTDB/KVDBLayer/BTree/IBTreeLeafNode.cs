using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTree
{
    interface IBTreeLeafNode
    {
        ByteBuffer GetKey(int idx);
        BTreeValue GetMemberValue(int idx);
        void SetMemberValue(int idx, BTreeValue value);
    }
}