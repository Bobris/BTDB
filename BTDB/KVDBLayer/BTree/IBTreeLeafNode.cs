using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTree
{
    internal interface IBTreeLeafNode
    {
        ByteBuffer GetKey(int idx);
        BTreeValue GetMemberValue(int idx);
        void SetMemberValue(int idx, BTreeValue value);
    }
}