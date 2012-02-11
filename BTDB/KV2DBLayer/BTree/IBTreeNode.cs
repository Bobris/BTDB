namespace BTDB.KV2DBLayer.BTree
{
    internal interface IBTreeNode
    {
        void CreateOrUpdate(CreateOrUpdateCtx ctx);
        long CalcKeyCount();
    }
}