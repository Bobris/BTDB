using System.Collections.Generic;

namespace BTDB.KV2DBLayer.BTree
{
    internal interface IBTreeRootNode : IBTreeNode
    {
        long TransactionId { get; }
        IBTreeRootNode NewTransactionRoot();
    }
}