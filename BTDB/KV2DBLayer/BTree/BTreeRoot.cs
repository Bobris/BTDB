using System.Collections.Generic;

namespace BTDB.KV2DBLayer.BTree
{
    internal class BTreeRoot : IBTreeNode
    {
        internal int Levels;
        internal long KeyValueCount;
        internal long TransactionId;
        internal IBTreeNode RootNode;

        public void CreateOrUpdate(CreateOrUpdateCtx ctx)
        {
            ctx.TransactionId = TransactionId;
            if (ctx.Stack == null) ctx.Stack = new List<NodeIdxPair>(Levels + 2);
            else ctx.Stack.Clear();
            ctx.Stack.Add(new NodeIdxPair { Node = this, Idx = 0 });
            ctx.Depth = 0;
            if (Levels == 0)
            {
                RootNode = BTreeLeaf.CreateFirst(ctx);
                Levels = 1;
                KeyValueCount = 1;
                ctx.Stack.Add(new NodeIdxPair { Node = RootNode, Idx = 0 });
                ctx.KeyIndex = 0;
                ctx.Created = true;
                return;
            }
            ctx.Stack.Add(new NodeIdxPair { Node = RootNode, Idx = 0 });
            RootNode.CreateOrUpdate(ctx);
            if (ctx.Split)
            {
                RootNode = BTreeBranch.CreateFromTwo(ctx.Node1, ctx.Node2);
                Levels++;
            }
            else if (ctx.Update)
            {
                RootNode = ctx.Node1;
            }
            if (ctx.Created)
            {
                KeyValueCount++;
            }
        }

        public long CalcKeyCount()
        {
            return KeyValueCount;
        }
    }
}