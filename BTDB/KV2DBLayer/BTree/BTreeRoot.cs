using System.Collections.Generic;
using System.Diagnostics;
using BTDB.Buffer;

namespace BTDB.KV2DBLayer.BTree
{
    internal class BTreeRoot : IBTreeRootNode
    {
        readonly long _transactionId;
        int _levels;
        long _keyValueCount;
        IBTreeNode _rootNode;

        public BTreeRoot(long transactionId)
        {
            _transactionId = transactionId;
        }

        public void CreateOrUpdate(CreateOrUpdateCtx ctx)
        {
            ctx.TransactionId = _transactionId;
            if (ctx.Stack == null) ctx.Stack = new List<NodeIdxPair>(_levels + 1);
            else ctx.Stack.Clear();
            if (_levels == 0)
            {
                _rootNode = BTreeLeaf.CreateFirst(ctx);
                _levels = 1;
                _keyValueCount = 1;
                ctx.Stack.Add(new NodeIdxPair { Node = _rootNode, Idx = 0 });
                ctx.KeyIndex = 0;
                ctx.Created = true;
                return;
            }
            ctx.Depth = 1;
            _rootNode.CreateOrUpdate(ctx);
            if (ctx.Split)
            {
                _rootNode = new BTreeBranch(ctx.TransactionId, ctx.Node1, ctx.Node2);
                ctx.Stack.Insert(1, new NodeIdxPair { Node = _rootNode, Idx = ctx.SplitInRight ? 1 : 0 });
                _levels++;
            }
            else if (ctx.Update)
            {
                _rootNode = ctx.Node1;
            }
            if (ctx.Created)
            {
                _keyValueCount++;
            }
        }

        public FindResult FindKey(List<NodeIdxPair> stack, out long keyIndex, byte[] prefix, ByteBuffer key)
        {
            stack.Clear();
            if (_rootNode == null)
            {
                keyIndex = -1;
                return FindResult.NotFound;
            }
            return _rootNode.FindKey(stack, out keyIndex, prefix, key);
        }

        public long CalcKeyCount()
        {
            return _keyValueCount;
        }

        public byte[] GetLeftMostKey()
        {
            return _rootNode.GetLeftMostKey();
        }

        public void FillStackByIndex(List<NodeIdxPair> stack, long keyIndex)
        {
            Debug.Assert(keyIndex >= 0 && keyIndex < _keyValueCount);
            stack.Clear();
            _rootNode.FillStackByIndex(stack, keyIndex);
        }

        public long TransactionId
        {
            get { return _transactionId; }
        }

        public IBTreeRootNode NewTransactionRoot()
        {
            var result = new BTreeRoot(_transactionId + 1);
            result._levels = _levels;
            result._keyValueCount = _keyValueCount;
            result._rootNode = _rootNode;
            return result;
        }
    }
}