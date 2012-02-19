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
            var result = _rootNode.FindKey(stack, out keyIndex, prefix, key);
            if (result == FindResult.Previous)
            {
                if (keyIndex < 0)
                {
                    keyIndex = 0;
                    stack[stack.Count - 1] = new NodeIdxPair { Node = stack[stack.Count - 1].Node, Idx = stack[stack.Count - 1].Idx - 1 };
                    result = FindResult.Next;
                }
            }
            return result;
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

        public long FindLastWithPrefix(byte[] prefix)
        {
            if (_rootNode == null) return -1;
            return _rootNode.FindLastWithPrefix(prefix);
        }

        public long TransactionId
        {
            get { return _transactionId; }
        }

        public IBTreeRootNode NewTransactionRoot()
        {
            return new BTreeRoot(_transactionId + 1) { _levels = _levels, _keyValueCount = _keyValueCount, _rootNode = _rootNode };
        }
    }
}