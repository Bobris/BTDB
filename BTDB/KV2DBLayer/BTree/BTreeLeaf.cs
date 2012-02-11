using System;

namespace BTDB.KV2DBLayer.BTree
{
    internal class BTreeLeaf : IBTreeNode
    {
        internal long TransactionId;
        Member[] _keyvalues;

        internal static IBTreeNode CreateFirst(CreateOrUpdateCtx ctx)
        {
            var result = new BTreeLeaf
                {
                    TransactionId = ctx.TransactionId,
                    _keyvalues = new[]
                        {
                            new Member
                                {
                                    Key = ctx.WholeKey(),
                                    ValueFileId = ctx.ValueFileId,
                                    ValueOfs = ctx.ValueOfs,
                                    ValueSize = ctx.ValueSize
                                }
                        }
                };
            return result;
        }

        struct Member
        {
            internal byte[] Key;
            internal int ValueFileId;
            internal int ValueOfs;
            internal int ValueSize;
        }

        public void CreateOrUpdate(CreateOrUpdateCtx ctx)
        {
            throw new NotImplementedException();
        }

        public long CalcKeyCount()
        {
            return _keyvalues.Length;
        }
    }
}