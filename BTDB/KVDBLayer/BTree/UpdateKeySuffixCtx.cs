using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer.BTree;

ref struct UpdateKeySuffixCtx
{
    internal ReadOnlySpan<byte> Key;
    internal uint PrefixLen;

    internal UpdateKeySuffixResult Result;
    internal List<NodeIdxPair>? Stack;
    internal long KeyIndex;

    internal long TransactionId;
    internal int Depth;
    internal bool Update; // Node set
    internal IBTreeNode? Node;
}
