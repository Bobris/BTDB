using System;
using System.Collections.Generic;
using BTDB.Collections;

namespace BTDB.KVDBLayer.BTreeMem;

ref struct UpdateKeySuffixCtx
{
    internal ReadOnlySpan<byte> Key;
    internal uint PrefixLen;

    internal UpdateKeySuffixResult Result;
    internal ref StructList<NodeIdxPair> Stack;
    internal long KeyIndex;
    internal bool Update; // Node set
    internal IBTreeNode? Node;

    internal int Depth;
    internal long TransactionId;
}
