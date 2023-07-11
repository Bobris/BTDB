using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer.BTreeMem;

ref struct UpdateKeySuffixCtx
{
    internal ReadOnlySpan<byte> Key;
    internal uint PrefixLen;

    internal bool Updated;
    internal List<NodeIdxPair> Stack;
    internal long KeyIndex;
    internal bool Update; // Node set
    internal IBTreeNode? Node;

    internal int Depth;
    internal long TransactionId;
}
