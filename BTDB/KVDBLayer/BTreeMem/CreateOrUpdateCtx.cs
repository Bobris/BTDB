using System;
using System.Collections.Generic;
using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTreeMem;

ref struct CreateOrUpdateCtx
{
    internal ReadOnlySpan<byte> Key;
    internal ReadOnlySpan<byte> Value;

    internal bool Created;
    internal List<NodeIdxPair> Stack;
    internal long KeyIndex;

    internal int Depth;
    internal long TransactionId;
    internal bool Split; // Node1+Node2 set
    internal bool SplitInRight; // false key is in Node1, true key is in Node2
    internal bool Update; // Node1 set
    internal IBTreeNode? Node1;
    internal IBTreeNode? Node2;
}
