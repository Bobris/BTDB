using System;
using System.Collections.Generic;
using BTDB.Buffer;

namespace BTDB.KVDBLayer.BTree;

ref struct CreateOrUpdateCtx
{
    internal ReadOnlySpan<byte> Key;
    internal uint ValueFileId;
    internal uint ValueOfs;
    internal int ValueSize;

    internal List<NodeIdxPair>? Stack;
    internal long KeyIndex;

    internal long TransactionId;
    internal int Depth;
    internal bool Created;
    internal bool Split; // Node1+Node2 set
    internal bool SplitInRight; // false key is in Node1, true key is in Node2
    internal bool Update; // Node1 set
    internal IBTreeNode? Node1;
    internal IBTreeNode? Node2;
}
