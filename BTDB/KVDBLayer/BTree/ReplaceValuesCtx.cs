using System;
using System.Collections.Generic;
using System.Threading;

namespace BTDB.KVDBLayer.BTree
{
    class ReplaceValuesCtx
    {
        internal CancellationToken _cancellation;
        internal long _transactionId;
        internal Dictionary<ulong, ulong> _newPositionMap;
        internal DateTime _iterationTimeOut;
        internal byte[]? _restartKey;
        internal bool _interrupt;
    }
}
