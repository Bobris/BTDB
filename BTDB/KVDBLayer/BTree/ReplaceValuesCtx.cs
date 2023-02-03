using System;
using System.Collections.Generic;
using System.Threading;
using BTDB.Collections;

namespace BTDB.KVDBLayer.BTree;

class ReplaceValuesCtx
{
    internal CancellationToken _cancellation;
    internal long _transactionId;
    internal RefDictionary<ulong, uint> _newPositionMap;
    internal DateTime _iterationTimeOut;
    internal byte[]? _restartKey;
    internal bool _interrupt;
    internal uint _targetFileId;
}
