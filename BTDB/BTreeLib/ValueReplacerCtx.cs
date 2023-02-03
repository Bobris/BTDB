using System;
using System.Collections.Generic;
using System.Threading;
using BTDB.Collections;

namespace BTDB.BTreeLib;

public struct ValueReplacerCtx
{
    internal DateTime _operationTimeout;
    internal bool _interrupted;
    internal RefDictionary<ulong, uint> _positionMap;
    internal CancellationToken _cancellation;
    internal byte[] _interruptedKey;
    internal bool _afterFirst;
    internal uint _targetFileId;
}
