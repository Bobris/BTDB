using System;
using System.Collections.Generic;
using System.Threading;

namespace BTDB.BTreeLib;

public struct ValueReplacerCtx
{
    internal DateTime _operationTimeout;
    internal bool _interrupted;
    internal Dictionary<ulong, ulong> _positionMap;
    internal CancellationToken _cancellation;
    internal byte[] _interruptedKey;
    internal bool _afterFirst;
}
