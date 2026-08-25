using System.Collections.Generic;

namespace BTDB.ODBLayer;

public readonly record struct LeakRemovalResult(
    ulong LeakedKeyCount,
    ulong RemovedKeyCount,
    IReadOnlyCollection<string> LeakedObjectTypeNames);
