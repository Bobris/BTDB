using System.Collections.Generic;

namespace BTDB.ODBLayer;

public readonly record struct LeakDetectionResult(
    ulong LeakedKeyCount,
    IReadOnlyCollection<string> LeakedObjectTypeNames);
