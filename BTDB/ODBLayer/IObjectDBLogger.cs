using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface IObjectDBLogger
{
    void ReportIncompatiblePrimaryKey(string relationName, string field);

    void ReportSkippedUnknownType(string typeName)
    {
    }

    void CompactorRemovedLeaks(IReadOnlyCollection<string> leakedObjectTypeNames, ulong removedKeyCount,
        TimeSpan elapsed)
    {
    }

    void CompactorDetectedLeaks(IReadOnlyCollection<string> leakedObjectTypeNames, ulong leakedKeyCount,
        TimeSpan elapsed)
    {
    }
}
