using System.Collections.Generic;

namespace BTDB.ODBLayer;

public class ODBIteratorRelationInfo
{
    public uint Id;
    public string Name;
    public IReadOnlyDictionary<uint, RelationVersionInfo> VersionInfos;
    public uint LastPersistedVersion;
    public long RowCount;
}
