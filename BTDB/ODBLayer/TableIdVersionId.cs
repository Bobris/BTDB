using System;

namespace BTDB.ODBLayer;

public readonly struct TableIdVersionId : IEquatable<TableIdVersionId>
{
    public readonly uint TableId;
    public readonly uint VersionId;

    public TableIdVersionId(uint tableid, uint version)
    {
        TableId = tableid;
        VersionId = version;
    }

    public bool Equals(TableIdVersionId other)
    {
        return TableId == other.TableId && VersionId == other.VersionId;
    }

    public override int GetHashCode()
    {
        return (int)(TableId * 33 + VersionId);
    }
}
