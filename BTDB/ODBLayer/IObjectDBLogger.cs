namespace BTDB.ODBLayer;

public interface IObjectDBLogger
{
    void ReportIncompatiblePrimaryKey(string relationName, string field);

    void ReportSkippedUnknownType(string typeName)
    {
    }
}
