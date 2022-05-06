namespace BTDB.ODBLayer;

public interface IObjectDBLogger
{
    void ReportIncompatiblePrimaryKey(string relationName, string field);
}
