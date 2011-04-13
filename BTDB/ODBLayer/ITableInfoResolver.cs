namespace BTDB.ODBLayer
{
    internal interface ITableInfoResolver
    {
        uint GetLastPesistedVersion(uint id);
        TableVersionInfo LoadTableVersionInfo(uint id, uint version);
        ulong GetSingletonOid(uint id);
    }
}