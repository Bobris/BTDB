using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    internal interface ITableInfoResolver
    {
        uint GetLastPesistedVersion(uint id);
        TableVersionInfo LoadTableVersionInfo(uint id, uint version, string tableName);
        ulong? GetSingletonOid(uint id);
        ulong AllocateNewOid();
        IFieldHandlerFactory FieldHandlerFactory { get; }
        ITypeConvertorGenerator TypeConvertorGenerator { get; }
    }
}