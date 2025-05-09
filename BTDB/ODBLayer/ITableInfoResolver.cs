using BTDB.FieldHandler;
using BTDB.IOC;
using BTDB.Serialization;

namespace BTDB.ODBLayer;

public interface ITableInfoResolver
{
    uint GetLastPersistedVersion(uint id);
    TableVersionInfo LoadTableVersionInfo(uint id, uint version, string tableName);
    long GetSingletonOid(uint id);
    ulong AllocateNewOid();
    IFieldHandlerFactory FieldHandlerFactory { get; }
    ITypeConvertorGenerator TypeConvertorGenerator { get; }
    ITypeConverterFactory TypeConverterFactory { get; }
    IContainer? Container { get; }
    DBOptions ActualOptions { get; }
    IFieldHandlerLogger? FieldHandlerLogger { get; }
}
