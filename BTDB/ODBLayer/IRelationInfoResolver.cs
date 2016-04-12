using BTDB.FieldHandler;

namespace BTDB.ODBLayer
{
    interface IRelationInfoResolver
    {
        uint GetLastPersistedVersion(uint id);
        RelationVersionInfo LoadRelationVersionInfo(uint id, uint version, string relationName);
        IFieldHandlerFactory FieldHandlerFactory { get; }
        ITypeConvertorGenerator TypeConvertorGenerator { get; }
    }
}