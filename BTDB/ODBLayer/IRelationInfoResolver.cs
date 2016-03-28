namespace BTDB.ODBLayer
{
    interface IRelationInfoResolver
    {
        uint GetLastPersistedVersion(uint id);
        RelationVersionInfo LoadRelationVersionInfo(uint id, uint version, string relationName);
    }
}