namespace BTDB.ODBLayer
{
    public interface IMidLevelObject
    {
        string TableName { get; }
        uint TableId { get; }
        ulong Oid { get; }
        bool Deleted { get; }
        void Delete();
    }
}