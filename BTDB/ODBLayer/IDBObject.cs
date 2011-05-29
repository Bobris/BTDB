namespace BTDB.ODBLayer
{
    public interface IDBObject
    {
        string TableName { get; }
        uint TableId { get; }
        ulong Oid { get; }
        bool Deleted { get; }
        void Delete();
        IObjectDBTransaction OwningTransaction { get; }
    }
}