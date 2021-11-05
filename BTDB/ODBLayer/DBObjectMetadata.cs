namespace BTDB.ODBLayer;

public class DBObjectMetadata
{
    public DBObjectMetadata(ulong id, DBObjectState state)
    {
        Id = id;
        State = state;
    }

    public ulong Id { get; set; }
    public DBObjectState State { get; set; }
}
