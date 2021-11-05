namespace BTDB.KVDBLayer;

struct KeyIndexInfo
{
    public uint Key;
    public long Generation;
    public ulong CommitUlong;
}
