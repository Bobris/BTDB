namespace BTDB.KVDBLayer
{
    internal interface IKeyIndex : IFileInfo
    {
        uint TrLogFileId { get; }
        uint TrLogOffset { get; }
        long KeyValueCount { get; }
    }
}