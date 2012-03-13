namespace BTDB.KV2DBLayer
{
    internal interface IKeyIndex : IFileInfo
    {
        int TrLogFileId { get; }
        int TrLogOffset { get; }
        long KeyValueCount { get; }
    }
}