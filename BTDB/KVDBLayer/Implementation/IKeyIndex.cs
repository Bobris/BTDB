namespace BTDB.KVDBLayer
{
    interface IKeyIndex : IFileInfo
    {
        uint TrLogFileId { get; }
        uint TrLogOffset { get; }
        long KeyValueCount { get; }
        ulong CommitUlong { get; }
        KeyIndexCompression Compression { get; }
        ulong[] Ulongs { get; }
    }
}