namespace BTDB.KVDBLayer;

public interface IKeyIndex : IFileInfo
{
    uint TrLogFileId { get; }
    uint TrLogOffset { get; }
    long KeyValueCount { get; }
    ulong CommitUlong { get; }
    KeyIndexCompression Compression { get; }
    ulong[]? Ulongs { get; }
    long[]? UsedFilesInOlderGenerations { get; set; }
}
