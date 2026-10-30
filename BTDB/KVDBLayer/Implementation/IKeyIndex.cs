namespace BTDB.KVDBLayer;

public interface IKeyIndex : IFileInfo
{
    uint TrLogFileId { get; }
    uint TrLogOffset { get; }
    long KeyValueCount { get; }
    ulong CommitUlong { get; }
    KeyIndexCompression Compression { get; }
    /// The file ends with the XxHash64 of its content after the end marker.
    bool HasChecksum => false;
    ulong[]? Ulongs { get; }
    long[]? UsedFilesInOlderGenerations { get; set; }
}
