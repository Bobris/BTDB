namespace BTDB.ChunkCache;

class UnknownFile : IFileInfo
{
    internal static IFileInfo Instance = new UnknownFile();

    UnknownFile() { }

    public DiskChunkFileType FileType => DiskChunkFileType.Unknown;

    public long Generation => -1;
}
