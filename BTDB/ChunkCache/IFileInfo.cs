namespace BTDB.ChunkCache;

interface IFileInfo
{
    DiskChunkFileType FileType { get; }
    long Generation { get; }
}
