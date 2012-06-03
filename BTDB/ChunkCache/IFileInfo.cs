namespace BTDB.ChunkCache
{
    internal interface IFileInfo
    {
        DiskChunkFileType FileType { get; }
        long Generation { get; }
    }
}