namespace BTDB.ChunkCache
{
    class UnknownFile : IFileInfo
    {
        internal static IFileInfo Instance = new UnknownFile();

        UnknownFile() { }

        public DiskChunkFileType FileType
        {
            get { return DiskChunkFileType.Unknown; }
        }

        public long Generation
        {
            get { return -1; }
        }
    }
}