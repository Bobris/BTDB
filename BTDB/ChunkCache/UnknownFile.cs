namespace BTDB.ChunkCache
{
    internal class UnknownFile : IFileInfo
    {
        internal static IFileInfo Instance = new UnknownFile();

        private UnknownFile() { }

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