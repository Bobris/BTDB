namespace BTDB.KVDBLayer
{
    class UnknownFile : IFileInfo
    {
        internal static readonly IFileInfo Instance = new UnknownFile();

        UnknownFile() { }

        public KVFileType FileType
        {
            get { return KVFileType.Unknown; }
        }

        public long Generation
        {
            get { return -1; }
        }

        public long SubDBId
        {
            get { return -1; }
        }
    }
}