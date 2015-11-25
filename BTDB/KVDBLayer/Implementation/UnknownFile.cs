namespace BTDB.KVDBLayer
{
    class UnknownFile : IFileInfo
    {
        internal static readonly IFileInfo Instance = new UnknownFile();

        UnknownFile() { }

        public KVFileType FileType => KVFileType.Unknown;

        public long Generation => -1;

        public long SubDBId => -1;
    }
}