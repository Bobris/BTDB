namespace BTDB.KV2DBLayer
{
    internal class UnknownFile : IFileInfo
    {
        internal static readonly IFileInfo Instance = new UnknownFile();

        private UnknownFile() { }

        public KV2FileType FileType
        {
            get { return KV2FileType.Unknown; }
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