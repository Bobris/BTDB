namespace BTDB.KV2DBLayer
{
    internal class UnknownFile : IFileInfo
    {
        internal static IFileInfo Instance = new UnknownFile();

        private UnknownFile() { }

        public KV2FileType FileType
        {
            get { return KV2FileType.Unknown; }
        }

        public long Generation
        {
            get { return -1; }
        }
    }
}