namespace BTDB.KV2DBLayer
{
    internal class UnknownFile : IFileInfo
    {
        public KV2FileType FileType
        {
            get { return KV2FileType.Unknown; }
        }
    }
}