namespace BTDB.KV2DBLayer
{
    internal interface IFileInfo
    {
        KV2FileType FileType { get; }
        long Generation { get; }
        long SubDBId { get; }
    }
}