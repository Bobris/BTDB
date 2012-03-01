namespace BTDB.KV2DBLayer
{
    internal interface IFileTransactionLog : IFileInfo
    {
        int PreviousFileId { get; }
        int NextFileId { get; set; }
    }
}