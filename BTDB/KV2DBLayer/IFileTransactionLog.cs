namespace BTDB.KV2DBLayer
{
    internal interface IFileTransactionLog
    {
        int PreviousFileId { get; }
        int NextFileId { get; set; }
        long StartingTransactionId { get; set; }
    }
}