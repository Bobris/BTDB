namespace BTDB.KVDBLayer
{
    internal interface IFileTransactionLog : IFileInfo
    {
        uint PreviousFileId { get; }
        uint NextFileId { get; set; }
    }
}