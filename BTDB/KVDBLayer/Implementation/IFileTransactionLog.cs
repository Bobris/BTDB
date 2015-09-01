namespace BTDB.KVDBLayer
{
    interface IFileTransactionLog : IFileInfo
    {
        uint PreviousFileId { get; }
        uint NextFileId { get; set; }
    }
}