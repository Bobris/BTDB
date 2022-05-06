namespace BTDB.KVDBLayer;

public interface IFileTransactionLog : IFileInfo
{
    uint PreviousFileId { get; }
    uint NextFileId { get; set; }
}
