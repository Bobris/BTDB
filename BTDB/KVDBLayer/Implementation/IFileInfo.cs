namespace BTDB.KVDBLayer
{
    interface IFileInfo
    {
        KVFileType FileType { get; }
        long Generation { get; }
        long SubDBId { get; }
    }
}