namespace BTDB.KVDBLayer
{
    internal interface IFileInfo
    {
        KVFileType FileType { get; }
        long Generation { get; }
        long SubDBId { get; }
    }
}