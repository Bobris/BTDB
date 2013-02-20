namespace BTDB.KVDBLayer
{
    internal interface IHashKeyIndex : IFileInfo
    {
        uint KeyLen { get; }
    }
}