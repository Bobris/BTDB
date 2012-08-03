namespace BTDB.KV2DBLayer
{
    internal interface IHashKeyIndex : IFileInfo
    {
        uint KeyLen { get; }
    }
}