namespace BTDB.KV2DBLayer
{
    interface IChunkStorage
    {
        IChunkStorageTransaction StartTransaction();
    }
}