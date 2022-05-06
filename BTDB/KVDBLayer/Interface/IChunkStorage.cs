namespace BTDB.KVDBLayer;

interface IChunkStorage
{
    IChunkStorageTransaction StartTransaction();
}
