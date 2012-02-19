namespace BTDB.KV2DBLayer
{
    internal enum KV2CommandType : byte
    {
        CreateOrUpdate,
        EraseOne,
        EraseRange,
        EndOfTransaction
    }
}