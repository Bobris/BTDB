namespace BTDB.KV2DBLayer
{
    internal enum KV2FileType : byte
    {
        TransactionLog,
        TransactionLogContinuation,
        KeyIndex,
        Unknown
    }
}