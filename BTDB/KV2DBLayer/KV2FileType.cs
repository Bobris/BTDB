namespace BTDB.KV2DBLayer
{
    internal enum KV2FileType : byte
    {
        TransactionLog,
        KeyIndex,
        PureValues,
        PureValuesWithId,
        HashKeyIndex,
        Unknown,
    }
}