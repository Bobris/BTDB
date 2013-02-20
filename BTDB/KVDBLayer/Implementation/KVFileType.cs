namespace BTDB.KVDBLayer
{
    internal enum KVFileType : byte
    {
        TransactionLog,
        KeyIndex,
        PureValues,
        PureValuesWithId,
        HashKeyIndex,
        Unknown,
    }
}