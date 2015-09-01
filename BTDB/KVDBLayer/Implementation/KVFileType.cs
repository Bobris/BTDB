namespace BTDB.KVDBLayer
{
    enum KVFileType : byte
    {
        TransactionLog,
        KeyIndex,
        PureValues,
        PureValuesWithId,
        HashKeyIndex,
        Unknown,
    }
}