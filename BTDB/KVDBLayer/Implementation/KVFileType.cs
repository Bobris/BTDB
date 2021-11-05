namespace BTDB.KVDBLayer;

public enum KVFileType : byte
{
    TransactionLog,
    KeyIndex,
    PureValues,
    PureValuesWithId,
    HashKeyIndex,
    KeyIndexWithCommitUlong,
    ModernKeyIndex,
    ModernKeyIndexWithUlongs,
    Unknown,
}
