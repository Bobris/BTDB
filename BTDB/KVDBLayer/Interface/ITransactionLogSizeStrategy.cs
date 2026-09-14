namespace BTDB.KVDBLayer;

/// Deterministic limits derived only from the numeric TRL file ID. The mapping must remain
/// identical across nodes, restarts and application versions for the database's lifetime.
public interface ITransactionLogSizeStrategy
{
    TransactionLogSizeLimits GetLimits(uint transactionLogFileId);
}

/// SoftLimit triggers rotation before the next transaction once reached. HardLimit bounds
/// the complete file, including its header and terminators, and can split a transaction
/// between commands. Commands themselves cannot be split and must fit the destination file.
/// Both limits are in bytes: 1024 &lt;= SoftLimit &lt;= HardLimit &lt; uint.MaxValue.
public readonly record struct TransactionLogSizeLimits(uint SoftLimit, uint HardLimit);
