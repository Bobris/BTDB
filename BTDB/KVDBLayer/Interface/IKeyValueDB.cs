using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer;

public interface IKeyValueDB : IDisposable
{
    // Default are durable, not corrupting commits (true). In case of false and crash of OS or computer, transactions could lost, but it should be possible to open DB.
    bool DurableTransactions { get; set; }

    IKeyValueDBTransaction StartTransaction();

    IKeyValueDBTransaction StartReadOnlyTransaction();

    /// <summary>
    /// Start a serialized writer. With inBatch, reuse a pending writable BTree where supported.
    /// Otherwise finish the pending batch before starting this writer, waiting for the active writer if necessary.
    /// </summary>
    ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false);

    /// <summary>
    /// Finish the current batch immediately if idle, otherwise after the active writer commits or rolls back,
    /// before the next queued writer starts. Returns immediately without committing the active transaction.
    /// </summary>
    void FinishTransactionBatchAfterCurrentTransaction() { }

    string CalcStats();

    // This returns all zeros for Managed memory implementations
    (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats();

    // Returns true if there was big compaction (probably will need another one)
    ValueTask<bool> Compact(CancellationToken cancellation);

    void CreateKvi(CancellationToken cancellation);

    ulong? PreserveHistoryUpToCommitUlong { get; set; }

    IKeyValueDBLogger? Logger { get; set; }

    // Try to limit additional memory for Compactor. Setting this value higher can speed up compactor.
    // Current default is 200MB. It will always do at least one iteration so it will make progress.
    uint CompactorRamLimitInMb { get; set; }

    // Transaction Log files will try to be split at this size, can be modified during running (maximum size is int.MaxValue)
    long MaxTrLogFileSize { get; set; }

    IEnumerable<IKeyValueDBTransaction> Transactions();

    public ulong CompactorReadBytesPerSecondLimit { get; set; }
    public ulong CompactorWriteBytesPerSecondLimit { get; set; }
}
