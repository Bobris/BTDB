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

    ValueTask<IKeyValueDBTransaction> StartWritingTransaction();

    string CalcStats();

    // This returns all zeros for Managed memory implementations
    (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats();

    // Returns true if there was big compaction (probably will need another one)
    bool Compact(CancellationToken cancellation);

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
