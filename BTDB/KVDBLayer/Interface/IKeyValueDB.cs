using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer
{
    public interface IKeyValueDB: IDisposable
    {
        // Default are durable, not corrupting commits (true). In case of false and crash of OS or computer, transactions could lost, but it should be possible to open DB.
        bool DurableTransactions { get; set; }

        IKeyValueDBTransaction StartTransaction();

        IKeyValueDBTransaction StartReadOnlyTransaction();

        ValueTask<IKeyValueDBTransaction> StartWritingTransaction();

        string CalcStats();

        // Returns true if there was big compaction (probably will need another one)
        bool Compact(CancellationToken cancellation);

        ulong? PreserveHistoryUpToCommitUlong { get; set; }

        IKeyValueDBLogger Logger { get; set; }

        // Try to limit additional memory for Compactor. Setting this value higher can speed up compactor.
        // Current default is 200MB. It will always do at least one iteration so it will make progress.
        uint CompactorRamLimitInMb { get; set; }
    }
}
