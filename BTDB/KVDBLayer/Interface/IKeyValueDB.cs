﻿using System;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer
{
    public interface IKeyValueDB: IDisposable
    {
        // Default are durable, not corrupting commits (true). In case of false and crash of OS or computer, database could became corrupted, and unopennable.
        bool DurableTransactions { get; set; }

        IKeyValueDBTransaction StartTransaction();

        IKeyValueDBTransaction StartReadOnlyTransaction();

        Task<IKeyValueDBTransaction> StartWritingTransaction();

        string CalcStats();

        // Returns true if there was big compaction (probably will need another one)
        bool Compact(CancellationToken cancellation);

        ulong? PreserveHistoryUpToCommitUlong { get; set; }

        IKeyValueDBLogger Logger { get; set; }
    }
}
