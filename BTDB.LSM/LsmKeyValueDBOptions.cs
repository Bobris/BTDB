using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;

namespace BTDB.LSM;

public sealed record LsmKeyValueDBOptions
{
    public required ILsmStorage Storage { get; init; }

    /// Prefix of every object key, for example "tenant-a/db/". Empty by default.
    public string Prefix { get; init; } = "";

    /// Memtable size that triggers a flush to a new table. Also exposed as IKeyValueDB.MaxTrLogFileSize.
    public long MemtableSize { get; init; } = 64 * 1024 * 1024;

    /// Keep the memtable in native memory (default); false uses the managed reference implementation.
    public bool NativeMemtable { get; init; } = true;

    /// Most memtable bytes one writing transaction keeps in memory. A larger transaction moves its changes into tables
    /// only it reads (spills) and commits them by one manifest edit, as the newest level 0 tables, so memtable memory
    /// stays below about MemtableSize + 2 * MaxTransactionMemory whatever the transaction size.
    public long MaxTransactionMemory { get; init; } = 64L * 1024 * 1024;

    /// Most memtable bytes one writing transaction may add in total, in memory and spilled. A modification above it
    /// throws BTDBException and the transaction can only be rolled back. Unlimited by default.
    public long MaxTransactionSize { get; init; } = long.MaxValue;

    /// Target uncompressed data block size.
    public int BlockSize { get; init; } = 32 * 1024;

    public int RestartInterval { get; init; } = 16;

    /// Target encoded size of one index partition of a table; each has its own bloom filter partition.
    public int IndexPartitionSize { get; init; } = 16 * 1024;

    /// Bloom filter bits per key (false positive rate about 0.05% at 16, 0.007% at 20). Every write of a key must know
    /// whether the key exists, and a false positive costs one uncached ranged read, so object storage favors more bits
    /// than local-disk LSMs. Filters share the metadata part of the block cache (one eighth of it): more bits keep
    /// fewer filters resident in a database much larger than the cache.
    public int BloomBitsPerKey { get; init; } = 16;
    public bool CompressBlocks { get; init; } = true;

    /// Block cache; when null the database creates and disposes its own cache of BlockCacheSize bytes. A cache passed
    /// here may be shared by several databases and is not disposed by them.
    public LsmBlockCache? BlockCache { get; init; }

    public long BlockCacheSize { get; init; } = 256L * 1024 * 1024;

    /// Ranges with at most this many live keys are erased with point deletes instead of a range tombstone.
    public int PointDeleteRangeLimit { get; init; } = 64;

    /// Number of level 0 tables that triggers their compaction into level 1.
    public int Level0CompactionTrigger { get; init; } = 4;

    /// Target size of level 1; each deeper level is LevelMultiplier times larger.
    public long LevelBaseBytes { get; init; } = 256L * 1024 * 1024;

    public int LevelMultiplier { get; init; } = 10;

    /// Number of levels including level 0; the last one is the bottom level.
    public int LevelCount { get; init; } = 7;

    /// Target size of tables written by compaction.
    public long TargetFileSize { get; init; } = 64L * 1024 * 1024;

    public int TableTailReadSize { get; init; } = 64 * 1024;

    /// Flush and compaction run on a background worker (default). When false they run synchronously in the commit
    /// that seals the memtable, which is deterministic for tests.
    public bool BackgroundCompaction { get; init; } = true;

    /// Writers wait while this many sealed memtables are waiting for flush.
    public int MaxImmutableMemtables { get; init; } = 2;

    /// Writers wait while level 0 has this many tables.
    public int Level0StopWritesTrigger { get; init; } = 12;

    /// Become the writer while opening; otherwise the first StartWritingTransaction acquires the lease.
    public bool AcquireWriterLeaseOnOpen { get; init; }

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(15);

    /// The writer stops writing this long before its lease could expire.
    public TimeSpan LeaseSafetyMargin { get; init; } = TimeSpan.FromSeconds(2);

    /// Longest pause between attempts to take a lease another instance holds; the first attempts follow each other
    /// within milliseconds, so a released lease is taken over at once.
    public TimeSpan LeaseRetryInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    /// Target size of one WAL chunk; larger commits span several chunks.
    public int WalChunkTargetSize { get; init; } = 256 * 1024;

    /// Longest pause of an idle WAL uploader; a commit wakes it at once and commits arriving during an upload are
    /// grouped into the next one.
    public TimeSpan WalFlushInterval { get; init; } = TimeSpan.FromMilliseconds(20);

    /// The WAL uploader waits briefly before an upload, up to this long, when commits usually follow the previous
    /// upload closely: several writers that wait for durability commit again as soon as their upload completes, and
    /// their commits then share one upload instead of splitting into a small and a large one. It chooses the wait from
    /// when commits arrive, measured in real time, and keeps it at zero for one writer awaiting each commit and for
    /// independent commits, so their latency does not grow. The wait ends early once pending commits fill a batch;
    /// waits below 2 ms yield the thread in a loop. Zero uploads at once.
    public TimeSpan AdaptiveWalLingerMax { get; init; } = TimeSpan.FromMilliseconds(3);

    public int WalUploadParallelism { get; init; } = 4;

    /// Writers wait while more WAL bytes than this are waiting for upload.
    public long MaxUnflushedWalBytes { get; init; } = 64L * 1024 * 1024;

    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// Fixes the writer session number that manifests record, so tests can write deterministic objects; null draws a
    /// random one at every takeover.
    internal ulong? WriterSession { get; init; }

    /// CreateKvi and Compact fail when flushes and compactions have kept failing this long; the worker keeps retrying.
    public TimeSpan WorkerFailureTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// The writer deletes objects no current state references only after this long, so readers in other processes can
    /// finish reading them. It is also the longest a transaction in another process may keep its snapshot: older
    /// snapshots fail with BTDBException once they need a deleted object. The pending deletions are kept in the
    /// manifest, so the period does not restart when another instance takes over the writer lease.
    public TimeSpan DeletionGracePeriod { get; init; } = TimeSpan.FromDays(1);

    /// First pause of WaitForCommitUlongAsync between refreshes; it doubles up to one second.
    public TimeSpan RefreshRetryInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    /// A reader lists manifests when probing the next one misses, at most this often.
    public TimeSpan ManifestListInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// Values longer than this are stored in value files when written to tables; int.MaxValue keeps all inline.
    public int ValueSeparationThreshold { get; init; } = 1024;

    /// Value files whose live bytes drop below this ratio are rewritten.
    public double ValueGcThreshold { get; init; } = 0.5;

    public IKeyValueDBLogger? Logger { get; init; }
}

public interface ILsmKeyValueDB : IKeyValueDB
{
    /// Makes every commit so far durable in the WAL.
    ValueTask FlushAsync(CancellationToken cancellation = default);

    /// Completes when the commit with the given transaction number is durable in the WAL. When omitted or null,
    /// waits for the latest transaction committed at the time of this call; later commits and uncommitted changes
    /// are not included.
    ValueTask WaitForDurabilityAsync(long? transactionNumber = null, CancellationToken cancellation = default);

    /// Becomes the writer: waits for the lease, fences the previous writer and catches up.
    ValueTask AcquireWriterLeaseAsync(CancellationToken cancellation = default);

    /// Hands the writer role over: waits for a running writing transaction, makes every commit durable, releases the
    /// lease and continues as a reader, so another instance can take over at once. Flushes and compactions are left
    /// to the next writer. A later StartWritingTransaction acquires the lease again.
    ValueTask ReleaseWriterLeaseAsync(CancellationToken cancellation = default);

    bool HoldsWriterLease { get; }

    /// Catches up to the newest durable state written by the writer; returns its CommitUlong. Existing transactions
    /// keep their snapshots. The writer is always current.
    ValueTask<ulong> RefreshAsync(CancellationToken cancellation = default);

    /// Refreshes until CommitUlong is at least commitUlong.
    ValueTask WaitForCommitUlongAsync(ulong commitUlong, CancellationToken cancellation = default);
}
