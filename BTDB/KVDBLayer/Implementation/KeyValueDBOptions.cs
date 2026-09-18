using BTDB.Allocators;

namespace BTDB.KVDBLayer;

public class KeyValueDBOptions
{
    public IFileCollection? FileCollection;
    public ICompressionStrategy? Compression = new SnappyCompressionStrategy();
    public IKviCompressionStrategy KviCompressionStrategy = new DefaultCompressionKviStrategy();
    /// Size target for PVLs and chunk storage, and for TRLs when no strategy is supplied.
    public uint FileSplitSize = int.MaxValue;
    /// Automatically size local files; a TransactionLogSizeStrategy still exclusively controls TRL limits.
    public bool AutoAdjustFileSize = false;

    /// Optional deterministic per-TRL soft and hard limits. Does not control PVL sizing.
    /// The strategy and its ID-to-size mapping must remain unchanged for the database's lifetime.
    public ITransactionLogSizeStrategy? TransactionLogSizeStrategy;
    /// Optional tracking of complete and acknowledged TRL positions for replication and compaction retention.
    public TransactionLogCapture? TransactionLogCapture;

    public ICompactorScheduler? CompactorScheduler = KVDBLayer.CompactorScheduler.Instance;
    public IKeyValueDBLogger? Logger;
    public ulong? OpenUpToCommitUlong;
    public ulong? PreserveHistoryUpToCommitUlong;
    public ulong? CompactorReadBytesPerSecondLimit;
    public ulong? CompactorWriteBytesPerSecondLimit;
    public IOffHeapAllocator? Allocator;
    public bool ReadOnly;

    /// Require explicit read-only or asynchronous writing transactions instead of StartTransaction.
    public bool RequireExplicitTransactions;

    /// Allocate new TRLs with odd IDs and all other files with even IDs. Rotate an existing even TRL before writing.
    public bool UseOddTransactionLogIds;

    /// If true it will try to recover data in DB as much as possible
    public bool LenientOpen;
}
