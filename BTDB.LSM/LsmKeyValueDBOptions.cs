using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;

namespace BTDB.LSM;

public sealed class LsmKeyValueDBOptions
{
    public required ILsmStorage Storage { get; init; }

    /// Prefix of every object key, for example "tenant-a/db/". Empty by default.
    public string Prefix { get; init; } = "";

    /// Memtable size that triggers a flush to a new table. Also exposed as IKeyValueDB.MaxTrLogFileSize.
    public long MemtableSize { get; init; } = 64 * 1024 * 1024;

    /// Target uncompressed data block size.
    public int BlockSize { get; init; } = 32 * 1024;

    public int RestartInterval { get; init; } = 16;
    public int BloomBitsPerKey { get; init; } = 10;
    public bool CompressBlocks { get; init; } = true;

    /// Block cache; a new 256 MiB cache is created when null. A cache may be shared by several databases.
    public LsmBlockCache? BlockCache { get; init; }

    /// Ranges with at most this many live keys are erased with point deletes instead of a range tombstone.
    public int PointDeleteRangeLimit { get; init; } = 64;

    /// Number of tables that triggers merging them into one (L1 placeholder for leveled compaction).
    public int MaxTables { get; init; } = 8;

    public int TableTailReadSize { get; init; } = 64 * 1024;

    public IKeyValueDBLogger? Logger { get; init; }
}

public interface ILsmKeyValueDB : IKeyValueDB
{
    /// Writes the memtable to a new table and publishes it in the manifest.
    ValueTask FlushAsync(CancellationToken cancellation = default);
}
