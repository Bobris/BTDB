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

    /// Keep the memtable in native memory (default); false uses the managed reference implementation.
    public bool NativeMemtable { get; init; } = true;

    /// Target uncompressed data block size.
    public int BlockSize { get; init; } = 32 * 1024;

    public int RestartInterval { get; init; } = 16;
    public int BloomBitsPerKey { get; init; } = 10;
    public bool CompressBlocks { get; init; } = true;

    /// Block cache; a new 256 MiB cache is created when null. A cache may be shared by several databases.
    public LsmBlockCache? BlockCache { get; init; }

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

    /// Values longer than this are stored in value files when written to tables; int.MaxValue keeps all inline.
    public int ValueSeparationThreshold { get; init; } = 1024;

    /// Value files whose live bytes drop below this ratio are rewritten.
    public double ValueGcThreshold { get; init; } = 0.5;

    public IKeyValueDBLogger? Logger { get; init; }
}

public interface ILsmKeyValueDB : IKeyValueDB
{
    /// Writes the memtable to a new table and publishes it in the manifest.
    ValueTask FlushAsync(CancellationToken cancellation = default);
}
