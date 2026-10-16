using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;
using BTDBTest;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Api;

/// The shared IKeyValueDB contract suite with default options: everything stays in the memtable for small tests.
public class LsmKeyValueDBTest(ITestOutputHelper testOutputHelper) : KeyValueDBTestBase(testOutputHelper)
{
    protected override IKeyValueDB NewKeyValueDB() =>
        LsmKeyValueDB.OpenAsync(new() { Storage = new InMemoryLsmStorage() }).AsTask().GetAwaiter().GetResult();
}

/// The shared suite with a tiny memtable and tiny blocks, so tests constantly flush, merge tables, use multi-block
/// tables, range tombstones and rank or select across several components.
public class LsmKeyValueDBTinyTest(ITestOutputHelper testOutputHelper) : KeyValueDBTestBase(testOutputHelper)
{
    protected override IKeyValueDB NewKeyValueDB() => LsmKeyValueDB.OpenAsync(new()
    {
        Storage = new InMemoryLsmStorage(), MemtableSize = 2048, BlockSize = 256, RestartInterval = 4, IndexPartitionSize = 64,
        PointDeleteRangeLimit = 2, Level0CompactionTrigger = 2, LevelBaseBytes = 8192, LevelMultiplier = 4,
        LevelCount = 5, TargetFileSize = 4096, BlockCache = new LsmBlockCache(64 * 1024)
    }).AsTask().GetAwaiter().GetResult();
}

/// The shared suite with tiny settings and every non-empty value stored in value files, so reads, compaction copies
/// handles and value-file garbage collection are exercised by every test.
public class LsmKeyValueDBSeparatedValuesTest(ITestOutputHelper testOutputHelper) : KeyValueDBTestBase(testOutputHelper)
{
    protected override IKeyValueDB NewKeyValueDB() => LsmKeyValueDB.OpenAsync(new()
    {
        Storage = new InMemoryLsmStorage(), MemtableSize = 2048, BlockSize = 256, RestartInterval = 4, IndexPartitionSize = 64,
        PointDeleteRangeLimit = 2, Level0CompactionTrigger = 2, LevelBaseBytes = 8192, LevelMultiplier = 4,
        LevelCount = 5, TargetFileSize = 4096, ValueSeparationThreshold = 0, ValueGcThreshold = 0.6,
        BlockCache = new LsmBlockCache(64 * 1024)
    }).AsTask().GetAwaiter().GetResult();
}
