using BTDB.Allocators;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;
using BTDBTest;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Api;

/// ObjectDB suites from BTDBTest running on LsmKeyValueDB with tiny settings, so relations, secondary keys and
/// enumerators read across flushed tables, compactions, range tombstones and value files. ObjectDB code generation
/// shares static state, so these suites do not run in parallel (BTDBTest disables parallelization for the same reason).
[CollectionDefinition(Name, DisableParallelization = true)]
public static class LsmObjectDb
{
    public const string Name = "LsmObjectDb";

    public static IKeyValueDB Create() => LsmKeyValueDB.OpenAsync(new()
    {
        Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true, MemtableSize = 4096, BlockSize = 512,
        RestartInterval = 4, IndexPartitionSize = 128, PointDeleteRangeLimit = 2, Level0CompactionTrigger = 2,
        LevelBaseBytes = 16 * 1024, LevelMultiplier = 4, LevelCount = 5, TargetFileSize = 8192,
        ValueSeparationThreshold = 200, BlockCache = new LsmBlockCache(256 * 1024)
    }).AsTask().GetAwaiter().GetResult();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableTest(ITestOutputHelper output) : ObjectDbTableTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableScanTest(ITestOutputHelper output) : ObjectDbTableScanTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbRelationQueryTest(ITestOutputHelper output) : ObjectDbRelationQueryTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableInKeyValueTest(ITestOutputHelper output) : ObjectDbTableInKeyValueTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableUpdateByIdTest(ITestOutputHelper output) : ObjectDbTableUpdateByIdTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbSecondaryKeyBufferTest(ITestOutputHelper output) : ObjectDbSecondaryKeyBufferTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTableTupleTest(ITestOutputHelper output) : ObjectDbTableTupleTest(output)
{
    protected override IKeyValueDB CreateKeyValueDB() => LsmObjectDb.Create();
}

[Collection(LsmObjectDb.Name)]
public class LsmObjectDbTest : ObjectDbTest
{
    protected override IKeyValueDB CreateKeyValueDB(IOffHeapAllocator allocator) => LsmObjectDb.Create();
}
