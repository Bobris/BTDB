using System;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

public class LsmKeyValueDBPersistenceTest
{
    static LsmKeyValueDB Open(ILsmStorage storage, long memtableSize = 64 * 1024 * 1024) =>
        LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, DeletionGracePeriod = TimeSpan.Zero, MemtableSize = memtableSize, BlockSize = 256, RestartInterval = 4, IndexPartitionSize = 64, Level0CompactionTrigger = 3,
            LevelBaseBytes = 4096, LevelMultiplier = 3, LevelCount = 5, TargetFileSize = 2048,
            PointDeleteRangeLimit = 2
        }).AsTask().GetAwaiter().GetResult();

    static byte[] Key(int i) => [(byte)(i >> 8), (byte)i];

    [Fact]
    public async Task FlushedDataAndMetadataSurviveReopen()
    {
        var storage = new InMemoryLsmStorage();
        using (var db = Open(storage))
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 500; i++) cursor.CreateOrUpdateKeyValue(Key(i), [(byte)i]);
            Assert.True(cursor.FindKeyIndex(100));
            Assert.True(cursor.FindKeyIndex(199) && cursor.FindKeyIndex(100));
            using var to = tr.CreateCursor();
            Assert.True(to.FindKeyIndex(199));
            Assert.True(cursor.FindKeyIndex(100));
            Assert.Equal(100, cursor.EraseUpTo(to));
            tr.SetCommitUlong(42);
            tr.SetUlong(1, 7);
            tr.Commit();
        }

        using (var db = Open(storage))
        {
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(400, tr.GetKeyValueCount());
            Assert.Equal(42ul, tr.GetCommitUlong());
            Assert.Equal(7ul, tr.GetUlong(1));
            Assert.Equal(1, tr.GetTransactionNumber());
            using var cursor = tr.CreateCursor();
            Assert.True(cursor.FindKeyIndex(100));
            Assert.Equal(Key(200), cursor.SlowGetKey());
            Assert.Equal(new[] { (byte)200 }, cursor.SlowGetValue());
        }
    }

    [Fact]
    public async Task MergedTablesAreDeletedOnceNoSnapshotUsesThem()
    {
        var storage = new InMemoryLsmStorage();
        using var db = Open(storage, memtableSize: 1024);
        var oldSnapshot = db.StartReadOnlyTransaction();
        for (var i = 0; i < 300; i++)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key(i % 50), new byte[100]);
            tr.Commit();
        }

        using (var tr = db.StartReadOnlyTransaction()) Assert.Equal(50, tr.GetKeyValueCount());
        var tablesWhileSnapshotLives = await CountTables(storage);
        Assert.Equal(0, oldSnapshot.GetKeyValueCount());
        oldSnapshot.Dispose();
        // The background worker deletes what no snapshot uses within about a second, and compaction may still be
        // catching up on a loaded machine.
        var tablesAfter = await CountTables(storage);
        for (var wait = 0; wait < 100 && tablesAfter > 4; wait++)
        {
            await Task.Delay(100);
            tablesAfter = await CountTables(storage);
        }

        Assert.True(tablesAfter < tablesWhileSnapshotLives, $"{tablesAfter} >= {tablesWhileSnapshotLives}");
        Assert.True(tablesAfter <= 4, $"{tablesAfter} tables remain");
    }

    static async Task<int> CountTables(InMemoryLsmStorage storage) => await Count(storage, "tables/");

    static async Task<int> Count(InMemoryLsmStorage storage, string prefix)
    {
        var count = 0;
        await foreach (var _ in storage.ListAsync(prefix, default)) count++;
        return count;
    }

    [Fact]
    public async Task OverwrittenSeparatedValuesAreCollected()
    {
        var storage = new InMemoryLsmStorage();
        var options = new LsmKeyValueDBOptions
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, DeletionGracePeriod = TimeSpan.Zero, MemtableSize = 8 * 1024, BlockSize = 512, Level0CompactionTrigger = 2,
            LevelBaseBytes = 16 * 1024, LevelMultiplier = 4, LevelCount = 4, TargetFileSize = 16 * 1024,
            ValueSeparationThreshold = 100, ValueGcThreshold = 0.5
        };
        var maxValueFiles = 0;
        using (var db = await LsmKeyValueDB.OpenAsync(options))
        {
            for (var round = 0; round < 40; round++)
            {
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 20; i++)
                {
                    var value = new byte[300];
                    value[0] = (byte)round;
                    value[1] = (byte)i;
                    cursor.CreateOrUpdateKeyValue(Key(i), value);
                }

                tr.Commit();
                maxValueFiles = System.Math.Max(maxValueFiles, await Count(storage, "values/"));
            }

            await db.Compact(default);
        }

        // 40 rounds of 6 KB each would leave many files without collection; after a full compaction only live ones stay.
        Assert.True(await Count(storage, "values/") <= 2, $"{await Count(storage, "values/")} value files remain");
        Assert.True(maxValueFiles < 20, $"up to {maxValueFiles} value files existed");
        using (var db = await LsmKeyValueDB.OpenAsync(options))
        {
            using var tr = db.StartReadOnlyTransaction();
            using var cursor = tr.CreateCursor();
            Assert.Equal(20, tr.GetKeyValueCount());
            for (var i = 0; i < 20; i++)
            {
                Assert.Equal(FindResult.Exact, cursor.Find(Key(i), 0));
                var value = cursor.SlowGetValue();
                Assert.Equal(300, value.Length);
                Assert.Equal(39, value[0]);
                Assert.Equal(i, value[1]);
                Assert.Equal(300u, cursor.GetStorageSizeOfCurrentKey().Value);
            }
        }
    }

    [Fact]
    public async Task ReaderSnapshotIsStableAcrossFlushAndMerge()
    {
        var storage = new InMemoryLsmStorage();
        using var db = Open(storage, memtableSize: 512);
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 20; i++) cursor.CreateOrUpdateKeyValue(Key(i), [1]);
            tr.Commit();
        }

        using var snapshot = db.StartReadOnlyTransaction();
        for (var round = 0; round < 50; round++)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            cursor.FindFirstKey([]);
            cursor.EraseCurrent();
            cursor.CreateOrUpdateKeyValue(Key(1000 + round), new byte[64]);
            tr.Commit();
        }

        using var snapshotCursor = snapshot.CreateCursor();
        Assert.Equal(20, snapshot.GetKeyValueCount());
        Assert.True(snapshotCursor.FindFirstKey([]));
        var keys = 1;
        var lastKey = snapshotCursor.SlowGetKey();
        while (snapshotCursor.FindNextKey([]))
        {
            keys++;
            lastKey = snapshotCursor.SlowGetKey();
        }

        Assert.Equal(20, keys);
        Assert.Equal(Key(19), lastKey);
    }
}
