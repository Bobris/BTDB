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
            Storage = storage, MemtableSize = memtableSize, BlockSize = 256, RestartInterval = 4, MaxTables = 3,
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
        var tablesAfter = await CountTables(storage);
        Assert.True(tablesAfter < tablesWhileSnapshotLives, $"{tablesAfter} >= {tablesWhileSnapshotLives}");
        Assert.True(tablesAfter <= 4, $"{tablesAfter} tables remain");
    }

    static async Task<int> CountTables(InMemoryLsmStorage storage)
    {
        var count = 0;
        await foreach (var _ in storage.ListAsync("tables/", default)) count++;
        return count;
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
