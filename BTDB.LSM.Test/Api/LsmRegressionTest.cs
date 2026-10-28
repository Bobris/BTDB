using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Holds table creates until Gate is set, so a flush can be kept running.
sealed class GatedTableStorage(InMemoryLsmStorage inner) : ILsmStorage
{
    public readonly ManualResetEventSlim Gate = new(true);

    public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation)
    {
        if (key.StartsWith("tables/")) await Task.Run(() => Gate.Wait(cancellation), cancellation);
        return await inner.CreateAsync(key, content, cancellation);
    }

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) => inner.TryReadAsync(key, cancellation);
    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);
    public void ReadRange(string key, long offset, Span<byte> destination) => inner.ReadRange(key, offset, destination);
    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
    public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
}

/// Regressions found by code review; each test reproduced its bug before the fix.
public class LsmRegressionTest
{
    static byte[] Key(int i) => [(byte)(i >> 8), (byte)i];

    static async Task Commit(IKeyValueDB db, int from, int count, int valueSize = 50)
    {
        using var tr = await db.StartWritingTransaction();
        using var cursor = tr.CreateCursor();
        for (var i = from; i < from + count; i++) cursor.CreateOrUpdateKeyValue(Key(i), new byte[valueSize]);
        tr.SetCommitUlong((ulong)(from + count));
        tr.Commit();
    }

    static long Count(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        return tr.GetKeyValueCount();
    }

    /// A checkpoint during a running flush sealed an empty memtable; the flush then set the WAL start past later
    /// commits and deleted their chunks, so durable commits were lost after a crash.
    [Fact]
    public async Task CheckpointDuringFlushKeepsLaterWal()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new GatedTableStorage(inner);
        var options = new LsmKeyValueDBOptions
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, MemtableSize = 8 * 1024,
            WalFlushInterval = TimeSpan.FromMilliseconds(1), DeletionGracePeriod = TimeSpan.Zero, MaxImmutableMemtables = 6
        };
        var db = await LsmKeyValueDB.OpenAsync(options);
        storage.Gate.Reset();
        // The commit fills the memtable: its generation is sealed and its flush waits at the gate.
        await Commit(db, 0, 200);
        var checkpoint = Task.Run(() => db.CreateKvi(default));
        await Task.Delay(200);
        await Commit(db, 1000, 5);
        await db.FlushAsync();
        storage.Gate.Set();
        await checkpoint;
        var expected = Count(db);
        db.SimulateCrash();
        inner.ExpireAllLeases();
        using var reopened = await LsmKeyValueDB.OpenAsync(options with { Storage = inner });
        Assert.Equal(expected, Count(reopened));
    }

    /// Sealing the memtable did not change the commit sequence, so a transaction opened before a checkpoint upgraded
    /// to writing on the sealed memtable and its commit counted every entry twice.
    [Fact]
    public async Task UpgradeAfterCheckpointRetries()
    {
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false
        });
        await Commit(db, 0, 10);
        using var tr = db.StartTransaction();
        Assert.Equal(10, tr.GetKeyValueCount());
        db.CreateKvi(default);
        using (var cursor = tr.CreateCursor())
            Assert.Throws<BTDBTransactionRetryException>(() => cursor.CreateOrUpdateKeyValue(Key(500), [1]));
        Assert.Equal(10, Count(db));
    }

    /// A shared block cache keyed entries by file number only, which every database starts at 1.
    [Fact]
    public async Task SharedCacheKeepsDatabasesApart()
    {
        using var cache = new LsmBlockCache(1 << 20);
        LsmKeyValueDBOptions Options() => new()
        {
            Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false,
            BlockCache = cache
        };
        using var a = await LsmKeyValueDB.OpenAsync(Options());
        using var b = await LsmKeyValueDB.OpenAsync(Options());
        foreach (var (db, value) in new[] { (a, (byte)1), (b, (byte)2) })
        {
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue([7], [value]);
                tr.Commit();
            }

            db.CreateKvi(default);
        }

        foreach (var (db, value) in new[] { (a, (byte)1), (b, (byte)2) })
        {
            using var tr = db.StartReadOnlyTransaction();
            using var cursor = tr.CreateCursor();
            Assert.True(cursor.FindExactKey([7]));
            Assert.Equal(new[] { value }, cursor.SlowGetValue());
        }
    }

    /// A cursor whose key was erased stays fuzzy between its neighbors. A key suffix update that moves the next or
    /// previous key past the erased key keeps the key's index, so the cursor stays on the same side of it: renamed
    /// down, the cursor is right before the new key; renamed up, right after it. Before the fix the cursor kept its
    /// erased key, so it moved to the other side of the renamed key.
    [Theory]
    [InlineData("shrink", "previous", "07FFFF")]
    [InlineData("shrink", "next", "FF")]
    [InlineData("shrink", "index", "0")]
    [InlineData("grow", "previous", "FF09")]
    [InlineData("grow", "next", "-")]
    [InlineData("grow", "index", "1")]
    [InlineData("empty", "previous", "-")]
    [InlineData("empty", "next", "")]
    [InlineData("empty", "index", "-1")]
    public async Task KeySuffixUpdateKeepsFuzzyCursorOnItsSide(string rename, string probe, string expected)
    {
        Assert.Equal(expected, await Probe(new BTreeKeyValueDB(new InMemoryFileCollection()), rename, probe));
        Assert.Equal(expected, await Probe(await LsmKeyValueDB.OpenAsync(new()
            { Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true }), rename, probe));
    }

    /// A cursor whose erased key was created again is current again, so it follows that key's suffix update.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CursorOnRecreatedKeyFollowsKeySuffixUpdate(bool lsm)
    {
        IKeyValueDB db = lsm
            ? await LsmKeyValueDB.OpenAsync(new() { Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true })
            : new BTreeKeyValueDB(new InMemoryFileCollection());
        using (db)
        {
            using var tr = await db.StartWritingTransaction();
            using var renaming = tr.CreateCursor();
            using var follower = tr.CreateCursor();
            renaming.CreateOrUpdateKeyValue([0x01], [1]);
            renaming.CreateOrUpdateKeyValue([0xFF, 0x05], [2]);
            Assert.True(follower.FindExactKey([0xFF, 0x05]));
            follower.EraseCurrent();
            renaming.CreateOrUpdateKeyValue([0xFF, 0x05], [3]);
            Assert.Equal(UpdateKeySuffixResult.Updated, renaming.UpdateKeySuffix([0xFF], 1));
            Assert.Equal(new byte[] { 0xFF }, follower.SlowGetKey());
            Assert.Equal(1, follower.GetKeyIndex());
        }
    }

    static async Task<string> Probe(IKeyValueDB db, string rename, string probe)
    {
        using (db)
        {
            var (keys, erased, key, prefixLen) = rename switch
            {
                "shrink" => (new byte[][] { [0x07, 0xFF, 0xFF], [0xFF, 0x01], [0xFF, 0xFF] }, new byte[] { 0xFF, 0x01 },
                    new byte[] { 0xFF }, 1u),
                "grow" => ([[0x00], [0xFF], [0xFF, 0x05]], [0xFF, 0x05], [0xFF, 0x09], 1u),
                _ => ([[0xFF, 0x05], [0xFF, 0xFF]], [0xFF, 0x05], [], 0u)
            };
            using var tr = await db.StartWritingTransaction();
            using var renaming = tr.CreateCursor();
            using var fuzzy = tr.CreateCursor();
            foreach (var k in keys) renaming.CreateOrUpdateKeyValue(k, [1]);
            Assert.True(fuzzy.FindExactKey(erased));
            fuzzy.EraseCurrent();
            Assert.Equal(UpdateKeySuffixResult.Updated, renaming.UpdateKeySuffix(key, prefixLen));
            return probe switch
            {
                "previous" => fuzzy.FindPreviousKey([]) ? Convert.ToHexString(fuzzy.SlowGetKey()) : "-",
                "next" => fuzzy.FindNextKey([]) ? Convert.ToHexString(fuzzy.SlowGetKey()) : "-",
                _ => fuzzy.GetKeyIndex().ToString()
            };
        }
    }
}

sealed class FailingReadStorage(InMemoryLsmStorage inner) : ILsmStorage
{
    public volatile bool Fail;

    void Check()
    {
        if (Fail) throw new System.IO.IOException("Simulated read failure.");
    }

    public ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation) => inner.CreateAsync(key, content, cancellation);
    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) => inner.TryReadAsync(key, cancellation);

    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation)
    {
        Check();
        return inner.ReadRangeAsync(key, offset, destination, cancellation);
    }

    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        Check();
        inner.ReadRange(key, offset, destination);
    }

    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
    public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
}

public class LsmReadaheadRegressionTest
{
    /// Readahead registered blocks as being read before it resolved the location of later blocks; when loading a later
    /// index partition failed, the registered blocks were never completed and every read of them hung.
    [Fact]
    public async Task FailedReadaheadDoesNotBlockLaterReads()
    {
        var inner = new InMemoryLsmStorage();
        var entries = new List<Engine.LsmEntry>();
        for (var i = 0; i < 2000; i++)
            entries.Add(new([(byte)(i >> 8), (byte)i], true, false, new byte[40], false));
        var content = TableBuilder.Build(entries, Engine.TombstoneSet.Empty,
            new() { BlockSize = 256, IndexPartitionSize = 64 });
        await inner.CreateAsync("tables/1.sst", content, default);
        var storage = new FailingReadStorage(inner);
        using var cache = new LsmBlockCache(16 << 20);
        var table = TableReader.Open(storage, cache, 1, 1, "tables/1.sst", content.Length);
        Assert.True(table.PartitionCount > 2);
        using (table.PinBlock(0))
        {
        }

        storage.Fail = true;
        Assert.ThrowsAny<Exception>(() => table.Readahead(1, 1, long.MaxValue));
        storage.Fail = false;
        var read = Task.Run(() =>
        {
            using var pin = table.PinBlock(1);
            return pin.Span.Length;
        });
        Assert.True(await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(10))) == read,
            "Reading a block after a failed readahead hung.");
    }
}
