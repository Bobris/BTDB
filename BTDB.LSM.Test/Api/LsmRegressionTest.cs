using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
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

    /// FastIterate from a cursor whose erased key was the last one has nothing to visit. Before the fix it threw
    /// InvalidOperationException, while BTreeKeyValueDB returns.
    [Fact]
    public async Task FastIterateAfterErasingTheLastKeyVisitsNothing()
    {
        using var db = await LsmKeyValueDB.OpenAsync(new()
            { Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true });
        using var tr = await db.StartWritingTransaction();
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([1], [1]);
        cursor.CreateOrUpdateKeyValue([2], [2]);
        cursor.EraseCurrent();
        var visited = 0;
        Span<byte> buffer = default;
        cursor.FastIterate(ref buffer, (_, _) =>
        {
            visited++;
            return false;
        });
        Assert.Equal(0, visited);
        Assert.False(cursor.IsValid());
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

    // Before the fix opening parsed every object name under wal/ as a chunk number, so a stray object such as the
    // .DS_Store Finder leaves in a folder failed every open with FormatException, and it took the last object listed
    // under manifest/ for the newest manifest, so a stray name sorting after the digits failed decoding.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StrayObjectsBesideWalChunksAndManifestsAreIgnored(bool flushed)
    {
        var storage = new InMemoryLsmStorage();
        LsmKeyValueDBOptions Options() => new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, MemtableSize = 1024, BackgroundCompaction = false
        };
        using (var db = await LsmKeyValueDB.OpenAsync(Options()))
        {
            await Commit(db, 0, flushed ? 100 : 3);
            await db.FlushAsync();
        }

        foreach (var key in new[] { "wal/.DS_Store", "wal/notes.txt", "manifest/README", "manifest/.DS_Store" })
            await storage.CreateAsync(key, new byte[] { 1, 2, 3 }, default);
        using (var db = await LsmKeyValueDB.OpenAsync(Options()))
        {
            Assert.Equal(flushed ? 100 : 3, Count(db));
            await Commit(db, 200, 1);
        }

        using (var db = await LsmKeyValueDB.OpenAsync(Options()))
            Assert.Equal((flushed ? 100 : 3) + 1, Count(db));
    }

    // Before the fix a level target past long.MaxValue wrapped: with a multiplier of 1000 level 5 got a negative
    // target, so its score was negative and it never compacted down.
    [Theory]
    [InlineData(256L * 1024 * 1024, 1000)]
    [InlineData(1L << 32, 1 << 16)]
    [InlineData(long.MaxValue, 2)]
    public void LevelTargetSaturatesInsteadOfWrapping(long levelBaseBytes, int levelMultiplier)
    {
        var options = new Engine.LsmCompactionOptions { LevelBaseBytes = levelBaseBytes, LevelMultiplier = levelMultiplier };
        var previous = Engine.LsmCompaction.LevelTarget(options, 1);
        Assert.Equal(levelBaseBytes, previous);
        for (var level = 2; level < 8; level++)
        {
            var target = Engine.LsmCompaction.LevelTarget(options, level);
            Assert.True(target >= previous, $"level {level}: {target} < {previous}");
            previous = target;
        }

        Assert.Equal(long.MaxValue, previous);
    }

    sealed class MutableClock : TimeProvider
    {
        public DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    sealed class ScriptedStorage(InMemoryLsmStorage inner) : ILsmStorage
    {
        public Func<string, ValueTask<LsmWriteOutcome?>>? OnCreate { get; set; }
        public Func<string, CancellationToken, ValueTask>? OnList { get; set; }
        public Func<string, string, TimeSpan, ValueTask<bool?>>? OnRenew { get; set; }

        /// Runs after an object was created; throwing from it reports a failure for a create that landed.
        public Action<string>? AfterCreate { get; set; }

        public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
            CancellationToken cancellation)
        {
            if (OnCreate != null && await OnCreate(key) is { } outcome) return outcome;
            var created = await inner.CreateAsync(key, content, cancellation);
            AfterCreate?.Invoke(key);
            return created;
        }

        public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) => inner.TryReadAsync(key, cancellation);
        public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);
        public void ReadRange(string key, long offset, Span<byte> destination) => inner.ReadRange(key, offset, destination);

        public async IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellation)
        {
            if (OnList != null) await OnList(prefix, cancellation);
            await foreach (var item in inner.ListAsync(prefix, cancellation)) yield return item;
        }

        public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
        public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);

        public async ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration,
            CancellationToken cancellation)
        {
            if (OnRenew != null && await OnRenew(key, leaseId, duration) is { } renewed) return renewed;
            return await inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
        }

        public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
    }

    // Before the fix WriteManifest incremented _manifestSequence before CreateObject succeeded, so a transient error
    // during manifest creation skipped a manifest sequence number and left a permanent gap.
    [Fact]
    public async Task FailedManifestWriteDoesNotSkipManifestSequence()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new ScriptedStorage(inner);
        var failedOnce = false;
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = true,
            MemtableSize = 4096, DeletionGracePeriod = TimeSpan.FromDays(1)
        });
        storage.OnCreate = key =>
        {
            if (!failedOnce && key == "manifest/0000000000000002")
            {
                failedOnce = true;
                throw new IOException("Transient failure creating manifest 2.");
            }

            return ValueTask.FromResult<LsmWriteOutcome?>(null);
        };
        await Commit(db, 0, 10);
        db.CreateKvi(default);
        Assert.True(failedOnce);
        Assert.Contains("manifest/0000000000000002", inner.Keys("manifest/"));
        Assert.DoesNotContain("manifest/0000000000000003", inner.Keys("manifest/"));
    }

    // Before the fix a manifest create that landed but reported a failure was retried under the same name with
    // different content, so the writer found "another writer's" manifest there and fenced itself.
    [Fact]
    public async Task LandedButFailedManifestWriteDoesNotFenceWriter()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new ScriptedStorage(inner);
        var failedOnce = false;
        using (var db = await LsmKeyValueDB.OpenAsync(new()
               {
                   Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = true,
                   MemtableSize = 4096, DeletionGracePeriod = TimeSpan.FromDays(1)
               }))
        {
            storage.AfterCreate = key =>
            {
                if (failedOnce || key != "manifest/0000000000000002") return;
                failedOnce = true;
                throw new IOException("Response lost after manifest 2 was created.");
            };
            await Commit(db, 0, 10);
            db.CreateKvi(default);
            Assert.True(failedOnce);
            Assert.True(db.HoldsWriterLease);
            Assert.Contains("manifest/0000000000000003", inner.Keys("manifest/"));
            await Commit(db, 10, 10);
            Assert.Equal(20, Count(db));
        }

        using var reopened = await LsmKeyValueDB.OpenAsync(new() { Storage = inner });
        Assert.Equal(20, Count(reopened));
    }

    // Before the fix a failure of the first work after a completed takeover (here the orphan search, which runs in
    // the acquiring call without background compaction) released the lease while the instance stayed the writer.
    [Fact]
    public async Task FailedWorkAfterTakeoverKeepsWriterLease()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new ScriptedStorage(inner);
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, BackgroundCompaction = false, LeaseDuration = TimeSpan.FromSeconds(30)
        });
        var failList = true;
        storage.OnList = (prefix, _) =>
            failList && prefix == "tables/"
                ? throw new IOException("Transient listing failure.")
                : ValueTask.CompletedTask;
        await Assert.ThrowsAsync<IOException>(async () => await db.AcquireWriterLeaseAsync());
        Assert.True(db.HoldsWriterLease);

        using var other = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = inner, LeaseDuration = TimeSpan.FromSeconds(30)
        });
        using var acquireTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await other.AcquireWriterLeaseAsync(acquireTimeout.Token));

        failList = false;
        await Commit(db, 0, 5);
        Assert.Equal(5, Count(db));
    }

    // Before the fix CompactAllNow skipped compaction when only one non-L0 table remained, even if it still
    // referenced a mostly dead value file below ValueGcThreshold.
    [Fact]
    public async Task CompactRewritesMostlyDeadValueFileEvenWithOneTable()
    {
        var storage = new InMemoryLsmStorage();
        LsmKeyValueDBOptions Options(double valueGcThreshold) => new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false,
            MemtableSize = 4096, BlockSize = 512, Level0CompactionTrigger = 2, LevelBaseBytes = 64 * 1024,
            LevelCount = 4, TargetFileSize = 64 * 1024, ValueSeparationThreshold = 64,
            ValueGcThreshold = valueGcThreshold, DeletionGracePeriod = TimeSpan.Zero
        };
        string[] originalValueFiles;
        using (var db = await LsmKeyValueDB.OpenAsync(Options(0)))
        {
            await Commit(db, 0, 40, valueSize: 200);
            db.CreateKvi(default);
            originalValueFiles = storage.Keys("values/");
            Assert.NotEmpty(originalValueFiles);
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 38; i++)
                {
                    Assert.True(cursor.FindExactKey(Key(i)));
                    cursor.EraseCurrent();
                }

                tr.Commit();
            }

            // L0->L1 produces a single L1 table that still references the mostly dead original value file.
            db.CreateKvi(default);
            Assert.Single(storage.Keys("tables/"));
            Assert.Equal(originalValueFiles, storage.Keys("values/"));
        }

        using (var db = await LsmKeyValueDB.OpenAsync(Options(0.5) with
               {
                   AcquireWriterLeaseOnOpen = false, BackgroundCompaction = true
               }))
        {
            Assert.Single(storage.Keys("tables/"));
            Assert.Equal(originalValueFiles, storage.Keys("values/"));
            await db.Compact(default);
            Assert.Empty(originalValueFiles.Intersect(storage.Keys("values/")));
            Assert.Equal(2, Count(db));
        }
    }

    // Before the fix CheckLeaseDeadline still allowed writes until the local deadline even when RenewLeaseAsync
    // definitively returned false (lease lost).
    [Fact]
    public async Task CheckLeaseDeadlineFailsImmediatelyOnDefinitiveLeaseLoss()
    {
        var clock = new MutableClock();
        var inner = new InMemoryLsmStorage(clock);
        var storage = new ScriptedStorage(inner);
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, TimeProvider = clock, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false,
            LeaseDuration = TimeSpan.FromSeconds(15), LeaseSafetyMargin = TimeSpan.FromSeconds(2)
        });
        await Commit(db, 0, 5);
        // Advance past LeaseDeadline - LeaseDuration/3 (13 - 5 = 8s), while still before LeaseDeadline (13s).
        clock.Now += TimeSpan.FromSeconds(9);
        storage.OnRenew = (_, _, _) => ValueTask.FromResult<bool?>(false);
        Assert.Throws<LsmWriterFencedException>(() => db.CreateKvi(default));
    }

    // Before the fix AcquireWriterLeaseAsync left the lease acquired in storage when BecomeWriterAsync was canceled,
    // forcing the next writer to wait out the full lease duration.
    [Fact]
    public async Task CanceledAcquireWriterLeaseReleasesLeaseInStorage()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new ScriptedStorage(inner);
        using var cts = new CancellationTokenSource();
        using var first = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, LeaseDuration = TimeSpan.FromSeconds(30)
        });
        storage.OnList = (_, _) =>
        {
            cts.Cancel();
            cts.Token.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await first.AcquireWriterLeaseAsync(cts.Token));

        using var second = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = inner, LeaseDuration = TimeSpan.FromSeconds(30)
        });
        using var acquireTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await second.AcquireWriterLeaseAsync(acquireTimeout.Token);
        Assert.True(second.HoldsWriterLease);
    }

    // Before the fix CheckpointAsync ignored its CancellationToken while waiting for flushes.
    [Fact]
    public async Task CreateKviHonorsCancellationWhileWaitingForFlush()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new GatedTableStorage(inner);
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, MemtableSize = 4096
        });
        storage.Gate.Reset();
        try
        {
            await Commit(db, 0, 10);
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            Assert.ThrowsAny<OperationCanceledException>(() => db.CreateKvi(cts.Token));
        }
        finally
        {
            storage.Gate.Set();
        }
    }

    // Before the fix a fuzzy cursor reported IsValid() == true and KeyHasPrefix threw on an erased last key or
    // silently advanced the cursor on an erased middle key.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FuzzyCursorIsInvalidAndKeyHasPrefixReturnsFalse(bool lsm)
    {
        IKeyValueDB db = lsm
            ? await LsmKeyValueDB.OpenAsync(new() { Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true })
            : new BTreeKeyValueDB(new InMemoryFileCollection());
        using (db)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([1, 1], [1]);
            cursor.CreateOrUpdateKeyValue([2, 2], [2]);
            cursor.EraseCurrent();
            Assert.False(cursor.IsValid());
            Assert.False(cursor.KeyHasPrefix([2]));
            Assert.False(cursor.KeyHasPrefix([]));
            Assert.Equal(0, cursor.GetKeyIndex());

            Assert.True(cursor.FindFirstKey([]));
            cursor.CreateOrUpdateKeyValue([3, 3], [3]);
            Assert.True(cursor.FindExactKey([1, 1]));
            cursor.EraseCurrent();
            Assert.False(cursor.IsValid());
            Assert.False(cursor.KeyHasPrefix([1]));
            Assert.False(cursor.KeyHasPrefix([3]));
            Assert.Equal(-1, cursor.GetKeyIndex());
            Assert.True(cursor.FindNextKey([]));
            Assert.Equal(new byte[] { 3, 3 }, cursor.SlowGetKey());

            Assert.False(cursor.FindKeyIndex([], -1));
            Assert.False(cursor.FindKeyIndex([], long.MaxValue));
        }
    }

    [Fact]
    public async Task EraseUpToRejectsCursorFromDifferentTransaction()
    {
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true
        });
        await Commit(db, 0, 5);
        using var tr1 = await db.StartWritingTransaction();
        using var tr2 = db.StartReadOnlyTransaction();
        using var c1 = tr1.CreateCursor();
        using var c2 = tr2.CreateCursor();
        Assert.True(c1.FindFirstKey([]));
        Assert.True(c2.FindLastKey([]));
        Assert.Throws<ArgumentException>(() => c1.EraseUpTo(c2));
    }

    // Before the fix WalReplay.Apply advanced NextChunk before replaying, so a corrupted WAL chunk was skipped on the
    // next RefreshAsync instead of failing again.
    [Fact]
    public async Task CorruptedWalChunkIsNotSkippedOnSubsequentRefresh()
    {
        var storage = new InMemoryLsmStorage();
        using var writer = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false
        });
        using var reader = await LsmKeyValueDB.OpenAsync(new() { Storage = storage });
        await Commit(writer, 0, 2);
        await reader.RefreshAsync();
        Assert.Equal(2, Count(reader));

        // Write a chunk with a valid outer checksum but a corrupted inner WAL record operation tag.
        var badChunk = WalFormat.Encode(1, 1, false, 0, [new WalPart(true, true, 2, [2, 2, 0, 0xFF])]);
        var nextWal = $"wal/{storage.Keys("wal/").Length:D16}";
        await storage.CreateAsync(nextWal, badChunk, default);

        await Assert.ThrowsAsync<BTDBException>(async () => await reader.RefreshAsync());
        await Assert.ThrowsAsync<BTDBException>(async () => await reader.RefreshAsync());
    }

    [Fact]
    public void CorruptedBlockBloomFilterAndValueRecordThrowBTDBException()
    {
        Assert.Throws<BTDBException>(() => _ = new RawBlock([1, 2, 3]));
        var badRestart = new byte[DataBlockBuilder.TrailerSize];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(badRestart, 16);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(badRestart.AsSpan(4), -1);
        Assert.Throws<BTDBException>(() => _ = new RawBlock(badRestart));

        var emptyPayloadBlock = new LsmEncoder();
        emptyPayloadBlock.WriteChecksum(0);
        Assert.Throws<BTDBException>(() => RawBlock.Decode(emptyPayloadBlock.WrittenSpan, "block"));

        var emptyBloom = new LsmEncoder();
        emptyBloom.WriteVUInt64(1);
        emptyBloom.WriteByteArray([]);
        Assert.Throws<BTDBException>(() => BloomFilter.Read(emptyBloom.WrittenSpan));

        Assert.Throws<BTDBException>(() => ValueFileWriter.VerifyRecord([1, 2, 3], "values/1.vlf"));

        Assert.Throws<ArgumentOutOfRangeException>(() => _ = new LsmBlockCache(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = new LsmBlockCache(64 * 1024, 0));
    }

    // Before the fix Dispose did not free native memtable arenas or its owned block cache when StepDownAsync threw.
    [Fact]
    public async Task DisposeReleasesNativeMemoryEvenWhenStepDownFails()
    {
        var inner = new InMemoryLsmStorage();
        var storage = new ScriptedStorage(inner);
        var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, WalFlushInterval = TimeSpan.FromSeconds(10)
        });
        storage.OnCreate = key => key.StartsWith("wal/", StringComparison.Ordinal)
            ? throw new IOException("Storage unavailable during shutdown.")
            : ValueTask.FromResult<LsmWriteOutcome?>(null);
        await Commit(db, 0, 20);
        Assert.True(db.GetNativeMemoryStats().AllocSize > 0);
        Assert.Throws<BTDBException>(() => db.Dispose());
        Assert.Equal(0UL, db.GetNativeMemoryStats().AllocSize);
    }

    // Before the fix a fuzzy cursor with _before == true returned early in NotifyRenamed even after its remembered key
    // had been erased, so a subsequent key suffix update crossing that key moved the cursor to the wrong side of it.
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task KeySuffixUpdateCrossingErasedBeforeCursorKeepsItOnItsSide(bool lsm, bool renameOntoSameKey)
    {
        IKeyValueDB db = lsm
            ? await LsmKeyValueDB.OpenAsync(new() { Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true })
            : new BTreeKeyValueDB(new InMemoryFileCollection());
        using (db)
        {
            using var tr = await db.StartWritingTransaction();
            using var r = tr.CreateCursor();
            using var c = tr.CreateCursor();
            r.CreateOrUpdateKeyValue([0xFF, 0x01], [1]);
            r.CreateOrUpdateKeyValue([0xFF, 0xFF], [2]);
            Assert.True(c.FindExactKey([0xFF, 0x01]));
            c.EraseCurrent();
            // Rename [0xFF, 0xFF] -> [0xFF]: c becomes fuzzy right before [0xFF] (_before = true).
            Assert.Equal(UpdateKeySuffixResult.Updated, r.UpdateKeySuffix([0xFF], 1));
            // Erase [0xFF] and insert [0xFE]: c is now after [0xFE] (index 0).
            Assert.True(r.FindExactKey([0xFF]));
            r.EraseCurrent();
            r.CreateOrUpdateKeyValue([0xFE], [3]);
            Assert.Equal(0, c.GetKeyIndex());
            // Rename [0xFE] across (or onto) [0xFF]: [0xFE] keeps index 0, so c must stay after the renamed key.
            byte[] target = renameOntoSameKey ? [0xFF] : [0xFF, 0x05];
            Assert.Equal(UpdateKeySuffixResult.Updated, r.UpdateKeySuffix(target, 0));
            Assert.Equal(0, c.GetKeyIndex());
            Assert.False(c.FindNextKey([]));
            Assert.True(c.FindPreviousKey([]));
            Assert.Equal(target, c.SlowGetKey());
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
