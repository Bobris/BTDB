using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Storage;
using Xunit;

namespace BTDB.LSM.Test.Api;

/// Counts the bytes read synchronously from tables.
sealed class TableReadCountingStorage(InMemoryLsmStorage inner) : ILsmStorage
{
    long _tableBytesRead;

    public long TableBytesRead => Interlocked.Read(ref _tableBytesRead);

    public ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content, CancellationToken cancellation) => inner.CreateAsync(key, content, cancellation);
    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) => inner.TryReadAsync(key, cancellation);
    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);

    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        if (key.StartsWith("tables/")) Interlocked.Add(ref _tableBytesRead, destination.Length);
        inner.ReadRange(key, offset, destination);
    }

    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
    public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);
    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration, CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) => inner.ReleaseLeaseAsync(key, leaseId, cancellation);
}

public class LsmLimitsTest
{
    static byte[] Key(int i) => [(byte)(i >> 16), (byte)(i >> 8), (byte)i];

    static long Count(IKeyValueDB db)
    {
        using var tr = db.StartReadOnlyTransaction();
        return tr.GetKeyValueCount();
    }

    public static TheoryData<string> InvalidOptions => new()
    {
        nameof(LsmKeyValueDBOptions.WalUploadParallelism), nameof(LsmKeyValueDBOptions.LeaseSafetyMargin),
        nameof(LsmKeyValueDBOptions.RefreshRetryInterval), nameof(LsmKeyValueDBOptions.LevelCount),
        nameof(LsmKeyValueDBOptions.Level0StopWritesTrigger)
    };

    /// Before the fix such options were taken as given: a zero upload parallelism spun in the WAL upload, a safety
    /// margin covering the lease kept every takeover retrying, a zero refresh interval spun while waiting for a commit,
    /// a single level crashed the first compaction, and writes stopping before level 0 compacts waited forever.
    [Theory]
    [MemberData(nameof(InvalidOptions))]
    public async Task OptionsTheEngineCannotRunWithAreRejectedOnOpen(string option)
    {
        var options = new LsmKeyValueDBOptions { Storage = new InMemoryLsmStorage() };
        options = option switch
        {
            nameof(LsmKeyValueDBOptions.WalUploadParallelism) => options with { WalUploadParallelism = 0 },
            nameof(LsmKeyValueDBOptions.LeaseSafetyMargin) => options with { LeaseSafetyMargin = options.LeaseDuration },
            nameof(LsmKeyValueDBOptions.RefreshRetryInterval) => options with { RefreshRetryInterval = TimeSpan.Zero },
            nameof(LsmKeyValueDBOptions.Level0StopWritesTrigger) => options with
            {
                Level0CompactionTrigger = 8, Level0StopWritesTrigger = 4
            },
            _ => options with { LevelCount = 1 }
        };
        await Assert.ThrowsAnyAsync<ArgumentOutOfRangeException>(async () =>
        {
            using var db = await LsmKeyValueDB.OpenAsync(options);
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TransactionAboveMaxTransactionSizeThrowsAndRollsBack(bool nativeMemtable)
    {
        var options = new LsmKeyValueDBOptions
        {
            Storage = new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true, NativeMemtable = nativeMemtable,
            MaxTransactionMemory = 16 * 1024, MaxTransactionSize = 64 * 1024
        };
        using var db = await LsmKeyValueDB.OpenAsync(options);
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 10; i++) cursor.CreateOrUpdateKeyValue(Key(i), new byte[100]);
            tr.Commit();
        }

        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            var exception = Assert.Throws<BTDBException>(() =>
            {
                for (var i = 100; i < 10_000; i++) cursor.CreateOrUpdateKeyValue(Key(i), new byte[100]);
            });
            Assert.Contains("MaxTransactionSize", exception.Message);
            Assert.Throws<BTDBException>(() => cursor.CreateOrUpdateKeyValue(Key(20_000), new byte[1]));
            Assert.Throws<BTDBException>(() => tr.Commit());
        }

        Assert.Equal(10, Count(db));
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key(10), new byte[100]);
            tr.Commit();
        }

        Assert.Equal(11, Count(db));
    }

    [Fact]
    public async Task CompactorReadBytesPerSecondLimitPacesCompactionReads()
    {
        var storage = new TableReadCountingStorage(new InMemoryLsmStorage());
        var options = new LsmKeyValueDBOptions
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false, CompressBlocks = false,
            Level0CompactionTrigger = 100, Level0StopWritesTrigger = 200
        };
        using var db = await LsmKeyValueDB.OpenAsync(options);
        for (var table = 0; table < 4; table++)
        {
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 1000; i++) cursor.CreateOrUpdateKeyValue(Key(table * 1000 + i), new byte[100]);
                tr.Commit();
            }

            db.CreateKvi(default);
        }

        const ulong limit = 512 * 1024;
        db.CompactorReadBytesPerSecondLimit = limit;
        var readBefore = storage.TableBytesRead;
        var start = Stopwatch.GetTimestamp();
        await db.Compact(default);
        var elapsed = Stopwatch.GetElapsedTime(start);
        var read = storage.TableBytesRead - readBefore;
        Assert.True(read > 300 * 1024, $"Compaction read only {read} bytes.");
        // Index and filter reads are not paced, so allow for them.
        Assert.True(elapsed >= TimeSpan.FromSeconds(0.8 * read / limit), $"{read} bytes read in {elapsed}.");
        Assert.Equal(4000, Count(db));
    }

    [Fact]
    public async Task CompactorReadPacingDoesNotCreditIdleTime()
    {
        var storage = new TableReadCountingStorage(new InMemoryLsmStorage());
        var options = new LsmKeyValueDBOptions
        {
            Storage = storage, AcquireWriterLeaseOnOpen = true, BackgroundCompaction = false, CompressBlocks = false,
            Level0CompactionTrigger = 100, Level0StopWritesTrigger = 200
        };
        using var db = await LsmKeyValueDB.OpenAsync(options);
        for (var table = 0; table < 4; table++)
        {
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 1000; i++) cursor.CreateOrUpdateKeyValue(Key(table * 1000 + i), new byte[100]);
                tr.Commit();
            }

            db.CreateKvi(default);
        }

        // Time before the limit applies must not let the compaction read faster than the limit afterwards.
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        const ulong limit = 512 * 1024;
        db.CompactorReadBytesPerSecondLimit = limit;
        var readBefore = storage.TableBytesRead;
        var start = Stopwatch.GetTimestamp();
        await db.Compact(default);
        var elapsed = Stopwatch.GetElapsedTime(start);
        var read = storage.TableBytesRead - readBefore;
        Assert.True(read > 300 * 1024, $"Compaction read only {read} bytes.");
        Assert.True(elapsed >= TimeSpan.FromSeconds(0.8 * read / limit), $"{read} bytes read in {elapsed}.");
    }
}
