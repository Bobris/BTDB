using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class TransactionLogSizeStrategyTest
{
    sealed class Strategy(Func<uint, uint> size) : ITransactionLogSizeStrategy
    {
        public readonly List<uint> Calls = new();
        public TransactionLogSizeLimits GetLimits(uint transactionLogFileId)
        {
            Calls.Add(transactionLogFileId);
            return new(size(transactionLogFileId), 16_384);
        }
    }

    sealed class FixedStrategy(uint soft, uint hard) : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint transactionLogFileId) => new(soft, hard);
    }

    static BTreeKeyValueDB Open(IFileCollection files, ITransactionLogSizeStrategy strategy, uint fallback = 1024) => new(new KeyValueDBOptions
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
        UseOddTransactionLogIds = true, FileSplitSize = fallback, TransactionLogSizeStrategy = strategy
    });

    static uint Limit(uint fileId) => 1024u * (1 + fileId % 3);

    static void Put(IKeyValueDBTransaction transaction, byte key)
    {
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], new byte[700]);
    }

    [Fact]
    public async Task FileIdsAloneSelectLimitsAndRotationRegardlessOfFallbackOrMemoryBatching()
    {
        static async Task<(uint, ulong)[]> Run(bool batch, uint fallback)
        {
            using var files = new InMemoryFileCollection();
            var strategy = new Strategy(Limit);
            using var db = Open(files, strategy, fallback);
            for (byte i = 1; i < 40; i++)
            {
                using var tr = await db.StartWritingTransaction(i, batch);
                Put(tr, i);
                tr.Commit();
                Assert.Equal(Limit(strategy.Calls[^1]), db.MaxTrLogFileSize);
            }
            Assert.True(strategy.Calls.Count > 3);
            Assert.Equal(strategy.Calls.Count, strategy.Calls.Distinct().Count());
            return files.Enumerate().OrderBy(f => f.Index).Select(f => (f.Index, f.GetSize())).ToArray();
        }
        Assert.Equal(await Run(false, 1024), await Run(true, 100_000));
    }

    [Fact]
    public async Task ReopenRecomputesActiveLimitAndMetadataFilesDoNotChangeIt()
    {
        using var files = new InMemoryFileCollection();
        uint active;
        using (var db = Open(files, new Strategy(Limit)))
        {
            using var tr = await db.StartWritingTransaction(1ul);
            Put(tr, 1);
            tr.Commit();
            active = Assert.Single(db.FileCollection.FileInfos, p => p.Value is IFileTransactionLog).Key;
        }
        var strategy = new Strategy(Limit);
        using (var db = Open(files, strategy, 100_000))
        {
            Assert.Equal(active, Assert.Single(strategy.Calls));
            Assert.Equal(Limit(active), db.MaxTrLogFileSize);
            db.CreateKvi(default);
            Assert.Equal(active, Assert.Single(strategy.Calls));
            Assert.Equal(Limit(active), db.MaxTrLogFileSize);
            Assert.Throws<InvalidOperationException>(() => db.MaxTrLogFileSize = 4096);
            db.AutoAdjustFileSize = true;
            using var tr = await db.StartWritingTransaction(2ul);
            Put(tr, 2);
            tr.Commit();
            Assert.Equal(Limit(active), db.MaxTrLogFileSize);
            db.CreateKvi(default);
            Assert.Equal(32 * 1024 * 1024, ((IKeyValueDBInternal)db).FileSplitSize);
            Assert.Equal(Limit(active), db.MaxTrLogFileSize);
        }
    }

    [Fact]
    public async Task AutomaticSizingDoesNotChangeStrategyLimits()
    {
        using var files = new InMemoryFileCollection();
        var strategy = new Strategy(Limit);
        using var db = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogSizeStrategy = strategy, AutoAdjustFileSize = true,
            FileSplitSize = 1024, CompactorScheduler = null, Compression = new NoCompressionStrategy()
        });
        Assert.Equal(32 * 1024 * 1024, ((IKeyValueDBInternal)db).FileSplitSize);
        for (byte i = 0; i < 12; i++)
        {
            using var tr = await db.StartWritingTransaction();
            Put(tr, i);
            tr.Commit();
            Assert.Equal(Limit(strategy.Calls[^1]), db.MaxTrLogFileSize);
            Assert.Equal(32 * 1024 * 1024, ((IKeyValueDBInternal)db).FileSplitSize);
        }
        Assert.True(strategy.Calls.Count > 1);
        db.AutoAdjustFileSize = false;
        db.AutoAdjustFileSize = true;
        Assert.Equal(Limit(strategy.Calls[^1]), db.MaxTrLogFileSize);
    }

    [Fact]
    public async Task CompactionUsesAutosizedPvlsWithStrategySizedTrls()
    {
        const uint mib = 1024 * 1024;
        using var files = new InMemoryFileCollection();
        var strategy = new FixedStrategy(20 * mib, 24 * mib);
        using (var db = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
            TransactionLogSizeStrategy = strategy, AutoAdjustFileSize = true, FileSplitSize = 1024
        }))
        {
            var value = new byte[mib];
            // Leave more than one autosized file's worth of waste in sealed TRLs; the active TRL is excluded.
            for (byte i = 1; i <= 100; i++)
            {
                value[0] = i;
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue([i % 2 == 0 ? i : (byte)0], value);
                tr.Commit();
            }
            Assert.True(await db.Compact(default));
            Assert.Equal(32 * mib, ((IKeyValueDBInternal)db).FileSplitSize);
            Assert.Equal(20 * mib, db.MaxTrLogFileSize);
            var pvls = db.FileCollection.FileInfos.Where(p => p.Value.FileType == KVFileType.PureValues).ToArray();
            Assert.NotEmpty(pvls);
            Assert.Contains(pvls, p => files.GetFile(p.Key)!.GetSize() > 20 * mib);
            Assert.All(pvls, p => Assert.InRange(files.GetFile(p.Key)!.GetSize(), 1ul, 32ul * mib));
            Assert.All(db.FileCollection.FileInfos.Where(p => p.Value is IFileTransactionLog),
                p => Assert.InRange(files.GetFile(p.Key)!.GetSize(), 1ul, 24ul * mib));
        }
        using var reopened = Open(files, strategy);
        using var read = reopened.StartReadOnlyTransaction();
        Assert.Equal(51, read.GetKeyValueCount());
        using var result = read.CreateCursor();
        Assert.True(result.FindExactKey([100]));
        Span<byte> buffer = default;
        var expected = new byte[mib];
        expected[0] = 100;
        Assert.Equal(expected, result.GetValueSpan(ref buffer).ToArray());
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1023u)]
    [InlineData(uint.MaxValue)]
    public async Task InvalidLimitLeavesNoLogAndReleasesWriter(uint size)
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files, new Strategy(_ => size));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            using var tr = await db.StartWritingTransaction(1ul);
        });
        Assert.Equal(0u, files.GetCount());
        using var next = await db.StartWritingTransaction();
        Assert.Equal(0ul, next.GetCommitUlong());
    }
    [Fact]
    public async Task SoftLimitRotatesOnlyBeforeTheNextTransactionIncludingAfterReopen()
    {
        using var files = new InMemoryFileCollection();
        var strategy = new FixedStrategy(1024, 8192);
        using (var db = Open(files, strategy))
        {
            using var tr = await db.StartWritingTransaction(1ul);
            Put(tr, 1);
            Put(tr, 2);
            Put(tr, 3);
            tr.Commit();
            Assert.Equal(1u, files.GetCount());
            Assert.True(Assert.Single(files.Enumerate()).GetSize() > 1024);
        }
        using (var db = Open(files, strategy))
        {
            Assert.Equal(1u, files.GetCount());
            using var tr = await db.StartWritingTransaction(2ul);
            Put(tr, 4);
            tr.Commit();
            Assert.Equal(2u, files.GetCount());
            using var read = db.StartReadOnlyTransaction();
            Assert.Equal(4L, read.GetKeyValueCount());
        }
    }

    [Fact]
    public async Task FixedFileSizeDoesNotRotateOnOpenOnlyBeforeTheNextTransaction()
    {
        using var files = new InMemoryFileCollection();
        BTreeKeyValueDB OpenFixed() => new(new KeyValueDBOptions
        {
            FileCollection = files, Compression = new NoCompressionStrategy(),
            CompactorScheduler = null, FileSplitSize = 1024
        });
        using (var db = OpenFixed())
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue("key"u8, new byte[1500]);
            tr.Commit();
        }
        var original = Assert.Single(files.Enumerate());
        var originalSize = original.GetSize();
        Assert.True(originalSize >= 1024);
        using (var db = OpenFixed())
        {
            Assert.Equal(original.Index, Assert.Single(files.Enumerate()).Index);
            Assert.Equal(originalSize, original.GetSize());
            using var read = db.StartReadOnlyTransaction();
            Assert.Equal(1L, read.GetKeyValueCount());
        }
        using (var db = OpenFixed())
        {
            Assert.Single(files.Enumerate());
            using var tr = await db.StartWritingTransaction();
            Put(tr, 2);
            tr.Commit();
            Assert.Equal(2u, files.GetCount());
            using var read = db.StartReadOnlyTransaction();
            Assert.Equal(2L, read.GetKeyValueCount());
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task HardLimitSplitsTransactionsAndPreservesCommitOrRollback(bool rollback, bool batch)
    {
        using var files = new InMemoryFileCollection();
        var strategy = new FixedStrategy(1024, 1536);
        using (var db = Open(files, strategy))
        {
            using (var tr = await db.StartWritingTransaction(1ul, batch))
            {
                Put(tr, 0);
                tr.Commit();
            }
            using (var tr = await db.StartWritingTransaction(2ul, batch))
            {
                for (byte i = 1; i <= 12; i++) Put(tr, i);
                // Metadata alone spans additional files during commit.
                for (uint i = 0; i < 500; i++) tr.SetUlong(i, ulong.MaxValue);
                if (!rollback) tr.Commit();
            }
            Assert.True(files.GetCount() > 3);
        }
        Assert.All(files.Enumerate(), f => Assert.InRange(f.GetSize(), 1ul, 1536ul));
        using (var db = Open(files, strategy))
        using (var tr = db.StartReadOnlyTransaction())
        {
            Assert.Equal(rollback ? 1ul : 2ul, tr.GetCommitUlong());
            Assert.Equal(rollback ? 1L : 13L, tr.GetKeyValueCount());
            Assert.Equal(rollback ? 0ul : ulong.MaxValue, tr.GetUlong(499));
            using var cursor = tr.CreateCursor();
            Assert.True(cursor.FindExactKey([0]));
            Span<byte> buffer = default;
            Assert.Equal(new byte[700], cursor.GetValueSpan(ref buffer).ToArray());
        }
    }

    [Fact]
    public async Task OversizedCommandIsRejectedWithoutExceedingHardLimit()
    {
        using var files = new InMemoryFileCollection();
        var strategy = new FixedStrategy(1024, 1536);
        using (var db = Open(files, strategy))
        {
            using (var tr = await db.StartWritingTransaction(1ul))
            {
                using var cursor = tr.CreateCursor();
                Assert.Throws<BTDBException>(() => cursor.CreateOrUpdateKeyValue([1], new byte[2000]));
            }
            using var next = await db.StartWritingTransaction(2ul);
            Put(next, 2);
            next.Commit();
        }
        Assert.All(files.Enumerate(), f => Assert.InRange(f.GetSize(), 1ul, 1536ul));
        using var reopened = Open(files, strategy);
        using var read = reopened.StartReadOnlyTransaction();
        Assert.Equal(1L, read.GetKeyValueCount());
        Assert.Equal(2ul, read.GetCommitUlong());
    }

    [Theory]
    [InlineData(1024u, uint.MaxValue)]
    [InlineData(2048u, 1024u)]
    public async Task InvalidHardLimitIsRejected(uint soft, uint hard)
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files, new FixedStrategy(soft, hard));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            using var tr = await db.StartWritingTransaction(1ul);
        });
        Assert.Equal(0u, files.GetCount());
    }

    [Fact]
    public async Task HardLimitJustBelowFourGiBIsAccepted()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files, new FixedStrategy(uint.MaxValue - 2, uint.MaxValue - 1));
        using var tr = await db.StartWritingTransaction(1ul);
        Put(tr, 1);
        tr.Commit();
        Assert.Equal((long)uint.MaxValue - 2, db.MaxTrLogFileSize);
    }

    [Fact]
    public async Task InvalidStrategyAlsoReleasesSynchronousWriter()
    {
        using var files = new InMemoryFileCollection();
        using var db = Open(files, new Strategy(_ => 0));
        using (var tr = db.StartTransaction())
            Assert.Throws<ArgumentOutOfRangeException>(() => tr.SetCommitUlong(1));
        using var next = await db.StartWritingTransaction();
        Assert.Equal(0ul, next.GetCommitUlong());
    }

    [Fact]
    public async Task LongKeyUpdatesAndErasesRespectHardLimitAndReplay()
    {
        using var files = new InMemoryFileCollection();
        var strategy = new FixedStrategy(1024, 1536);
        static byte[] Key(byte first, byte last = 0)
        {
            var key = new byte[600];
            key[0] = first;
            key[^1] = last;
            return key;
        }
        using (var db = Open(files, strategy))
        {
            using var tr = await db.StartWritingTransaction(1ul);
            using var cursor = tr.CreateCursor();
            for (byte i = 1; i <= 4; i++) cursor.CreateOrUpdateKeyValue(Key(i), [42]);
            cursor.UpdateKeySuffix(Key(1, 1), 1);
            Assert.True(cursor.FindExactKey(Key(2)));
            cursor.EraseCurrent();
            Assert.True(cursor.FindExactKey(Key(3)));
            using var end = tr.CreateCursor();
            Assert.True(end.FindExactKey(Key(4)));
            Assert.Equal(2, cursor.EraseUpTo(end));
            tr.Commit();
        }
        Assert.All(files.Enumerate(), f => Assert.InRange(f.GetSize(), 1ul, 1536ul));
        using var reopened = Open(files, strategy);
        using var read = reopened.StartReadOnlyTransaction();
        Assert.Equal(1L, read.GetKeyValueCount());
        using var result = read.CreateCursor();
        Assert.True(result.FindExactKey(Key(1, 1)));
    }

    [Fact]
    public async Task RepeatedOpenAndCloseKeepsEndMarkersWithinHardLimit()
    {
        using var files = new InMemoryFileCollection();
        var strategy = new FixedStrategy(1024, 1024);
        using (var db = Open(files, strategy))
        using (var tr = await db.StartWritingTransaction(1ul))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([1], new byte[940]);
            tr.NextCommitTemporaryCloseTransactionLog();
            tr.Commit();
        }
        for (var i = 0; i < 100; i++)
        {
            using var db = Open(files, strategy);
            using var tr = db.StartReadOnlyTransaction();
            Assert.Equal(1ul, tr.GetCommitUlong());
            Assert.Equal(1L, tr.GetKeyValueCount());
        }
        Assert.True(files.GetCount() > 1);
        Assert.All(files.Enumerate(), f => Assert.InRange(f.GetSize(), 1ul, 1024ul));
    }

}
