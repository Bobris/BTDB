using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public abstract class KeyValueDBFileTestBase : KeyValueDBTestBase
{
    protected override IKeyValueDB NewKeyValueDB()
    {
        return NewKeyValueDB(new InMemoryFileCollection());
    }

    protected abstract IKeyValueDB NewKeyValueDB(IFileCollection fileCollection);

    protected abstract IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression,
        uint fileSplitSize = int.MaxValue);

    protected abstract IKeyValueDB NewKeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression,
        uint fileSplitSize,
        ICompactorScheduler? compactorScheduler);

    protected abstract IKeyValueDB NewKeyValueDB(KeyValueDBOptions options);

    [Fact]
    public void UlongsAreRemembered()
    {
        var snapshot = new MemoryStream();
        using (var fileCollection = new InMemoryFileCollection())
        {
            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    Assert.Equal(0ul, tr1.GetUlong(0));
                    tr1.SetUlong(0, 42);
                    tr1.Commit();
                }

                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }

            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }

            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                    KeyValueDBExportImporter.Export(tr2, snapshot);
                }
            }
        }

        snapshot.Position = 0;
        using (var fileCollection = new InMemoryFileCollection())
        {
            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(0ul, tr2.GetUlong(0));
                    KeyValueDBExportImporter.Import(tr2, snapshot);
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }
        }
    }

    [Fact]
    public async Task UlongsAreRememberedAsync()
    {
        var snapshot = new MemoryStream();
        using (var fileCollection = new InMemoryFileCollection())
        {
            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr1 = db.StartTransaction())
                {
                    Assert.Equal(0ul, tr1.GetUlong(0));
                    tr1.SetUlong(0, 42);
                    tr1.Commit();
                }

                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }

            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }

            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(42ul, tr2.GetUlong(0));
                    await KeyValueDBExportImporter.ExportAsync(tr2, snapshot);
                }
            }
        }

        snapshot.Position = 0;
        using (var fileCollection = new InMemoryFileCollection())
        {
            using (var db = NewKeyValueDB(fileCollection))
            {
                using (var tr2 = db.StartTransaction())
                {
                    Assert.Equal(0ul, tr2.GetUlong(0));
                    await KeyValueDBExportImporter.ImportAsync(tr2, snapshot);
                    Assert.Equal(42ul, tr2.GetUlong(0));
                }
            }
        }
    }

    [Fact]
    public void RepairsOnReopen()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateKey(Key1);
                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateKey(Key2);
                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateKey(Key3);
                // rollback
            }

            using (var db2 = NewKeyValueDB(fileCollection))
            {
                using (var tr = db2.StartTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    Assert.True(cursor.FindExactKey(Key1));
                    Assert.True(cursor.FindExactKey(Key2));
                    Assert.False(cursor.FindExactKey(Key3));
                }
            }
        }

        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                Assert.True(cursor.FindExactKey(Key1));
                Assert.True(cursor.FindExactKey(Key2));
                Assert.False(cursor.FindExactKey(Key3));
            }
        }
    }

    [Fact]
    public void MoreComplexReopen()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            for (var i = 0; i < 100; i++)
            {
                var key = new byte[100];
                using var tr = db.StartTransaction();
                using var cursor = tr.CreateCursor();
                key[0] = (byte)(i / 256);
                key[1] = (byte)(i % 256);
                Assert.True(cursor.CreateOrUpdateKeyValue(key, key));
                tr.Commit();
            }

            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.FindKeyIndex(0);
                cursor.EraseCurrent();
                cursor.FindKeyIndex(1);
                using var cursor2 = tr.CreateCursor();
                cursor2.FindKeyIndex(3);
                cursor.EraseUpTo(cursor2);
                tr.Commit();
            }
        }

        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                var key = new byte[100];
                key[1] = 1;
                Assert.True(cursor.FindExactKey(key));
                cursor.FindNextKey(new());
                Assert.Equal(5, cursor.SlowGetKey()[1]);
                Assert.Equal(96, tr.GetKeyValueCount());
            }
        }
    }

    [Fact]
    public void AddingContinueToSameFileAfterReopen()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key1, Key1);
                tr.Commit();
            }
        }

        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key2, Key2);
                tr.Commit();
            }

            TestOutputHelper.WriteLine(db.CalcStats());
        }

        Assert.Equal(1u, fileCollection.GetCount()); // Log
    }

    [Fact]
    public void AddingContinueToNewFileAfterReopenWithCorruption()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key1, Key1);
                tr.Commit();
            }
        }

        fileCollection.SimulateCorruptionBySetSize(20 + 16);
        using (var db = NewKeyValueDB(fileCollection))
        {
            using (var tr = db.StartTransaction())
            {
                Assert.Equal(0, tr.GetKeyValueCount());
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key2, Key2);
                tr.Commit();
            }

            TestOutputHelper.WriteLine(db.CalcStats());
        }

        Assert.True(2 <= fileCollection.GetCount());
    }

    [Fact]
    public void AddingContinueToSameFileAfterReopenOfDBWith2TransactionLogFiles()
    {
        using var fileCollection = new InMemoryFileCollection();
        using (var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key1, new byte[1024]);
                cursor.CreateOrUpdateKeyValue(Key2, new byte[10]);
                tr.Commit();
            }
        }

        Assert.Equal(2u, fileCollection.GetCount());
        using (var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key2, new byte[1024]);
                cursor.CreateOrUpdateKeyValue(Key3, new byte[10]);
                tr.Commit();
            }
        }

        Assert.Equal(3u, fileCollection.GetCount());
        using (var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key2, Key2);
                tr.Commit();
            }
        }

        Assert.Equal(3u, fileCollection.GetCount());
    }

    [Fact]
    public async Task CompactionDoesNotRemoveStillUsedFiles()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024, null);
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key1, new byte[1024]);
            cursor.CreateOrUpdateKeyValue(Key2, new byte[10]);
            tr.Commit();
        }

        var longTr = db.StartTransaction();
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.FindExactKey(Key1);
            cursor.EraseCurrent();
            tr.Commit();
        }

        await db.Compact(CancellationToken.None);
        Assert.Equal(3u, fileCollection.GetCount()); // 2 Logs, 1 KeyIndex
        longTr.Dispose();
        await db.Compact(CancellationToken.None);
        Assert.Equal(2u, fileCollection.GetCount()); // 1 Log, 1 KeyIndex
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key3, new byte[10]);
            tr.Commit();
        }

        using (var db2 = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db2.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                Assert.True(cursor.FindExactKey(Key3));
            }
        }
    }

    [Fact]
    public async Task CompactionStabilizedEvenWithOldTransactions()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 10240, null);
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key1, new byte[4000]);
            cursor.CreateOrUpdateKeyValue(Key2, new byte[4000]);
            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key3, new byte[4000]); // creates new Log
            cursor.FindExactKey(Key1);
            cursor.EraseCurrent();
            tr.Commit();
        }

        Assert.Equal(2u, fileCollection.GetCount()); // 2 Logs

        using (var longTr = db.StartTransaction())
        {
            await db.Compact(CancellationToken.None);
            Assert.Equal(3u, fileCollection.GetCount()); // 2 Logs, 1 KeyIndex
            await db.Compact(CancellationToken.None);
            Assert.Equal(3u, fileCollection.GetCount()); // 2 Logs, 1 KeyIndex
        }

        await db.Compact(CancellationToken.None);
        Assert.Equal(3u, fileCollection.GetCount()); // 1 Log, 1 values, 1 KeyIndex
    }

    byte[] CreateBigKey(int id, int length)
    {
        var key = new byte[length];
        PackUnpack.PackInt32BE(key, 0, id);
        return key;
    }

    [Fact]
    public async Task SmallCompactionDoesNotCorruptDB()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 10240, null);
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = 0; i < 10; i++)
            {
                cursor.CreateOrUpdateKeyValue(CreateBigKey(i, 4000), new byte[4000]);
            }

            tr.Commit();
        }

        await db.Compact(CancellationToken.None);
        Assert.Equal("9:TRL 10:TRL 11:PVL 12:PVL 13:PVL 14:PVL 15:KVI", fileCollection.CalcFileStats());
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            for (var i = 4; i < 10; i++)
            {
                cursor.FindExactKey(CreateBigKey(i, 4000));
                cursor.EraseCurrent();
            }

            tr.Commit();
        }

        Assert.Equal("9:TRL 10:TRL 11:PVL 12:PVL 13:PVL 14:PVL 15:KVI 16:TRL 17:TRL", fileCollection.CalcFileStats());
        await db.Compact(CancellationToken.None);
        Assert.Equal("11:PVL 12:PVL 17:TRL 18:KVI", fileCollection.CalcFileStats());
    }

    [Fact]
    public async Task PreapprovedCommitAndCompaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024);
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key1, new byte[1024]);
            cursor.CreateOrUpdateKeyValue(Key2, new byte[10]);
            tr.Commit();
        }

        await db.Compact(CancellationToken.None);
        using (var tr = await db.StartWritingTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.FindKeyIndex(0);
            cursor.EraseCurrent();
            tr.Commit();
        }

        await db.Compact(CancellationToken.None);
        using (var db2 = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024))
        {
            using (var tr = db2.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                Assert.False(cursor.FindExactKey(Key1));
                Assert.True(cursor.FindExactKey(Key2));
            }
        }
    }

    [Fact]
    public void ReportTransactionLeak()
    {
        using var fileCollection = new InMemoryFileCollection();
        using var db = NewKeyValueDB(fileCollection, new NoCompressionStrategy(), 1024);
        var logger = new LoggerMock();
        db.Logger = logger;
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key1, new byte[1]);
            tr.Commit();
        }

        Assert.Equal(fileCollection.GetCount(), logger.TrlCreatedCount);
        StartLeakingTransaction(db);
        GC.Collect(GC.MaxGeneration);
        GC.WaitForPendingFinalizers();
        Assert.NotNull(logger.Leaked);
        Assert.Equal("Leak", logger.Leaked.DescriptionForLeaks);
    }

    static void StartLeakingTransaction(IKeyValueDB db)
    {
        db.StartTransaction().DescriptionForLeaks = "Leak";
    }

    class LoggerMock : IKeyValueDBLogger
    {
        public IKeyValueDBTransaction? Leaked;
        public TimeSpan KviTime;
        public string? LastWarning;
        public uint TrlCreatedCount;
        public uint MarkedForDeleteCount;

        public void ReportTransactionLeak(IKeyValueDBTransaction transaction)
        {
            Leaked = transaction;
        }

        public void CompactionStart(ulong totalWaste)
        {
        }

        public void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory)
        {
        }

        public void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration,
            ulong beforeCompressionSize)
        {
            KviTime = duration;
        }

        public void TransactionLogCreated(uint fileId)
        {
            TrlCreatedCount++;
        }

        public void FileMarkedForDelete(uint fileId)
        {
            MarkedForDeleteCount++;
        }

        public void LogWarning(string message)
        {
            LastWarning = message;
        }
    }

    [Fact]
    public void CompactionLimitsKviWriteSpeed()
    {
        using var fileCollection = new InMemoryFileCollection();
        var logger = new LoggerMock();
        using var db = NewKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = fileCollection,
            Compression = new NoCompressionStrategy(),
            CompactorScheduler = null,
            CompactorWriteBytesPerSecondLimit = 20000,
            FileSplitSize = 60000,
            Logger = logger
        });
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            var key = new byte[100];
            for (var i = 0; i < 256; i++)
            {
                for (var j = 0; j < 100; j++) key[j] = (byte)i;
                cursor.CreateOrUpdateKeyValue(key, key);
            }

            tr.Commit();
        }

        db.Compact(CancellationToken.None);
        // Kvi size = 27640 => ~1.4s
        Assert.InRange(logger.KviTime.TotalMilliseconds, 1000, 2000);
    }

    [Fact]
    public async Task BigCompaction()
    {
        using var fileCollection = new InMemoryFileCollection();
        var logger = new LoggerMock();
        using var db = NewKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = fileCollection,
            Compression = new NoCompressionStrategy(),
            CompactorScheduler = null,
            FileSplitSize = 10000,
            Logger = logger
        });
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            var key = new byte[100];
            var value = new byte[2000];
            for (var i = 0; i < 2000; i++)
            {
                PackUnpack.PackInt32BE(key, 0, i);
                cursor.CreateOrUpdateKeyValue(key, value);
            }

            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            var key = new byte[100];
            for (var i = 0; i < 2000; i += 2)
            {
                PackUnpack.PackInt32BE(key, 0, i);
                cursor.FindExactKey(key);
                cursor.EraseCurrent();
            }

            for (var i = 0; i < 2000; i += 3)
            {
                PackUnpack.PackInt32BE(key, 0, i);
                if (cursor.FindExactKey(key))
                    cursor.EraseCurrent();
            }

            Assert.Equal(667, tr.GetKeyValueCount());
            tr.Commit();
        }

        await db.Compact(CancellationToken.None);
        Assert.Equal(93u, logger.MarkedForDeleteCount);
        Assert.Equal(
            "Compactor didn't removed all waste (2375267), because it created 20 PVL files already. Remaining waste left to next compaction.",
            logger.LastWarning);
        await db.Compact(CancellationToken.None);
        Assert.Equal(174u, logger.MarkedForDeleteCount);
        Assert.Equal(
            "Compactor didn't removed all waste (1859507), because it created 20 PVL files already. Remaining waste left to next compaction.",
            logger.LastWarning);
    }

    [Fact]
    public void OpeningDbWithMissingFirstTrlAndKviWarnsAndOpenEmptyDb()
    {
        using var fileCollection = new InMemoryFileCollection();
        var options = new KeyValueDBOptions
        {
            FileCollection = fileCollection, Compression = new NoCompressionStrategy(), FileSplitSize = 1024,
            Logger = new LoggerMock()
        };
        Create2TrlFiles(options);
        fileCollection.GetFile(1)!.Remove();
        using var db = NewKeyValueDB(options);
        Assert.Equal("No valid Kvi and lowest Trl in chain is not first. Missing 1",
            ((LoggerMock)options.Logger).LastWarning);
        using var tr = db.StartTransaction();
        Assert.Equal(0, tr.GetKeyValueCount());
        Assert.Equal(0u, fileCollection.GetCount());
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue(Key1, new byte[1024]);
        tr.Commit();
    }

    [Fact]
    public void OpeningDbWithLenientOpenWithMissingFirstTrlAndKviWarnsAndRecoversData()
    {
        using var fileCollection = new InMemoryFileCollection();
        var options = new KeyValueDBOptions
        {
            FileCollection = fileCollection, Compression = new NoCompressionStrategy(), FileSplitSize = 1024,
            LenientOpen = true,
            Logger = new LoggerMock()
        };
        Create2TrlFiles(options);
        fileCollection.GetFile(1)!.Remove();
        using var db = NewKeyValueDB(options);
        Assert.Equal(
            "No valid Kvi and lowest Trl in chain is not first. Missing 1. LenientOpen is true, recovering data.",
            ((LoggerMock)options.Logger).LastWarning);
        using var tr = db.StartTransaction();
        Assert.Equal(1, tr.GetKeyValueCount());
        Assert.Equal(1u, fileCollection.GetCount());
    }

    void Create2TrlFiles(KeyValueDBOptions options)
    {
        using var db = NewKeyValueDB(options);
        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key1, new byte[1024]);
            tr.Commit();
        }

        using (var tr = db.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue(Key3, new byte[50]);
            tr.Commit();
        }
    }

    [Fact]
    public async Task CompactorEvenWithLossOfNotFlushedDataWillNotAnyData()
    {
        using var fc = new InMemoryFileCollection();
        {
            using var db = NewKeyValueDB(fc, new NoCompressionStrategy(), 1024);
            {
                using var tr = db.StartTransaction();
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(new byte[10], new byte[400]);
                tr.Commit();
            }
            {
                using var tr = db.StartTransaction();
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(new byte[40], new byte[560]);
                tr.Commit();
            }
            await db.Compact(CancellationToken.None);

            {
                using var tr = db.StartTransaction();
                using var cursor = tr.CreateCursor();
                cursor.FindFirstKey(new byte[40]);
                cursor.EraseCurrent();
                cursor.CreateOrUpdateKeyValue(new byte[10], new byte[100]);
                tr.Commit();
            }
            await db.Compact(CancellationToken.None);
            fc.SimulateDataLossOfNotFlushedData();
        }
        {
            using var db = NewKeyValueDB(fc, new NoCompressionStrategy(), 1024);
            using var tr = db.StartTransaction();
            using var cursor = tr.CreateCursor();
            Assert.Equal(FindResult.Exact, cursor.Find(new byte[10], 0));
            Assert.Equal(100, cursor.SlowGetValue().Length);
        }
    }

    public void CanChangeKeySuffixAndDataAreCorrectlyReplayedFromTrl(int keyLength)
    {
        using var fc = new InMemoryFileCollection();
        {
            using var db = NewKeyValueDB(fc);
            using var tr = db.StartTransaction();
            using var cursor = tr.CreateCursor();
            var key = new byte[keyLength];
            for (var i = 0; i < 250; i++)
            {
                key[10] = (byte)i;
                cursor.CreateOrUpdateKeyValue(key, new byte[i]);
            }

            key[keyLength - 1] = 1;
            for (var i = 0; i < 250; i++)
            {
                key[10] = (byte)i;
                Assert.Equal(UpdateKeySuffixResult.Updated, cursor.UpdateKeySuffix(key, (uint)keyLength / 2));
                Assert.True(key.AsSpan().SequenceEqual(cursor.SlowGetKey().AsSpan()));
                Assert.Equal(i, cursor.SlowGetValue().Length);
                Assert.Equal(i, cursor.GetKeyIndex());
                Assert.Equal(250, tr.GetKeyValueCount());
            }

            tr.Commit();
        }
        {
            using var db = NewKeyValueDB(fc);
            using var tr = db.StartTransaction();
            using var cursor = tr.CreateCursor();
            var key = new byte[keyLength];
            key[keyLength - 1] = 1;
            for (var i = 0; i < 250; i++)
            {
                key[10] = (byte)i;
                Assert.True(cursor.FindExactKey(key));
                Assert.Equal(i, cursor.SlowGetValue().Length);
            }
        }
    }

    protected KeyValueDBFileTestBase(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }
}
