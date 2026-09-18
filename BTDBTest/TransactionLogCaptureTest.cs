using System;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class TransactionLogCaptureTest
{
    sealed class TinyLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint fileId) => new(1024, 1536);
    }

    static void Put(IKeyValueDBTransaction transaction, byte key)
    {
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], new byte[700]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompactionRetainsUnacknowledgedHistory(bool captureEnabled)
    {
        using var files = new InMemoryFileCollection();
        var capture = new TransactionLogCapture();
        using var db = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = captureEnabled ? capture : null,
            CompactorScheduler = null, Compression = new NoCompressionStrategy(),
            UseOddTransactionLogIds = true, TransactionLogSizeStrategy = new TinyLogs()
        });
        TransactionLogPosition earlier = default;
        for (ulong i = 1; i <= 20; i++)
        {
            using var tr = await db.StartWritingTransaction(i);
            Put(tr, 0);
            tr.Commit();
            if (i == 10) earlier = capture.Completed;
        }
        var first = files.Enumerate().Min(f => f.Index);
        await db.Compact(CancellationToken.None);
        Assert.Equal(captureEnabled, files.GetFile(first) != null);
        if (!captureEnabled) return;
        capture.Acknowledge(earlier);
        await db.Compact(CancellationToken.None);
        Assert.Null(files.GetFile(first));
        Assert.NotNull(files.GetFile(earlier.FileId));
        capture.Acknowledge(capture.Completed);
        await db.Compact(CancellationToken.None);
        Assert.Null(files.GetFile(earlier.FileId));
        Assert.NotNull(files.GetFile(capture.Acknowledged.FileId));
        Assert.Throws<ArgumentOutOfRangeException>(() => capture.Acknowledge(earlier));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PositionAdvancesOnlyAtCompleteCommitOrRollback(bool batch)
    {
        using var files = new InMemoryFileCollection();
        var capture = new TransactionLogCapture();
        using var db = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = capture, CompactorScheduler = null,
            TransactionLogSizeStrategy = new TinyLogs(), Compression = new NoCompressionStrategy()
        });
        using (var tr = await db.StartWritingTransaction()) { }
        Assert.Equal(default, capture.Completed);
        for (ulong i = 1; i <= 1000; i++)
        {
            var previous = capture.Completed;
            using (var tr = await db.StartWritingTransaction(i, batch))
            {
                Put(tr, 0);
                Put(tr, 1);
                Assert.Equal(previous, capture.Completed);
                if (i % 2 == 0) tr.Commit();
            }
            Assert.NotEqual(previous, capture.Completed);
        }
        Assert.Equal(0u, capture.Acknowledged.Offset);
        var completed = capture.Completed;
        using (var tr = await db.StartWritingTransaction(1001ul, batch))
        {
            Put(tr, 2);
            Assert.Equal(completed, capture.Completed);
            capture.Acknowledge(completed);
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                capture.Acknowledge(new(completed.FileId, completed.Offset + 1)));
        }
        Assert.NotEqual(capture.Acknowledged, capture.Completed);
    }

    [Fact]
    public async Task EnablingCapturePreservesNativeBytesAcrossCommitRollbackAndRotations()
    {
        static async Task<byte[]> Run(bool enabled)
        {
            using var files = new InMemoryFileCollection();
            var capture = new TransactionLogCapture();
            var file = files.AddFile("trl", FileIdParity.Odd);
            var writer = new MemWriter(file.GetAppenderWriter());
            writer.WriteBlock("BTDB3"u8);
            writer.WriteGuid(new Guid("ce41a07c-fb38-4bf7-949e-c5076fc91cdb"));
            writer.WriteUInt8((byte)KVFileType.TransactionLog);
            writer.WriteVInt64(1);
            writer.WriteVInt32(0);
            writer.Flush();
            using (var db = new BTreeKeyValueDB(new KeyValueDBOptions
            {
                FileCollection = files, TransactionLogCapture = enabled ? capture : null, CompactorScheduler = null,
                Compression = new NoCompressionStrategy(), UseOddTransactionLogIds = true,
                TransactionLogSizeStrategy = new TinyLogs()
            }))
            {
                for (ulong i = 1; i <= 4; i++)
                {
                    using var transaction = await db.StartWritingTransaction(i, true);
                    for (byte k = 0; k < 5; k++) Put(transaction, k);
                    transaction.SetUlong(0, i);
                    if (i == 3) continue;
                    if (i == 4) transaction.NextCommitTemporaryCloseTransactionLog();
                    transaction.Commit();
                }
            }
            return files.Enumerate().OrderBy(f => f.Index).SelectMany(f =>
            {
                var bytes = new byte[f.GetSize()];
                f.RandomRead(bytes, 0, false);
                return bytes;
            }).ToArray();
        }
        Assert.Equal(await Run(false), await Run(true));
    }

}
