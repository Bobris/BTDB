using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class NativeFileRestoreTest
{
    sealed class TinyLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint transactionLogFileId) => new(1024, 1536);
    }

    static BTreeKeyValueDB Open(IFileCollection files) => new(new KeyValueDBOptions
    {
        FileCollection = files, CompactorScheduler = null,
        Compression = new NoCompressionStrategy(), TransactionLogSizeStrategy = new TinyLogs()
    });

    static void Put(IKeyValueDBTransaction tr, byte key)
    {
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([key], Enumerable.Repeat(key, 700).ToArray());
    }

    static byte[] Value(IKeyValueDBCursor cursor)
    {
        Span<byte> buffer = default;
        return cursor.GetValueSpan(ref buffer).ToArray();
    }

    static byte[] Bytes(IFileCollectionFile file)
    {
        var result = new byte[checked((int)file.GetSize())];
        file.RandomRead(result, 0, false);
        return result;
    }

    [Fact]
    public async Task RestoredNativeIdsRemainPhysicalIdsThroughKviAndTrl()
    {
        using var original = new InMemoryReplicationFileStorage();
        using (var db = Open(original))
        {
            for (byte i = 1; i < 10; i++)
            {
                using var tr = await db.StartWritingTransaction(i);
                Put(tr, i);
                tr.Commit();
            }
            db.CreateKvi(CancellationToken.None);
        }
        using var cache = new InMemoryReplicationFileStorage();
        foreach (var file in original.Enumerate().OrderBy(f => f.Index))
        {
            var target = cache.ImportFile(file.Index, "cache");
            var writer = new MemWriter(target.GetAppenderWriter());
            writer.WriteBlock(Bytes(file));
            writer.Flush();
            Assert.Equal(file.Index, target.Index);
        }
        using var restored = Open(cache);
        using var reader = restored.StartReadOnlyTransaction();
        using var cursor = reader.CreateCursor();
        for (byte i = 1; i < 10; i++)
        {
            Assert.True(cursor.FindExactKey([i]));
            Assert.Equal(Enumerable.Repeat(i, 700), Value(cursor));
        }
    }

}
