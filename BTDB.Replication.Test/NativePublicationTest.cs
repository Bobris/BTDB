using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class NativePublicationTest
{
    sealed class TinyLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint transactionLogFileId) => new(1024, 1536);
    }

    static BTreeKeyValueDB Open(InMemoryReplicationFileStorage files) => new(new KeyValueDBOptions
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
        TransactionLogSizeStrategy = new TinyLogs()
    });

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SelectedMultiFileTransactionReopensWithoutChangingNativeBytes(bool rollback)
    {
        using var source = new InMemoryReplicationFileStorage();
        NodeFixture.SeedNativeHeader(source, new Guid("b0e93627-7632-4dd9-914c-a79e112e8656"));
        byte[] initial;
        uint firstId;
        using (var db = Open(source))
        {
            using (var transaction = await db.StartWritingTransaction(1ul))
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([0], new byte[700]);
                transaction.Commit();
            }
            var first = Assert.Single(source.Enumerate());
            firstId = first.Index;
            initial = new byte[checked((int)first.GetSize())];
            first.RandomRead(initial, 0, true);
            using (var transaction = await db.StartWritingTransaction(2ul))
            {
                using var cursor = transaction.CreateCursor();
                for (byte i = 1; i <= 8; i++) cursor.CreateOrUpdateKeyValue([i], new byte[700]);
                if (!rollback) transaction.Commit();
            }
        }
        var native = source.Enumerate().OrderBy(f => f.Index).Select(file =>
        {
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, true);
            return (file.Index, Bytes: bytes);
        }).ToArray();
        Assert.True(native.Length > 2);
        Assert.True(native[0].Bytes.AsSpan(0, initial.Length).SequenceEqual(initial));
        var remote = new Dictionary<string, Blob>();
        for (var i = 0; i < native.Length; i++)
        {
            remote.Add($"{native[i].Index}.trl", new(native[i].Bytes));
            VerifyRestore(remote, firstId, i == native.Length - 1 && !rollback ? 2ul : 1ul,
                i == native.Length - 1 && !rollback ? 9 : 1);
        }
    }

    [Fact]
    public async Task NativeCheckpointStillNeedsItsReferencedValueFiles()
    {
        using var source = new InMemoryReplicationFileStorage();
        NodeFixture.SeedNativeHeader(source, new Guid("b0e93627-7632-4dd9-914c-a79e112e8656"));
        using (var db = Open(source))
        {
            using (var transaction = await db.StartWritingTransaction(1ul))
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([0], new byte[700]);
                transaction.Commit();
            }
            db.CreateKvi(default);
        }
        var snapshots = source.Enumerate().OrderBy(f => f.Index).Select(file =>
        {
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, true);
            return (file.Index, Bytes: bytes);
        }).ToArray();
        Assert.Contains(snapshots, f => f.Index % 2 == 0);
        (ulong EventId, byte[]? Value) ReadState(bool omitValueFiles)
        {
            using var restoredFiles = new InMemoryReplicationFileStorage();
            foreach (var snapshot in snapshots)
            {
                // Preserve IDs; delete the value-bearing TRL only after allocating the native file inventory.
                var file = restoredFiles.AddFile(snapshot.Index % 2 == 0 ? "kvi" : "trl");
                Assert.Equal(snapshot.Index, file.Index);
                var writer = new MemWriter(file.GetAppenderWriter());
                writer.WriteBlock(snapshot.Bytes);
                writer.Flush();
                file.HardFlush();
            }
            if (omitValueFiles) restoredFiles.GetFile(1)!.Remove();
            using var restored = Open(restoredFiles);
            using var transaction = restored.StartReadOnlyTransaction();
            using var cursor = transaction.CreateCursor();
            if (!cursor.FindExactKey([0])) return (transaction.GetCommitUlong(), null);
            Span<byte> buffer = default;
            return (transaction.GetCommitUlong(), cursor.GetValueSpan(ref buffer).ToArray());
        }
        var complete = ReadState(false);
        Assert.Equal(1ul, complete.EventId);
        Assert.Equal(new byte[700], complete.Value);
        // Native open may discard an unusable KVI and return an empty DB. Replication must validate the selected
        // recovery cursor/closure before making the node ready; successful construction is not proof of recovery.
        var missing = ReadState(true);
        Assert.Equal(0ul, missing.EventId);
        Assert.Null(missing.Value);
    }

    sealed record Blob(byte[] Content);

    static void VerifyRestore(Dictionary<string, Blob> remote, uint firstId, ulong eventId, int count)
    {
        using var files = new InMemoryReplicationFileStorage();
        foreach (var (key, blob) in remote.OrderBy(p => TrlFileName.FileIdFromKey(p.Key)))
        {
            var id = TrlFileName.FileIdFromKey(key);
            var file = files.ImportFile(id, "trl");
            var writer = new MemWriter(file.GetAppenderWriter());
            writer.WriteBlock(blob.Content);
            writer.Flush();
            file.HardFlush();
        }
        using var restored = Open(files);
        using var transaction = restored.StartReadOnlyTransaction();
        Assert.Equal(eventId, transaction.GetCommitUlong());
        Assert.Equal(count, transaction.GetKeyValueCount());
        using var cursor = transaction.CreateCursor();
        for (byte i = 0; i < count; i++)
        {
            Assert.True(cursor.FindExactKey([i]));
            Span<byte> buffer = default;
            Assert.Equal(new byte[700], cursor.GetValueSpan(ref buffer).ToArray());
        }
    }
}
