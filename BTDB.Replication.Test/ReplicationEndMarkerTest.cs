using System;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;

namespace BTDB.Replication.Test;

public class ReplicationEndMarkerTest
{
    [Fact]
    public async Task ReplicationWritesNoTemporaryEndMarkerAndReopensTheCommittedTail()
    {
        var node = await Node.Create(false);
        try
        {
            using (var transaction = await node.Db.StartWritingTransaction(1ul))
            {
                transaction.NextCommitTemporaryCloseTransactionLog(); // Still forces a commit without changes.
                transaction.Commit();
            }
            var end = node.Capture.Completed;
            Assert.NotEqual(0u, end.FileId);
            Assert.Equal((ulong)end.Offset, node.Files.GetFile(end.FileId)!.GetSize());
            node.Db.Dispose();
            Assert.Equal((ulong)end.Offset, node.Files.GetFile(end.FileId)!.GetSize()); // No shutdown marker.
            var capture = new TransactionLogCapture();
            node.Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = new LocalReplicatedCollection(node.Files), TransactionLogCapture = capture,
                Compression = new NoCompressionStrategy(), CompactorScheduler = null
            });
            using (var transaction = await node.Db.StartWritingTransaction(2ul))
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([2], [2]);
                transaction.Commit();
            }
            Assert.Equal(end.FileId, capture.Completed.FileId); // The committed tail continues in the same file.
            Assert.True(capture.Completed.Offset > end.Offset);
        }
        finally
        {
            node.Dispose();
        }
    }

    [Fact]
    public async Task ReplicationRotationWritesNoEndOfFileAndReplaysMultiFileTransactions()
    {
        var node = await Node.Create(); // Tiny logs rotate every few transactions.
        try
        {
            for (ulong id = 1; id <= 12; id++)
            {
                using var transaction = await node.Db.StartWritingTransaction(id);
                using var cursor = transaction.CreateCursor();
                // Transaction 6 writes more than a whole TRL holds, so it spans several files.
                for (var part = 0; part < (id == 6 ? 5 : 1); part++)
                    cursor.CreateOrUpdateKeyValue([(byte)id, (byte)part], Enumerable.Repeat((byte)0x55, 700).ToArray());
                transaction.Commit();
            }
            var tail = node.Capture.Completed.FileId;
            var sealedLogs = node.Files.Enumerate()
                .Where(f => node.Files.GetFileType(f.Index) == KVFileType.TransactionLog && f.Index != tail).ToArray();
            Assert.True(sealedLogs.Length >= 4);
            foreach (var file in sealedLogs)
            {
                var last = new byte[1];
                file.RandomRead(last, file.GetSize() - 1, false);
                Assert.NotEqual((byte)KVCommandType.EndOfFile, last[0]);
            }
            node.Db.Dispose();
            node.Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = new LocalReplicatedCollection(node.Files), TransactionLogCapture = new TransactionLogCapture(),
                Compression = new NoCompressionStrategy(), CompactorScheduler = null,
                TransactionLogSizeStrategy = new TrlPrefixComparerTest.TinyLogs()
            });
            using var read = node.Db.StartReadOnlyTransaction();
            Assert.Equal(12ul, read.GetCommitUlong());
            Assert.Equal(16, read.GetKeyValueCount());
            using var check = read.CreateCursor();
            Assert.True(check.FindExactKey([6, 4]));
            Span<byte> buffer = default;
            Assert.Equal(700, check.GetValueSpan(ref buffer).Length);
        }
        finally
        {
            node.Dispose();
        }
    }
}
