using System;
using System.Linq;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class TrlAllocationTest
{
    [Fact]
    public async Task DifferentRetainedLegacyFilesDoNotChangeNewTrlIdsOrBytes()
    {
        using var remote = new CheckpointPublisherTest.Storage();
        NodeFixture.SeedNativeHeader(remote.Files, new Guid("5d076258-e492-4931-a5b8-a19dc9fe6c76"));
        remote.Types.Add(1, KVFileType.TransactionLog);
        using var firstLocal = new InMemoryReplicationFileStorage();
        await using var firstFiles = new ReplicationFileSet(firstLocal, remote);
        await firstFiles.InitializeAsync();
        var writer = new MemWriter(remote.Files.ImportFile(101, "pvl").GetAppenderWriter());
        writer.WriteBlock([1, 2, 3]);
        writer.Flush();
        remote.Types.Add(101, KVFileType.PureValues);
        using var secondLocal = new InMemoryReplicationFileStorage();
        await using var secondFiles = new ReplicationFileSet(secondLocal, remote);
        await secondFiles.InitializeAsync();
        var firstCapture = new TransactionLogCapture();
        var secondCapture = new TransactionLogCapture();
        using var first = await Open(firstFiles, firstCapture);
        using var second = await Open(secondFiles, secondCapture);
        for (ulong id = 1; id <= 8; id++)
        {
            await Write(first, id);
            await Write(second, id);
        }
        Assert.Equal(firstCapture.Completed, secondCapture.Completed);
        var logs = firstLocal.Enumerate().Where(f => firstLocal.GetFileType(f.Index) == KVFileType.TransactionLog)
            .OrderBy(f => f.Index).ToArray();
        Assert.True(logs.Length > 2);
        Assert.Equal(Enumerable.Range(0, logs.Length).Select(i => (uint)(i * 2 + 1)), logs.Select(f => f.Index));
        foreach (var log in logs)
        {
            var other = secondLocal.GetFile(log.Index)!;
            Assert.Equal(log.GetSize(), other.GetSize());
            var bytes = new byte[checked((int)log.GetSize())];
            var otherBytes = new byte[bytes.Length];
            log.RandomRead(bytes, 0, false);
            other.RandomRead(otherBytes, 0, false);
            Assert.Equal(bytes, otherBytes);
        }
    }

    static ValueTask<BTreeKeyValueDB> Open(IFileReplicatedCollection files, TransactionLogCapture capture) =>
        BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = capture, Compression = new NoCompressionStrategy(),
            CompactorScheduler = null, TransactionLogSizeStrategy = new TrlPrefixComparerTest.TinyLogs()
        });

    static async Task Write(BTreeKeyValueDB db, ulong id)
    {
        using var transaction = await db.StartWritingTransaction(id);
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([(byte)id], Enumerable.Repeat((byte)id, 700).ToArray());
        transaction.Commit();
    }
}
