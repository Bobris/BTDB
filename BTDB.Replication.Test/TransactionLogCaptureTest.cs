using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;

namespace BTDB.Replication.Test;

public class TransactionLogCaptureTest
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NonApplicationCommitsAreObservedLiveAndByReplayButRollbacksAreNot(bool tiny)
    {
        using var node = await Node.Create(tiny);
        await node.Write(1, 1);
        await node.Write(2, 2, rollback: true);
        await node.Write(2, 2, size: tiny ? 700 : 300_000);
        Assert.Equal(default, node.Capture.NonApplicationCommitted);
        await node.Write(2, 3); // Unchanged committed cursor, even though a write API supplied it explicitly.
        var schema = node.Capture.Completed;
        Assert.Equal(schema, node.Capture.NonApplicationCommitted);
        await node.Write(3, 3);
        await node.Write(3, 4, batch: true);
        var batched = node.Capture.Completed;
        Assert.Equal(batched, node.Capture.NonApplicationCommitted);
        await node.Write(4, 4);
        Assert.Equal(batched, node.Capture.NonApplicationCommitted);

        node.Db.Dispose();
        var capture = new TransactionLogCapture();
        node.Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(node.Files), TransactionLogCapture = capture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null,
            TransactionLogSizeStrategy = tiny ? new TrlPrefixComparerTest.TinyLogs() : null
        });
        Assert.Equal(batched, capture.NonApplicationCommitted);
    }
}
