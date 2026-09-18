using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class WriterCancellationTest
{
    [Fact]
    public async Task CancellationWhileQueuedDoesNotLeakAWriterReservation()
    {
        using var files = new InMemoryFileCollection();
        using var db = new BTreeKeyValueDB(new KeyValueDBOptions { FileCollection = files, CompactorScheduler = null });
        using var active = await db.StartWritingTransaction(1ul);
        using var cancellation = new CancellationTokenSource();
        var schema = db.StartWritingTransaction(false, cancellation.Token).AsTask();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => schema);
        active.Commit();
        using var next = await db.StartWritingTransaction(2ul);
        next.Commit();
    }

}
