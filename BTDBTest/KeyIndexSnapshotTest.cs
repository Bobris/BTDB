using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class KeyIndexSnapshotTest
{
    [Fact]
    public async Task CancellingRemoteExportLeavesRunningLocalCompactionAlive()
    {
        using var files = new InMemoryFileCollection();
        using var output = new InMemoryFileCollection();
        using var db = new BTreeKeyValueDB(new KeyValueDBOptions { FileCollection = files, CompactorScheduler = null, FileSplitSize = 1024, Compression = new NoCompressionStrategy() });
        using (var tr = await db.StartWritingTransaction(1ul))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue("key"u8, new byte[1000]);
            tr.Commit();
        }
        using var snapshot = db.CaptureKeyIndexSnapshot();
        using var localCancellation = new CancellationTokenSource();
        using var remoteCancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        db.CompactorStartAction = async token =>
        {
            entered.SetResult();
            await release.Task.WaitAsync(token);
        };
        var compact = db.Compact(localCancellation.Token).AsTask();
        await entered.Task;
        remoteCancellation.Cancel();
        var destination = output.AddFile("kvi");
        Assert.Throws<OperationCanceledException>(() => snapshot.WriteTo(destination.GetAppenderWriter(), 1000,
            new Dictionary<uint, uint>(), remoteCancellation.Token));
        Assert.False(compact.IsCompleted);
        Assert.False(localCancellation.IsCancellationRequested);
        release.SetResult();
        await compact;
        using var reader = db.StartReadOnlyTransaction();
        Assert.Equal(1ul, reader.GetCommitUlong());
    }

    [Fact]
    public async Task SnapshotRetainsFilesDuringLocalCompaction()
    {
        using var files = new InMemoryFileCollection();
        using var db = new BTreeKeyValueDB(new KeyValueDBOptions { FileCollection = files, CompactorScheduler = null, FileSplitSize = 1024, Compression = new NoCompressionStrategy() });
        using (var tr = await db.StartWritingTransaction(1ul))
        {
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue("key"u8, new byte[1000]);
            tr.Commit();
        }
        using var snapshot = db.CaptureKeyIndexSnapshot();
        var source = Assert.Single(snapshot.Sources);
        for (ulong eventId = 2; eventId <= 20; eventId++)
        {
            using var tr = await db.StartWritingTransaction(eventId);
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue("key"u8, new byte[1000]);
            tr.Commit();
        }
        await db.Compact(CancellationToken.None);
        Assert.NotNull(files.GetFile(source.FileId));

    }
}
