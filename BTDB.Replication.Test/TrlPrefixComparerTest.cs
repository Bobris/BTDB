using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class TrlPrefixComparerTest
{
    sealed class TinyLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint fileId) => new(1024, 1536);
    }

    sealed class Node : IDisposable
    {
        public readonly InMemoryReplicationFileStorage Files = new();
        public readonly TransactionLogCapture Capture = new();
        public BTreeKeyValueDB Db = null!;
        public static async Task<Node> Create(bool tiny = true, bool legacyEven = false)
        {
            var node = new Node();
            NodeFixture.SeedNativeHeader(node.Files, new Guid("5d076258-e492-4931-a5b8-a19dc9fe6c76"));
            if (legacyEven)
            {
                var original = node.Files.GetFile(1)!;
                var bytes = new byte[checked((int)original.GetSize())];
                original.RandomRead(bytes, 0, false);
                original.Remove();
                var writer = new MemWriter(node.Files.ImportFile(2, "trl").GetAppenderWriter());
                writer.WriteBlock(bytes);
                writer.Flush();
            }
            node.Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = new LocalReplicatedCollection(node.Files), TransactionLogCapture = node.Capture,
                Compression = new NoCompressionStrategy(), CompactorScheduler = null,
                TransactionLogSizeStrategy = tiny ? new TinyLogs() : null
            });
            return node;
        }
        public async Task Write(ulong eventId, byte value, bool rollback = false, bool batch = false, int size = 700)
        {
            using var tr = await Db.StartWritingTransaction(eventId, batch);
            using var cursor = tr.CreateCursor();
            cursor.CreateOrUpdateKeyValue([(byte)eventId], Enumerable.Repeat(value, size).ToArray());
            if (!rollback) tr.Commit();
        }
        public LeaderReader Reader() => new(Files);
        public void Dispose() { Db.Dispose(); Files.Dispose(); }
    }

    sealed class LeaderReader(InMemoryReplicationFileStorage files) : ILeaderTrlReader, IDisposable
    {
        public int ReadChunkSize = int.MaxValue;
        public bool CorruptRead, TruncateRead;
        public uint? MissingFile;
        public Func<uint, ulong, CancellationToken, ValueTask>? BeforeRead;
        public async ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            if (BeforeRead != null) await BeforeRead(fileId, offset, cancellation);
            cancellation.ThrowIfCancellationRequested();
            if (MissingFile == fileId) throw new FileNotFoundException("Leader no longer retains this TRL.");
            var file = files.GetFile(fileId) ?? throw new FileNotFoundException();
            if (TruncateRead) return 0;
            if (offset > file.GetSize()) throw new IOException("Range is beyond leader EOF.");
            var count = (int)Math.Min((ulong)Math.Min(destination.Length, ReadChunkSize), file.GetSize() - offset);
            file.RandomRead(destination.Span[..count], offset, false);
            if (CorruptRead && count != 0) destination.Span[0] ^= 1;
            return count;
        }
        public void Dispose() { }
    }

    [Fact]
    public async Task MatchingNativeTransactionsAcrossFilesAndDifferentBatchesAdvanceOnlyAcknowledgement()
    {
        using var leader = await Node.Create();
        using var follower = await Node.Create();
        for (ulong id = 1; id <= 8; id++)
        {
            await leader.Write(id, (byte)id, rollback: id == 3);
            await follower.Write(id, (byte)id, rollback: id == 3, batch: true);
        }
        Assert.Equal(leader.Capture.Completed, follower.Capture.Completed);
        using var remote = leader.Reader();
        Assert.True(leader.Files.GetCount() > 1);
        remote.ReadChunkSize = 17;
        using var before = follower.Db.StartReadOnlyTransaction();
        var visible = before.GetCommitUlong();
        var comparer = new TrlPrefixComparer(follower.Files, follower.Capture);
        Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(remote, leader.Capture.Completed));
        Assert.Equal(leader.Capture.Completed, follower.Capture.Acknowledged);
        using var after = follower.Db.StartReadOnlyTransaction();
        Assert.Equal(visible, after.GetCommitUlong());
        remote.BeforeRead = (_, _, _) => throw new InvalidOperationException("Duplicate progress must not read again.");
        Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(remote, leader.Capture.Completed));
    }

    [Fact]
    public async Task LegacyEvenBootstrapContinuesWithNextOddIdWithoutDiscovery()
    {
        using var leader = await Node.Create(legacyEven: true);
        using var follower = await Node.Create(legacyEven: true);
        Assert.Equal(2u, follower.Capture.Acknowledged.FileId);
        await leader.Write(1, 1);
        await follower.Write(1, 1);
        Assert.Equal(3u, leader.Capture.Completed.FileId);
        using var reader = leader.Reader();
        Assert.Equal(TrlCompareResult.Matched,
            await new TrlPrefixComparer(follower.Files, follower.Capture).CompareAsync(reader, leader.Capture.Completed));
    }

    [Fact]
    public async Task LagWaitsAndAnOlderCutExcludesLaterLocalBytes()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        await leader.Write(1, 1);
        using var remote = leader.Reader();
        var comparer = new TrlPrefixComparer(follower.Files, follower.Capture);
        Assert.Equal(TrlCompareResult.LocalBehind, await comparer.CompareAsync(remote, leader.Capture.Completed));
        await follower.Write(1, 1);
        await follower.Write(2, 2);
        Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(remote, leader.Capture.Completed));
        Assert.Equal(leader.Capture.Completed, follower.Capture.Acknowledged);
        Assert.NotEqual(follower.Capture.Completed, follower.Capture.Acknowledged);
    }

    [Fact]
    public async Task EqualEventIdDoesNotHideDifferentBytesAndDivergenceIsSticky()
    {
        using var leader = await Node.Create();
        using var follower = await Node.Create();
        await leader.Write(1, 1);
        await follower.Write(1, 2);
        using var remote = leader.Reader();
        var start = follower.Capture.Acknowledged;
        var comparer = new TrlPrefixComparer(follower.Files, follower.Capture);
        Assert.Equal(TrlCompareResult.Diverged, await comparer.CompareAsync(remote, leader.Capture.Completed));
        Assert.Equal(start, follower.Capture.Acknowledged);
        remote.BeforeRead = (_, _, _) => throw new InvalidOperationException("A divergent session cannot resume comparison.");
        Assert.Equal(TrlCompareResult.Diverged, await comparer.CompareAsync(remote, leader.Capture.Completed));
    }

    [Fact]
    public async Task UnchangedEventIdStillComparesNewNativeBytes()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        await leader.Write(1, 1);
        await follower.Write(1, 1);
        var comparer = new TrlPrefixComparer(follower.Files, follower.Capture);
        using (var first = leader.Reader())
            Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(first, leader.Capture.Completed));
        var old = follower.Capture.Acknowledged;
        await leader.Write(1, 2);
        await follower.Write(1, 2);
        using var next = leader.Reader();
        Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(next, leader.Capture.Completed));
        Assert.NotEqual(old, follower.Capture.Acknowledged);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LargePrefixUsesBoundedReadsAndDoesNotAcknowledgeBeforeTheLastBlock(bool corruptLast)
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        await leader.Write(1, 7, size: 200000);
        await follower.Write(1, 7, size: 200000);
        using var remote = leader.Reader();
        var start = follower.Capture.Acknowledged;
        var reads = 0;
        remote.BeforeRead = (_, offset, _) =>
        {
            reads++;
            Assert.Equal(start, follower.Capture.Acknowledged);
            if (corruptLast && offset >= 128 * 1024) remote.CorruptRead = true;
            return ValueTask.CompletedTask;
        };
        // The adapter limits each actual read, exercising the comparison's short-read loop too.
        remote.ReadChunkSize = 32000;
        var comparer = new TrlPrefixComparer(follower.Files, follower.Capture);
        Assert.Equal(corruptLast ? TrlCompareResult.Diverged : TrlCompareResult.Matched,
            await comparer.CompareAsync(remote, leader.Capture.Completed));
        Assert.True(reads > 4);
        Assert.Equal(corruptLast ? start : leader.Capture.Completed, follower.Capture.Acknowledged);
    }

    [Theory]
    [InlineData("cancel")]
    [InlineData("session")]
    [InlineData("truncated")]
    [InlineData("missing")]
    public async Task InterruptedComparisonNeverAcknowledgesPartialPrefixAndCanRetry(string fault)
    {
        using var leader = await Node.Create();
        using var follower = await Node.Create();
        for (ulong id = 1; id <= 6; id++)
        {
            await leader.Write(id, (byte)id);
            await follower.Write(id, (byte)id);
        }
        using var remote = leader.Reader();
        var start = follower.Capture.Acknowledged;
        var comparer = new TrlPrefixComparer(follower.Files, follower.Capture);
        using var cancellation = new CancellationTokenSource();
        var readCount = 0;
        if (fault == "missing")
            remote.MissingFile = leader.Files.Enumerate().Select(f => f.Index).Order().Skip(1).First();
        remote.BeforeRead = (_, _, _) =>
        {
            if (++readCount == 2)
            {
                if (fault == "cancel") cancellation.Cancel();
                if (fault == "session") throw new IOException("Leader session changed.");
                if (fault == "truncated") remote.TruncateRead = true;
            }
            return ValueTask.CompletedTask;
        };
        if (fault == "cancel")
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                comparer.CompareAsync(remote, leader.Capture.Completed, cancellation.Token).AsTask());
        else await Assert.ThrowsAnyAsync<IOException>(() => comparer.CompareAsync(remote, leader.Capture.Completed).AsTask());
        Assert.Equal(start, follower.Capture.Acknowledged);
        using var fresh = leader.Reader();
        Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(fresh, leader.Capture.Completed));
    }
}
