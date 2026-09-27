using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;

namespace BTDB.Replication.Test;

public class RetainingLeaderTrlReaderTest
{
    [Fact]
    public async Task RepeatedComparisonReusesRetainedLeaderBytesUntilReleased()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        var start = follower.Capture.Acknowledged;
        for (ulong id = 1; id <= 3; id++)
        {
            await leader.Write(id, (byte)id, size: 300_000);
            await follower.Write(id, (byte)id, size: 300_000);
        }
        using var inner = leader.Reader();
        var reads = 0;
        inner.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        var reader = new RetainingLeaderTrlReader(inner);
        var end = leader.Capture.Completed;
        var comparer = new TrlPrefixComparer(follower.Files.GetFile, follower.Capture, start);
        Assert.Equal(TrlCompareResult.Matched, await comparer.CompareAsync(reader, end));
        var firstReads = reads;
        Assert.True(firstReads > 0);
        var again = new TrlPrefixComparer(follower.Files.GetFile, follower.Capture, start);
        Assert.Equal(TrlCompareResult.Matched, await again.CompareAsync(reader, end));
        Assert.Equal(firstReads, reads); // The second comparison crossed no additional peer round trips.
        reader.Release(end);
        var fresh = new TrlPrefixComparer(follower.Files.GetFile, follower.Capture, start);
        Assert.Equal(TrlCompareResult.Matched, await fresh.CompareAsync(reader, end));
        Assert.True(reads > firstReads);
    }

    sealed class NoLeader : ILeaderTrlReader
    {
        public ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation) =>
            throw new InvalidOperationException($"Unexpected peer read of {fileId}@{offset}.");
    }

    [Fact]
    public async Task InlineFileSwitchAnswersEndOfFileAndContinuesIntoTheSuccessor()
    {
        var reader = new RetainingLeaderTrlReader(new NoLeader());
        reader.Retain(new(3, 10), [new(3, 10, new byte[] { 1, 2 }), new(7, 0, new byte[] { 3, 4, 5 })]);
        Assert.Equal(0, await reader.ReadAsync(3, 12, new byte[1], default)); // Known end, no peer round trip.
        Assert.Equal(new TransactionLogPosition(7, 3), reader.ContiguousEnd(new(3, 10)));
        Assert.Equal(new TransactionLogPosition(7, 3), reader.ContiguousEnd(new(3, 11)));
        Assert.Equal(new TransactionLogPosition(3, 5), reader.ContiguousEnd(new(3, 5))); // A gap stops the extension.
        await Assert.ThrowsAsync<InvalidOperationException>(() => reader.ReadAsync(7, 3, new byte[1], default).AsTask());
        reader.Release(new(7, 0));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reader.ReadAsync(3, 12, new byte[1], default).AsTask());
        Assert.Equal(new TransactionLogPosition(7, 3), reader.ContiguousEnd(new(7, 0)));
        // Requested exactly at a file end: the leader starts with the successor.
        reader.Retain(new(7, 3), [new(9, 0, new byte[] { 6 })]);
        Assert.Equal(0, await reader.ReadAsync(7, 3, new byte[1], default));
        Assert.Equal(new TransactionLogPosition(9, 1), reader.ContiguousEnd(new(7, 3)));
    }

    [Fact]
    public async Task RetentionIsBoundedAndLaterReadsGoToTheLeader()
    {
        using var leader = await Node.Create(false);
        await leader.Write(1, 1, size: 300_000);
        using var inner = leader.Reader();
        var reads = 0;
        inner.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        var reader = new RetainingLeaderTrlReader(inner, capacity: 1000);
        var buffer = new byte[800];
        var fileId = leader.Capture.Completed.FileId;
        Assert.Equal(800, await reader.ReadAsync(fileId, 0, buffer, default));
        Assert.Equal(800, await reader.ReadAsync(fileId, 800, buffer, default)); // Over capacity: not retained.
        Assert.Equal(2, reads);
        Assert.Equal(300, await reader.ReadAsync(fileId, 500, buffer, default)); // Served from the first chunk.
        Assert.Equal(800, await reader.ReadAsync(fileId, 800, buffer, default));
        Assert.Equal(3, reads);
    }
}
