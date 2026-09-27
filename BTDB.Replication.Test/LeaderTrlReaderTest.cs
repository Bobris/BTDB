using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;

namespace BTDB.Replication.Test;

public class LeaderTrlReaderTest
{
    [Fact]
    public async Task TwoFollowersCompareUnpublishedLeaderBytesAndDrainExistingGrants()
    {
        using var leader = await Node.Create();
        using var first = await Node.Create();
        using var second = await Node.Create();
        var clock = new DeterministicScheduler(401);
        var scope = clock.CreateScope("leader");
        var authority = new LeaseAuthority(scope, 0, TimeSpan.Zero);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromTicks(100)));
        var grants = new ConfirmationGrants(scope, authority);
        var reader = new LeaderTrlReader(leader.Db, leader.Capture, authority);
        var reader2 = new LeaderTrlReader(leader.Db, leader.Capture, authority);
        var follower1 = new FollowerComparisonSession(first.Files, first.Capture, reader,
            () => Assert.Fail("Unexpected divergence"));
        var follower2 = new FollowerComparisonSession(second.Files, second.Capture, reader2,
            () => Assert.Fail("Unexpected divergence"));
        for (ulong id = 1; id <= 8; id++)
        {
            await leader.Write(id, (byte)id);
            await first.Write(id, (byte)id, batch: true);
            await second.Write(id, (byte)id);
        }
        var end = leader.Capture.Completed;
        var progress = new LeaderTrlProgress(8, end.FileId, end.Offset);
        foreach (var follower in new[] { follower1, follower2 })
        {
            follower.NotifyProgress(progress);
            Assert.True(grants.TryIssue(TimeSpan.FromTicks(10)));
            Assert.Equal(TrlCompareResult.Matched, await follower.CompareLatestAsync());
            Assert.Equal(progress, follower.Compared);
        }
        grants.BeginDrain();
        Assert.False(grants.TryIssue(TimeSpan.FromTicks(10)));
        Assert.False(grants.IsDrained);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        Assert.True(grants.IsDrained);
        reader.Close();
        reader2.Close();
        await Assert.ThrowsAsync<IOException>(() => reader.ReadAsync(end.FileId, 0, new byte[1], default).AsTask());
        await first.Write(9, 9);
        await second.Write(9, 9);
        follower1.Close();
        follower2.Close();
    }

    [Fact]
    public async Task ReadStopsAtCompleteCutWhileLocalWriterHasUncommittedBytes()
    {
        using var leader = await Node.Create(false);
        await leader.Write(1, 1);
        var clock = new DeterministicScheduler(402);
        var authority = new LeaseAuthority(clock.CreateScope("leader"), 0, TimeSpan.Zero);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromTicks(100)));
        var reader = new LeaderTrlReader(leader.Db, leader.Capture, authority);
        var end = leader.Capture.Completed;
        using var transaction = await leader.Db.StartWritingTransaction(2);
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([2], new byte[200000]);
        Assert.Equal(end, leader.Capture.Completed);
        Assert.True(leader.Files.GetFile(end.FileId)!.GetSize() > end.Offset);
        Assert.Equal(0, await reader.ReadAsync(end.FileId, end.Offset, new byte[16], default));
        Assert.Equal(1, await reader.ReadAsync(end.FileId, end.Offset - 1, new byte[16], default));
        await Assert.ThrowsAsync<IOException>(() => reader.ReadAsync(end.FileId, end.Offset + 1, new byte[1], default).AsTask());
        await Assert.ThrowsAsync<IOException>(() => reader.ReadAsync(end.FileId + 2, 0, new byte[1], default).AsTask());
    }

    [Fact]
    public async Task CancellationAndLeaseExpiryRejectReadsWithoutStoppingLocalWrites()
    {
        using var leader = await Node.Create(false);
        await leader.Write(1, 1);
        var clock = new DeterministicScheduler(403);
        var authority = new LeaseAuthority(clock.CreateScope("leader"), 0, TimeSpan.Zero);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromTicks(10)));
        var reader = new LeaderTrlReader(leader.Db, leader.Capture, authority);
        var end = leader.Capture.Completed;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            reader.ReadAsync(end.FileId, 0, new byte[1], new CancellationToken(true)).AsTask());
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        await Assert.ThrowsAsync<IOException>(() => reader.ReadAsync(end.FileId, 0, new byte[1], default).AsTask());
        await leader.Write(2, 2);
    }

    static LeaderTrlReader Reader(Node node)
    {
        var clock = new DeterministicScheduler(402);
        var authority = new LeaseAuthority(clock.CreateScope("leader"), 0, TimeSpan.Zero);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromTicks(100)));
        return new(node.Db, node.Capture, authority);
    }

    [Fact]
    public async Task InlineBytesFollowLineageAcrossFilesAndMatchRangeReads()
    {
        using var leader = await Node.Create();
        await leader.Write(1, 1);
        var from = leader.Capture.Completed;
        for (ulong id = 2; id <= 12; id++) await leader.Write(id, (byte)id);
        var end = leader.Capture.Completed;
        Assert.True(end.FileId > from.FileId + 2);
        var reader = Reader(leader);
        var chunks = await reader.ReadInlineAsync(from, end, ReplicationPeerPoll.MaximumInlineBytes, default);
        Assert.Equal((from.FileId, from.Offset), (chunks[0].FileId, chunks[0].Offset));
        Assert.Equal(end, new(chunks[^1].FileId, chunks[^1].Offset + (uint)chunks[^1].Bytes.Length));
        new ReplicationPeerPoll(1, true, [new("main", new(12, end.FileId, end.Offset), null, chunks)])
            .Validate(1, [new("main", from)], ReplicationPeerPoll.MaximumInlineBytes);
        foreach (var chunk in chunks)
        {
            var expected = new byte[chunk.Bytes.Length];
            Assert.Equal(expected.Length, await reader.ReadAsync(chunk.FileId, chunk.Offset, expected, default));
            Assert.Equal(expected, chunk.Bytes.ToArray());
        }
    }

    [Fact]
    public async Task InlineBytesRespectBudgetAndStopAtUnservableRanges()
    {
        using var leader = await Node.Create(false);
        await leader.Write(1, 1);
        var from = leader.Capture.Completed;
        await leader.Write(2, 2, size: 10_000);
        var end = leader.Capture.Completed;
        var reader = Reader(leader);
        var limited = Assert.Single(await reader.ReadInlineAsync(from, end, 100, default));
        Assert.Equal(100, limited.Bytes.Length);
        Assert.Empty(await reader.ReadInlineAsync(from, end, 0, default));
        Assert.Empty(await reader.ReadInlineAsync(end, end, 100, default));
        Assert.Empty(await reader.ReadInlineAsync(new(end.FileId + 2, 0), new(end.FileId + 2, 10), 100, default));
        reader.Close();
        await Assert.ThrowsAsync<IOException>(() => reader.ReadInlineAsync(from, end, 100, default).AsTask());
    }
}
