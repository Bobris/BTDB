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
            clock.CreateScope("first"), () => Assert.Fail("Unexpected divergence"));
        var follower2 = new FollowerComparisonSession(second.Files, second.Capture, reader2,
            clock.CreateScope("second"), () => Assert.Fail("Unexpected divergence"));
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
            var challenge = follower.BeginChallenge(TimeSpan.FromTicks(10));
            Assert.True(grants.TryIssue(TimeSpan.FromTicks(10)));
            Assert.True(follower.AcceptChallenge(challenge));
            Assert.Equal(TrlCompareResult.Matched, await follower.CompareLatestAsync());
            Assert.Equal(progress, follower.Confirmed);
        }
        grants.BeginDrain();
        Assert.False(grants.TryIssue(TimeSpan.FromTicks(10)));
        Assert.False(grants.IsDrained);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        Assert.True(grants.IsDrained);
        Assert.Null(follower1.Confirmed);
        Assert.Null(follower2.Confirmed);
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
}
