using System;
using System.IO;
using System.Threading.Tasks;
using BTDB.Replication.Test.Simulation;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;

namespace BTDB.Replication.Test;

public class FollowerComparisonSessionTest
{
    static LeaderTrlProgress Progress(Node node, ulong eventId = 1) =>
        new(eventId, node.Capture.Completed.FileId, node.Capture.Completed.Offset);

    [Fact]
    public async Task NewLeaderCannotConfirmBytesOnlyAcknowledgedByItsPredecessor()
    {
        using var follower = await Node.Create(false);
        using var leader = await Node.Create(false);
        var restoredBase = follower.Capture.Acknowledged;
        await follower.Write(1, 1);
        await leader.Write(1, 9);
        follower.Capture.Acknowledge(follower.Capture.Completed);
        using var reader = leader.Reader();
        var clock = new DeterministicScheduler(305);
        var restarts = 0;
        var session = new FollowerComparisonSession(follower.Files, follower.Capture, reader,
            clock.CreateScope("follower"), () => restarts++, restoredBase);
        session.NotifyProgress(Progress(leader));
        Assert.Equal(TrlCompareResult.Diverged, await session.CompareLatestAsync());
        Assert.Equal(1, restarts);
        Assert.Null(session.Confirmed);
    }

    [Fact]
    public async Task CoalescesProgressRetriesLagAndExpiresOnlyLiveConfirmation()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        using var reader = leader.Reader();
        var clock = new DeterministicScheduler(301);
        var session = new FollowerComparisonSession(follower.Files, follower.Capture, reader,
            clock.CreateScope("follower"), () => Assert.Fail("Unexpected restart"));
        Assert.Null(await session.CompareLatestAsync());
        await leader.Write(1, 1);
        var first = Progress(leader);
        session.NotifyProgress(first);
        await leader.Write(1, 2); // A changed cut with unchanged event ID must not be discarded.
        var latest = Progress(leader);
        session.NotifyProgress(latest);
        session.NotifyProgress(first);
        Assert.Equal(TrlCompareResult.LocalBehind, await session.CompareLatestAsync());
        await follower.Write(1, 1);
        await follower.Write(1, 2);
        Assert.Equal(TrlCompareResult.Matched, await session.CompareLatestAsync());
        Assert.Equal(latest, session.Compared);
        Assert.Null(session.Confirmed);
        var challenge = session.BeginChallenge(TimeSpan.FromTicks(10));
        Assert.True(session.AcceptChallenge(challenge));
        Assert.Equal(latest, session.Confirmed);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        Assert.Null(session.Confirmed);
        Assert.Equal(latest, session.Compared);
        await follower.Write(2, 3);
        Assert.NotEqual(latest.Position, follower.Capture.Completed);
        session.Close();
    }

    [Fact]
    public async Task ProgressDuringReadKeepsTheComparedCutFixedAndExpiryCannotConfirmIt()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        await leader.Write(1, 1);
        await follower.Write(1, 1);
        using var reader = leader.Reader();
        var clock = new DeterministicScheduler(304);
        var session = new FollowerComparisonSession(follower.Files, follower.Capture, reader,
            clock.CreateScope("follower"), () => Assert.Fail("Unexpected restart"));
        var first = Progress(leader);
        session.NotifyProgress(first);
        Assert.True(session.AcceptChallenge(session.BeginChallenge(TimeSpan.FromTicks(10))));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        reader.BeforeRead = async (_, _, cancellation) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(cancellation);
        };
        var pending = session.CompareLatestAsync().AsTask();
        await entered.Task;
        await leader.Write(2, 2);
        await follower.Write(2, 2);
        var latest = Progress(leader, 2);
        session.NotifyProgress(latest);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        release.SetResult();
        Assert.Equal(TrlCompareResult.Matched, await pending);
        Assert.Equal(first, session.Compared);
        Assert.Null(session.Confirmed);
        Assert.Equal(TrlCompareResult.Matched, await session.CompareLatestAsync());
        Assert.Equal(latest, session.Compared);
        session.Close();
    }

    [Fact]
    public async Task CloseCancelsAnOutstandingReadAndRejectsLateGrant()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        await leader.Write(1, 1);
        await follower.Write(1, 1);
        using var reader = leader.Reader();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        reader.BeforeRead = async (_, _, cancellation) =>
        {
            entered.SetResult();
            await Task.Delay(-1, cancellation);
        };
        var clock = new DeterministicScheduler(302);
        var session = new FollowerComparisonSession(follower.Files, follower.Capture, reader,
            clock.CreateScope("follower"), () => Assert.Fail("Disconnect is not divergence"));
        var challenge = session.BeginChallenge(TimeSpan.FromSeconds(1));
        session.NotifyProgress(Progress(leader));
        var start = follower.Capture.Acknowledged;
        var pending = session.CompareLatestAsync().AsTask();
        await entered.Task;
        session.Close();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.False(session.AcceptChallenge(challenge));
        Assert.Null(session.Confirmed);
        Assert.Equal(start, follower.Capture.Acknowledged);
        await follower.Write(2, 2);
    }

    [Fact]
    public async Task FailureCanRetryButDivergenceClosesAndRequestsRestartOnce()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        await leader.Write(1, 1);
        await follower.Write(1, 2);
        using var reader = leader.Reader();
        var clock = new DeterministicScheduler(303);
        var restarts = 0;
        var session = new FollowerComparisonSession(follower.Files, follower.Capture, reader,
            clock.CreateScope("follower"), () => restarts++);
        session.NotifyProgress(Progress(leader));
        reader.TruncateRead = true;
        await Assert.ThrowsAsync<IOException>(() => session.CompareLatestAsync().AsTask());
        Assert.Equal(0, restarts);
        reader.TruncateRead = false;
        Assert.Equal(TrlCompareResult.Diverged, await session.CompareLatestAsync());
        Assert.Equal(1, restarts);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => session.CompareLatestAsync().AsTask());
        session.Close();
        Assert.Equal(1, restarts);
        Assert.Null(session.Confirmed);
        await follower.Write(2, 2);
    }
}
