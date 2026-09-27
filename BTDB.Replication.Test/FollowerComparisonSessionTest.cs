using System;
using System.IO;
using System.Threading.Tasks;
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
        var restarts = 0;
        var session = new FollowerComparisonSession(follower.Files.GetFile, follower.Capture, reader,
            () => restarts++, restoredBase);
        Assert.Equal(TrlCompareResult.Diverged, await session.CompareAsync(Progress(leader)));
        Assert.Equal(1, restarts);
    }

    [Fact]
    public async Task EachPollSuppliesItsCutAndRetriesLocalLag()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        using var reader = leader.Reader();
        var session = new FollowerComparisonSession(follower.Files.GetFile, follower.Capture, reader,
            () => Assert.Fail("Unexpected restart"), follower.Capture.Acknowledged);
        await leader.Write(1, 1);
        var first = Progress(leader);
        Assert.Equal(TrlCompareResult.LocalBehind, await session.CompareAsync(first));
        await leader.Write(1, 2); // A changed cut with unchanged event ID must not be discarded.
        var latest = Progress(leader);
        Assert.Equal(TrlCompareResult.LocalBehind, await session.CompareAsync(latest));
        await follower.Write(1, 1);
        await follower.Write(1, 2);
        Assert.Equal(TrlCompareResult.Matched, await session.CompareAsync(latest));
        Assert.Equal(latest, session.Compared);
        Assert.Equal(latest.Position, session.ComparedPosition);
        await follower.Write(2, 3);
        Assert.NotEqual(latest.Position, follower.Capture.Completed);
        session.Close();
    }

    [Fact]
    public async Task LocalCommitsDuringReadKeepTheRequestedCutFixed()
    {
        using var leader = await Node.Create(false);
        using var follower = await Node.Create(false);
        await leader.Write(1, 1);
        await follower.Write(1, 1);
        using var reader = leader.Reader();
        var session = new FollowerComparisonSession(follower.Files.GetFile, follower.Capture, reader,
            () => Assert.Fail("Unexpected restart"), follower.Capture.Acknowledged);
        var first = Progress(leader);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        reader.BeforeRead = async (_, _, cancellation) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(cancellation);
        };
        var pending = session.CompareAsync(first).AsTask();
        await entered.Task;
        await leader.Write(2, 2);
        await follower.Write(2, 2);
        var latest = Progress(leader, 2);
        release.SetResult();
        Assert.Equal(TrlCompareResult.Matched, await pending);
        Assert.Equal(first, session.Compared);
        Assert.Equal(TrlCompareResult.Matched, await session.CompareAsync(latest));
        Assert.Equal(latest, session.Compared);
        session.Close();
    }

    [Fact]
    public async Task CloseCancelsAnOutstandingRead()
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
        var session = new FollowerComparisonSession(follower.Files.GetFile, follower.Capture, reader,
            () => Assert.Fail("Disconnect is not divergence"), follower.Capture.Acknowledged);
        var start = follower.Capture.Acknowledged;
        var pending = session.CompareAsync(Progress(leader)).AsTask();
        await entered.Task;
        session.Close();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
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
        var restarts = 0;
        var session = new FollowerComparisonSession(follower.Files.GetFile, follower.Capture, reader,
            () => restarts++, follower.Capture.Acknowledged);
        var progress = Progress(leader);
        reader.TruncateRead = true;
        await Assert.ThrowsAsync<IOException>(() => session.CompareAsync(progress).AsTask());
        Assert.Equal(0, restarts);
        reader.TruncateRead = false;
        Assert.Equal(TrlCompareResult.Diverged, await session.CompareAsync(progress));
        Assert.Equal(1, restarts);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => session.CompareAsync(progress).AsTask());
        session.Close();
        Assert.Equal(1, restarts);
        await follower.Write(2, 2);
    }
}
