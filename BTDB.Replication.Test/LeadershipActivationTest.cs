using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using Xunit;
using Node = BTDB.Replication.Test.TrlPrefixComparerTest.Node;
using Storage = BTDB.Replication.Test.CanonicalTrlPublisherTest.Storage;

namespace BTDB.Replication.Test;

public class LeadershipActivationTest
{
    static LeaseAuthority Lease(DeterministicScheduler clock, string name)
    {
        var authority = new LeaseAuthority(clock.CreateScope(name), 0, TimeSpan.Zero);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromSeconds(60)));
        return authority;
    }
    static string Key(uint id) => $"{id}.trl";
    static ActivationDatabase Input(Node node, Storage remote) => new("main", node.Db, node.Capture, remote,
        new(Key(1), 1), new(1, 0), id => $"{id}.trl");

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task FailedParallelValidationReadCancelsSiblingsAndPreservesTheFailure(int failedBlock)
    {
        using var node = await Node.Create(false);
        await node.Write(1, 1, size: 2 * 1024 * 1024);
        var remote = new Storage();
        var clock = new DeterministicScheduler(712);
        using var original = new CanonicalTrlPublisher(node.Db, node.Capture, remote, Lease(clock, "old"), 1, Key);
        await original.PublishNextAsync();
        var writes = remote.Requests.Count;
        var failure = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = 0;
        var cancelled = 0;
        remote.BeforeRangeReadAsync = async (offset, token) =>
        {
            Interlocked.Increment(ref started);
            if (offset == failedBlock * 256 * 1024)
            {
                await failure.Task;
                return;
            }
            try { await Task.Delay(Timeout.Infinite, token); }
            catch (OperationCanceledException) { Interlocked.Increment(ref cancelled); throw; }
        };
        using var cancellation = new CancellationTokenSource();
        var run = LeadershipActivation.ActivateAsync(new(Lease(clock, "new"), 2, "session", ["main"]),
            [Input(node, remote)], cancellation.Token).AsTask();
        var expected = new IOException("Later validation block failed.");
        try
        {
            Assert.Equal(4, Volatile.Read(ref started));
            failure.SetException(expected);
            var actual = await Assert.ThrowsAsync<IOException>(() => run.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Same(expected, actual);
            Assert.Equal(3, Volatile.Read(ref cancelled));
            Assert.Equal(writes, remote.Requests.Count); // Validation failure must not adopt anything.
        }
        finally
        {
            cancellation.Cancel();
            await ((Task)run).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
        }
    }

    [Fact]
    public async Task ValidatesBlobDespitePeerAcknowledgementAndAdoptsWithoutPublishingOptimisticTail()
    {
        using var leader = await Node.Create();
        using var candidate = await Node.Create();
        var remote = new Storage();
        var clock = new DeterministicScheduler(701);
        using var original = new CanonicalTrlPublisher(leader.Db, leader.Capture, remote, Lease(clock, "old"), 1, Key);
        for (ulong id = 1; id <= 5; id++)
        {
            await leader.Write(id, (byte)id);
            await candidate.Write(id, (byte)id);
            if (id == 3) Assert.Equal(TrlPublishResult.Published, await original.PublishNextAsync());
        }
        var published = original.PublishedPosition;
        candidate.Capture.Acknowledge(candidate.Capture.Completed); // Direct peer comparison is ahead of Blob.
        var reads = 0;
        remote.BeforeRangeRead = _ => reads++;
        var publishers = (await LeadershipActivation.ActivateAsync(new(Lease(clock, "new"), 2, "new-session", ["main"]),
            [Input(candidate, remote)]))!;
        using var activated = Assert.Single(publishers);
        Assert.True(reads > 0);
        Assert.Equal(published, activated.PublishedPosition);
        Assert.NotNull(activated.Tail);
        Assert.Equal(TrlPublishResult.Published, await activated.PublishNextAsync());
        Assert.Equal(candidate.Capture.Completed, activated.PublishedPosition);
        Assert.All(remote.Requests.Where(r => r.Write.ExpectedToken == null),
            r => Assert.StartsWith("", r.Write.Key));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LaggingCandidateWaitsForLocalExecutionAndResumesValidation(bool tiny)
    {
        using var leader = await Node.Create(tiny);
        using var candidate = await Node.Create(tiny);
        var remote = new Storage();
        var clock = new DeterministicScheduler(704);
        using var original = new CanonicalTrlPublisher(leader.Db, leader.Capture, remote, Lease(clock, "old"), 1, Key);
        for (ulong id = 1; id <= 6; id++) await leader.Write(id, (byte)id);
        for (ulong id = 1; id <= 3; id++) await candidate.Write(id, (byte)id);
        Assert.Equal(TrlPublishResult.Published, await original.PublishNextAsync());
        var selected = new SelectedLeadership(Lease(clock, "new"), 2, "new-session", ["main"]);
        var validated = new Dictionary<string, TransactionLogPosition>(StringComparer.Ordinal);
        var writes = remote.Requests.Count;
        // A matching but shorter local prefix is not divergence: no restore, no adoption and no publisher yet.
        Assert.Null(await LeadershipActivation.ActivateAsync(selected, [Input(candidate, remote)], validated: validated));
        Assert.Equal(writes, remote.Requests.Count);
        var checkpoint = validated["main"];
        Assert.Equal(candidate.Capture.Completed, checkpoint);
        for (ulong id = 4; id <= 6; id++) await candidate.Write(id, (byte)id);
        var firstRead = uint.MaxValue;
        remote.BeforeRangeRead = offset => firstRead = Math.Min(firstRead, offset);
        using var activated = Assert.Single((await LeadershipActivation.ActivateAsync(selected, [Input(candidate, remote)],
            validated: validated))!);
        Assert.Equal(original.PublishedPosition, activated.PublishedPosition);
        Assert.NotNull(activated.Tail);
        if (checkpoint.FileId == original.PublishedPosition.FileId) Assert.Equal(checkpoint.Offset, firstRead);
    }

    [Fact]
    public async Task LaggingDatabaseDoesNotAdoptTheDatabasesBeforeIt()
    {
        using var leaderA = await Node.Create(false);
        using var leaderB = await Node.Create(false);
        using var candidateA = await Node.Create(false);
        using var candidateB = await Node.Create(false);
        var remoteA = new Storage();
        var remoteB = new Storage();
        var clock = new DeterministicScheduler(706);
        var old = Lease(clock, "old");
        using var originalA = new CanonicalTrlPublisher(leaderA.Db, leaderA.Capture, remoteA, old, 1, Key);
        using var originalB = new CanonicalTrlPublisher(leaderB.Db, leaderB.Capture, remoteB, old, 1, Key);
        for (ulong id = 1; id <= 3; id++)
        {
            await leaderA.Write(id, (byte)id);
            await leaderB.Write(id, (byte)id);
            await candidateA.Write(id, (byte)id);
        }
        await candidateB.Write(1, 1);
        Assert.Equal(TrlPublishResult.Published, await originalA.PublishNextAsync());
        Assert.Equal(TrlPublishResult.Published, await originalB.PublishNextAsync());
        var selected = new SelectedLeadership(Lease(clock, "new"), 2, "session", ["a", "b"]);
        ActivationDatabase[] inputs =
        [
            Input(candidateA, remoteA) with { Name = "a" }, Input(candidateB, remoteB) with { Name = "b" }
        ];
        var validated = new Dictionary<string, TransactionLogPosition>(StringComparer.Ordinal);
        var writesA = remoteA.Requests.Count;

        for (var retry = 0; retry < 3; retry++)
            Assert.Null(await LeadershipActivation.ActivateAsync(selected, inputs, validated: validated));
        Assert.Equal(writesA, remoteA.Requests.Count);
        for (ulong id = 2; id <= 3; id++) await candidateB.Write(id, (byte)id);
        var publishers = (await LeadershipActivation.ActivateAsync(selected, inputs, validated: validated))!;
        foreach (var publisher in publishers) publisher.Dispose();
        Assert.Equal(writesA + 1, remoteA.Requests.Count);
    }

    [Fact]
    public async Task LaggingCandidateWithDivergentPrefixStillRequiresRestore()
    {
        using var leader = await Node.Create(false);
        using var candidate = await Node.Create(false);
        var remote = new Storage();
        var clock = new DeterministicScheduler(705);
        using var original = new CanonicalTrlPublisher(leader.Db, leader.Capture, remote, Lease(clock, "old"), 1, Key);
        for (ulong id = 1; id <= 4; id++) await leader.Write(id, (byte)id);
        await candidate.Write(1, 1);
        await candidate.Write(2, 9);
        Assert.Equal(TrlPublishResult.Published, await original.PublishNextAsync());
        await Assert.ThrowsAsync<InvalidDataException>(() => LeadershipActivation.ActivateAsync(
            new(Lease(clock, "new"), 2, "session", ["main"]), [Input(candidate, remote)]).AsTask());
    }

    [Fact]
    public async Task ValidationFollowsSelectedLinksAcrossReservedOddIds()
    {
        using var leader = await Node.Create(reservedOdd: true);
        using var candidate = await Node.Create(reservedOdd: true);
        var remote = new Storage();
        var clock = new DeterministicScheduler(703);
        using var original = new CanonicalTrlPublisher(leader.Db, leader.Capture, remote, Lease(clock, "old"), 1, Key);
        for (ulong id = 1; id <= 3; id++)
        {
            await leader.Write(id, (byte)id);
            await candidate.Write(id, (byte)id);
        }
        Assert.Equal(TrlPublishResult.Published, await original.PublishNextAsync());
        Assert.True(original.PublishedPosition.FileId >= 5); // The chain continues 1 -> 5, skipping reserved 3.
        var publishers = (await LeadershipActivation.ActivateAsync(new(Lease(clock, "new"), 2, "new-session", ["main"]),
            [Input(candidate, remote)]))!;
        using var activated = Assert.Single(publishers);
        Assert.Equal(original.PublishedPosition, activated.PublishedPosition);
    }

    [Fact]
    public async Task ProgressingCanonicalValidationDoesNotExpireActivationDeadline()
    {
        using var node = await Node.Create(false);
        await node.Write(1, 1, size: 4 * 1024 * 1024);
        var remote = new Storage();
        var clock = new DeterministicScheduler(709);
        using var original = new CanonicalTrlPublisher(node.Db, node.Capture, remote, Lease(clock, "old"), 1, Key);
        await original.PublishNextAsync();
        var control = new LeaderSelectionTest.Storage();
        var scope = clock.CreateScope("new");
        var leases = new LeaseSessionController(control, scope, 0, TimeSpan.Zero);
        var authority = (await leases.MaintainAsync())!;
        var expired = 0;
        using var watchdog = new ReplicationProgressWatchdog(scope, TimeSpan.FromTicks(10), () => expired++, "activation");
        watchdog.Progress();
        // Each verified block progresses, but the complete activation lasts several deadline intervals. The fake
        // storage completes synchronously, so the clock advances when a read is issued: four pipelined reads must fit
        // inside one deadline, as overlapping real reads would.
        remote.BeforeRangeRead = _ => clock.AdvanceBy(TimeSpan.FromTicks(2));
        using var session = new LeadershipSession(new(control, leases, authority, LeaderSelectionTest.Candidate()),
            [Input(node, remote)], progress: watchdog.Progress);
        Assert.NotNull(await session.ActivateAsync());
        Assert.True(clock.Elapsed.Ticks > 30);
        Assert.Equal(0, expired);
    }

    [Fact]
    public async Task DivergenceNeverAdoptsOrReturnsPublishers()
    {
        using var leader = await Node.Create(false);
        using var candidate = await Node.Create(false);
        await leader.Write(1, 1);
        await candidate.Write(1, 9);
        var remote = new Storage();
        var clock = new DeterministicScheduler(702);
        using var original = new CanonicalTrlPublisher(leader.Db, leader.Capture, remote, Lease(clock, "old"), 1, Key);
        await original.PublishNextAsync();
        candidate.Capture.Acknowledge(candidate.Capture.Completed);
        var count = remote.Requests.Count;
        await Assert.ThrowsAsync<InvalidDataException>(() => LeadershipActivation.ActivateAsync(
            new(Lease(clock, "new"), 2, "session", ["main"]), [Input(candidate, remote)]).AsTask());
        Assert.Equal(count, remote.Requests.Count);
    }

    [Fact]
    public async Task AllDatabasesMustAdoptBeforeAnyPublisherIsReturnedAndRetryDoesNotPublishSuffix()
    {
        using var first = await Node.Create(false);
        using var second = await Node.Create(false);
        var firstRemote = new Storage();
        var secondRemote = new Storage();
        var clock = new DeterministicScheduler(704);
        using var firstOld = new CanonicalTrlPublisher(first.Db, first.Capture, firstRemote, Lease(clock, "old1"), 1, Key);
        using var secondOld = new CanonicalTrlPublisher(second.Db, second.Capture, secondRemote, Lease(clock, "old2"), 1, Key);
        await first.Write(1, 1);
        await second.Write(1, 1);
        await firstOld.PublishNextAsync();
        await secondOld.PublishNextAsync();
        var firstCut = firstOld.PublishedPosition;
        await first.Write(2, 2);
        var selected = new SelectedLeadership(Lease(clock, "new"), 2, "session", ["main", "other"]);
        secondRemote.BeforeRangeRead = _ => throw new IOException("Second database download interrupted.");
        await Assert.ThrowsAsync<IOException>(() => LeadershipActivation.ActivateAsync(selected,
            [Input(first, firstRemote), Input(second, secondRemote) with { Name = "other" }]).AsTask());
        Assert.Equal(firstCut.Offset, firstRemote.Blobs[firstOld.Tail!.Key].State.Length);
        secondRemote.BeforeRangeRead = null;
        var publishers = (await LeadershipActivation.ActivateAsync(selected,
            [Input(first, firstRemote), Input(second, secondRemote) with { Name = "other" }]))!;
        try
        {
            Assert.Equal(2, publishers.Count);
            Assert.Equal(firstCut, publishers[0].PublishedPosition);
            Assert.All(publishers, p => Assert.NotNull(p.Tail));
        }
        finally { foreach (var publisher in publishers) publisher.Dispose(); }
    }

    [Fact]
    public async Task AppendRacingAdoptionRequiresRediscoveryAndValidationOfItsActualBytes()
    {
        using var leader = await Node.Create(false);
        using var candidate = await Node.Create(false);
        await leader.Write(1, 1);
        await candidate.Write(1, 1);
        var remote = new Storage();
        var clock = new DeterministicScheduler(703);
        using var original = new CanonicalTrlPublisher(leader.Db, leader.Capture, remote, Lease(clock, "old"), 1, Key);
        await original.PublishNextAsync();
        var tail = original.Tail!;
        await leader.Write(2, 2);
        await candidate.Write(2, 2);
        var raced = false;
        remote.BeforeEffect = write =>
        {
            if (raced || write.AppendLength != 0) return;
            raced = true;
            remote.BeforeEffect = null;
            original.PublishNextAsync().AsTask().GetAwaiter().GetResult();
        };
        var selected = new SelectedLeadership(Lease(clock, "new"), 2, "session", ["main"]);
        await Assert.ThrowsAsync<IOException>(() => LeadershipActivation.ActivateAsync(selected, [Input(candidate, remote)]).AsTask());
        Assert.True(raced);
        var publishers = (await LeadershipActivation.ActivateAsync(selected, [Input(candidate, remote)]))!;
        using var activated = Assert.Single(publishers);
        Assert.NotEqual(tail.State.Length, activated.Tail!.State.Length);
        Assert.Equal(candidate.Capture.Completed, activated.PublishedPosition);
    }
}
