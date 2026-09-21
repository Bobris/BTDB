using System;
using System.IO;
using System.Linq;
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
    static string Key(uint id) => $"trl/{id}";
    static ActivationDatabase Input(Node node, Storage remote) => new("main", node.Db, node.Capture, remote,
        new(Key(1), 1), new(1, 0), id => $"term2/{id}");

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
        var publishers = await LeadershipActivation.ActivateAsync(new(Lease(clock, "new"), 2, "new-session", ["main"]),
            [Input(candidate, remote)]);
        using var activated = Assert.Single(publishers);
        Assert.True(reads > 0);
        Assert.Equal(published, activated.PublishedPosition);
        Assert.Equal(2ul, activated.Tail!.State.Metadata.Term);
        Assert.Equal(TrlPublishResult.Published, await activated.PublishNextAsync());
        Assert.Equal(candidate.Capture.Completed, activated.PublishedPosition);
        Assert.All(remote.Requests.Where(r => r.Write.ExpectedToken == null && r.Write.Metadata.Term == 2),
            r => Assert.StartsWith("term2/", r.Write.Key));
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
        var publishers = await LeadershipActivation.ActivateAsync(selected,
            [Input(first, firstRemote), Input(second, secondRemote) with { Name = "other" }]);
        try
        {
            Assert.Equal(2, publishers.Count);
            Assert.Equal(firstCut, publishers[0].PublishedPosition);
            Assert.All(publishers, p => Assert.Equal(2ul, p.Tail!.State.Metadata.Term));
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
            if (raced || write.Metadata.Term != 2) return;
            raced = true;
            remote.BeforeEffect = null;
            original.PublishNextAsync().AsTask().GetAwaiter().GetResult();
        };
        var selected = new SelectedLeadership(Lease(clock, "new"), 2, "session", ["main"]);
        await Assert.ThrowsAsync<IOException>(() => LeadershipActivation.ActivateAsync(selected, [Input(candidate, remote)]).AsTask());
        Assert.True(raced);
        var publishers = await LeadershipActivation.ActivateAsync(selected, [Input(candidate, remote)]);
        using var activated = Assert.Single(publishers);
        Assert.NotEqual(tail.State.Length, activated.Tail!.State.Length);
        Assert.Equal(candidate.Capture.Completed, activated.PublishedPosition);
    }
}
