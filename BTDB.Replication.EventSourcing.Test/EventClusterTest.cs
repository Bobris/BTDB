using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class EventClusterTest
{
    static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    static async Task Until(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(Wait);
        while (!condition()) await Task.Delay(5, timeout.Token);
    }

    [Fact]
    public async Task DiscoveryIncludesOnlyCaughtUpConsumersAndTracksLeaderAndGracefulShutdown()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        await using var network = new PresenceNetwork("a", "b");
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var ac = new EventCluster("a", network.Node("a"));
        var bc = new EventCluster("b", network.Node("b"));
        await using var ap = a.Publisher(data, metadata, new Handler(), ac);
        await ap.StartAsync();
        Assert.Equal(new[] { "a" }, ap.Cluster.LiveNodeIds);
        Assert.Equal("a", ap.Cluster.SelfNodeId);
        Assert.Equal("a", ap.Cluster.LeaderNodeId);
        await ap.PublishAsync(new AddPure { Amount = 1 });
        var delayed = new DelayedTopic(data);
        var bh = new Handler();
        bh.Consuming = _ =>
        {
            Assert.Equal("b", Ambient.Cluster.SelfNodeId);
            Assert.Equal("a", Ambient.Cluster.LeaderNodeId);
            Assert.DoesNotContain("b", Ambient.Cluster.LiveNodeIds);
        };
        await using var bp = b.Publisher(delayed, metadata, bh, bc);
        var starting = bp.StartAsync();
        await delayed.Waiting.Task.WaitAsync(Wait);
        Assert.False(starting.IsCompleted);
        Assert.Equal(new[] { "a" }, (await ac.RefreshAsync()).LiveNodeIds);
        delayed.Release.SetResult();
        await starting.WaitAsync(Wait);
        // Registration returns after the leader confirmed it.
        Assert.Equal(new[] { "a", "b" }, ac.Current.LiveNodeIds);
        Assert.Equal(new[] { "a", "b" }, bc.Current.LiveNodeIds);
        using (ap.StartQuery()) Assert.Equal("a", Ambient.Cluster.SelfNodeId);
        await bp.DisposeAsync();
        Assert.Equal(new[] { "a" }, (await ac.RefreshAsync()).LiveNodeIds);
    }

    [Fact]
    public async Task ANewLeaderSettlesUntilEveryLiveNodeAnnouncedItselfAgain()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        await using var network = new PresenceNetwork("a", "b");
        var ac = new EventCluster("a", network.Node("a"));
        var bc = new EventCluster("b", network.Node("b"));
        await using var ap = a.Publisher(new MemoryTopic("a"), new MemoryTopic("am"), new Handler(), ac);
        await using var bp = b.Publisher(new MemoryTopic("b"), new MemoryTopic("bm"), new Handler(), bc);
        await Task.WhenAll(ap.StartAsync(), bp.StartAsync());
        await Until(() => !ac.Current.Settling && !bc.Current.Settling);
        Assert.Equal(new[] { "a", "b" }, ap.Cluster.LiveNodeIds);

        network.Lead("b");
        var settling = bp.Cluster;
        Assert.True(settling.Settling);
        Assert.Equal("b", settling.LeaderNodeId);
        await Until(() => ac.Current.LeaderNodeId == "b" && ac.Current.LiveNodeIds.Count == 2);
        await Until(() => !ac.Current.Settling && !bc.Current.Settling);
        Assert.Equal(new[] { "a", "b" }, ap.Cluster.LiveNodeIds);
        Assert.Equal(new[] { "a", "b" }, bp.Cluster.LiveNodeIds);
    }

    [Fact]
    public async Task CrashedConsumersExpireAndAReplacementOnAnotherNodeKeepsItsNodeId()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        using var c = await TestDatabase.OpenAsync();
        await using var network = new PresenceNetwork("leader", "x", "y");
        await using var leader = a.Publisher(new MemoryTopic("l"), new MemoryTopic("lm"), new Handler(),
            new EventCluster("leader", network.Node("leader")));
        var oldCluster = new EventCluster("node", network.Node("x"));
        await using var old = b.Publisher(new MemoryTopic("a"), new MemoryTopic("am"), new Handler(), oldCluster);
        await Task.WhenAll(leader.StartAsync(), old.StartAsync());
        Assert.Equal(new[] { "leader", "node" }, leader.Cluster.LiveNodeIds);
        network.Crash("x");
        await Until(() => leader.Cluster.LiveNodeIds.Count == 1);
        var replacementCluster = new EventCluster("node", network.Node("y"));
        await using var replacement = c.Publisher(new MemoryTopic("b"), new MemoryTopic("bm"), new Handler(), replacementCluster);
        await replacement.StartAsync();
        await old.DisposeAsync(); // its late withdrawal concerns only its own node
        Assert.Equal(new[] { "leader", "node" }, (await replacementCluster.RefreshAsync()).LiveNodeIds);
    }

    [Fact]
    public async Task DuplicateLiveNodeIdsAndSharedPresenceFailWithoutRemovingTheOriginal()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        await using var network = new PresenceNetwork("x", "y");
        var ac = new EventCluster("node", network.Node("x"));
        await using var ap = a.Publisher(new MemoryTopic("a"), new MemoryTopic("am"), new Handler(), ac);
        await ap.StartAsync();
        var duplicate = b.Publisher(new MemoryTopic("b"), new MemoryTopic("bm"), new Handler(),
            new EventCluster("node", network.Node("y")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => duplicate.StartAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => duplicate.DisposeAsync().AsTask());
        var shared = b.Publisher(new MemoryTopic("c"), new MemoryTopic("cm"), new Handler(),
            new EventCluster("other", network.Node("x")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => shared.StartAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => shared.DisposeAsync().AsTask());
        Assert.Equal(new[] { "node" }, (await ac.RefreshAsync()).LiveNodeIds);
        Assert.Throws<InvalidOperationException>(() => b.Publisher(new MemoryTopic("d"), new MemoryTopic("dm"), new Handler(), ac));
    }

    [Fact]
    public async Task FailedConsumersUnregister()
    {
        using var db = await TestDatabase.OpenAsync();
        await using var network = new PresenceNetwork("node");
        var cluster = new EventCluster("node", network.Node("node"));
        var publisher = db.Publisher(new MemoryTopic("data"), new MemoryTopic("metadata"), new Handler { Fail = true }, cluster);
        await publisher.StartAsync();
        Assert.Equal(new[] { "node" }, cluster.Current.LiveNodeIds);
        var publishing = publisher.PublishAsync(new AddPure { Amount = -1 }).AsTask();
        await Assert.ThrowsAsync<IOException>(() => publisher.Completion);
        await Assert.ThrowsAnyAsync<Exception>(() => publishing);
        Assert.Empty((await cluster.RefreshAsync()).LiveNodeIds);
        await Assert.ThrowsAsync<IOException>(() => publisher.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task WithoutAReachableLeaderStartupContinuesWithASettlingView()
    {
        using var db = await TestDatabase.OpenAsync();
        await using var network = new PresenceNetwork("leader");
        var cluster = new EventCluster("node", network.Node("node"),
            new EventClusterOptions { RequestTimeout = TimeSpan.FromMilliseconds(100) });
        network.Crash("node");
        await using var publisher = db.Publisher(new MemoryTopic("data"), new MemoryTopic("metadata"), new Handler(), cluster);
        await publisher.StartAsync().WaitAsync(Wait);
        Assert.True(publisher.Cluster.Settling);
        Assert.Empty(publisher.Cluster.LiveNodeIds);
    }
}
