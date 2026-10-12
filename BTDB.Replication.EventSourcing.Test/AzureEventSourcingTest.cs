using BTDB.Replication.Azure;
using BTDB.Replication.Azure.Test;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class AzureEventSourcingTest(BlobStorageFixture blobs) : IClassFixture<BlobStorageFixture>
{
    sealed class NoPeers : IEventLogPeerTransport
    {
        public ValueTask<EventLogSubmitResponse> SubmitAsync(string endpoint, EventLogSubmitRequest request,
            CancellationToken cancellation) => throw new InvalidOperationException("The log owns its topics locally.");
        public ValueTask<EventLogBoundsResponse> GetBoundsAsync(string endpoint, string topic, CancellationToken cancellation) =>
            throw new InvalidOperationException("The log owns its topics locally.");
        public IAsyncEnumerable<EventLogLiveMessage> SubscribeAsync(string endpoint, string topic, ulong from,
            CancellationToken cancellation) => throw new InvalidOperationException("The log owns its topics locally.");
    }

    [Fact]
    public async Task RealEventLogStoresSeparateTopicsAndColdObjectDbReplaysThem()
    {
        var container = await blobs.ContainerAsync();
        await using var log = new EventLogService(new AzureEventLogStorage(container, "application"), new NoPeers(),
            "http://127.0.0.1:5100", new SystemReplicationScheduler(), new() { MergeFanOut = 0 });
        var data = log.GetTopic("events");
        var metadata = log.GetTopic("metadata");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var network = new PresenceNetwork("a", "b");
        var ac = new EventCluster("a", network.Node("a"));
        var bc = new EventCluster("b", network.Node("b"));
        using (var a = await TestDatabase.OpenAsync())
        using (var b = await TestDatabase.OpenAsync())
        {
            var ah = new Handler();
            var bh = new Handler();
            var actions = 0;
            ah.Consuming = _ => Ambient.RegisterPostCommit(() => Interlocked.Increment(ref actions));
            bh.Consuming = _ => Ambient.RegisterPostCommit(() => Interlocked.Increment(ref actions));
            await using var ap = a.Publisher(data, metadata, ah, ac);
            await using var bp = b.Publisher(data, metadata, bh, bc);
            await Task.WhenAll(ap.StartAsync(timeout.Token), bp.StartAsync(timeout.Token));
            Assert.Equal(new[] { "a", "b" }, (await ac.RefreshAsync(timeout.Token)).LiveNodeIds);
            Assert.Equal("a", ap.Cluster.LeaderNodeId);
            var first = ap.PublishAndWaitAsync(new Add { Amount = 8 }, timeout.Token);
            var second = bp.PublishAsync(new AddPure { Amount = 9 }, timeout.Token).AsTask();
            await Task.WhenAll(first, second);
            while (ap.LastAppliedEventId != 2 || bp.LastAppliedEventId != 2) await Task.Delay(1, timeout.Token);
            while (Volatile.Read(ref actions) != 4) await Task.Delay(1, timeout.Token);
            Assert.Equal(a.Read().Select(e => (e.Id, e.Amount, e.When, e.AllocatedId)),
                b.Read().Select(e => (e.Id, e.Amount, e.When, e.AllocatedId)));
        }
        Assert.Empty((await ac.RefreshAsync(timeout.Token)).LiveNodeIds);
        Assert.Equal(2ul, (await data.GetBoundsAsync(timeout.Token)).Next);
        Assert.True((await metadata.GetBoundsAsync(timeout.Token)).Next > 0);
        using var cold = await TestDatabase.OpenAsync();
        var handler = new Handler();
        await using var replay = cold.Publisher(data, metadata, handler);
        await replay.StartAsync(timeout.Token);
        Assert.Equal(2, cold.Read().Length);
        Assert.All(handler.Seen, e => Assert.True(e.Replay));
    }
}
