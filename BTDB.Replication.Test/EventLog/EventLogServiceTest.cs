using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test.EventLog;

/// <summary>A node's view of the shared storage that counts its own requests.</summary>
internal sealed class CountingStorage(IEventLogStorage inner) : IEventLogStorage
{
    int _requests;
    public int Requests => Volatile.Read(ref _requests);
    public bool Unavailable { get; set; }

    void Count()
    {
        Interlocked.Increment(ref _requests);
        if (Unavailable) throw new InvalidOperationException("Storage is unreachable from this node.");
    }

    public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken c) { Count(); return inner.ReadAsync(key, c); }
    public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken c) { Count(); return inner.GetPropertiesAsync(key, c); }
    public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination, CancellationToken c)
    { Count(); return inner.ReadRangeAsync(key, version, offset, destination, c); }
    public ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
        EventLogOwner? owner, string? sha256, CancellationToken c)
    { Count(); return inner.WriteAsync(key, expectedVersion, content, owner, sha256, c); }
    public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner, CancellationToken c)
    { Count(); return inner.SetOwnerAsync(key, expectedVersion, owner, c); }
    public IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken c) { Count(); return inner.ListAsync(prefix, c); }
    public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay, CancellationToken c)
    { Count(); return inner.ScheduleDeletionAsync(key, version, delay, c); }
    public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken c) { Count(); return inner.DeleteAsync(key, version, c); }
}

internal sealed class EventLogCluster : IAsyncDisposable
{
    public readonly InMemoryEventLogStorage Storage = new();
    public readonly InProcessEventLogTransport Transport = new();
    public readonly Dictionary<string, (EventLogService Service, CountingStorage Storage)> Nodes = new();
    public readonly SystemReplicationScheduler Scheduler = new();

    public static EventLogOptions FastOptions => new()
    {
        SplitCap = 4096, SealFreeSpace = 512, HeartbeatInterval = TimeSpan.FromMilliseconds(40),
        OwnerTimeout = TimeSpan.FromMilliseconds(250), ContentionBackoff = TimeSpan.FromMilliseconds(15),
        AmbiguousRetryDelay = TimeSpan.FromMilliseconds(5)
    };

    public EventLogService Start(string name, EventLogOptions? options = null)
    {
        var storage = new CountingStorage(Storage);
        var service = new EventLogService(storage, Transport.From(name), name, Scheduler, options ?? FastOptions);
        Transport.Register(name, service);
        Nodes[name] = (service, storage);
        return service;
    }

    public EventLogService this[string name] => Nodes[name].Service;

    public async Task Stop(string name)
    {
        Transport.Partition(name);
        await Nodes[name].Service.DisposeAsync();
        Nodes.Remove(name);
        Transport.Heal(name);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var (service, _) in Nodes.Values) await service.DisposeAsync();
    }
}

public class EventLogServiceTest
{
    const string Topic = "events";
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    static async Task<List<EventLogRecord>> Collect(IEventTopic topic, ulong start, ulong end)
    {
        var result = new List<EventLogRecord>();
        await foreach (var record in topic.ReadAsync(start, end).WithCancellation(new CancellationTokenSource(Timeout).Token))
            result.Add(new(record.Offset, record.Payload.ToArray()));
        return result;
    }

    static async Task<ulong> Publish(EventLogService node, int value) =>
        await node.GetTopic(Topic).PublishAsync(Bytes(value)).AsTask().WaitAsync(Timeout);

    [Fact]
    public async Task PublicationsFromAllNodesGetUniqueContiguousOffsets()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        var c = cluster.Start("c");
        Assert.Equal(0ul, await Publish(a, 0));
        Assert.True(a.IsOwner(Topic));
        var tasks = new List<Task<ulong>>();
        for (var i = 1; i < 60; i++) tasks.Add(Publish(new[] { a, b, c }[i % 3], i));
        var offsets = await Task.WhenAll(tasks);
        Assert.Equal(Enumerable.Range(1, 59).Select(i => (ulong)i), offsets.OrderBy(o => o));
        Assert.False(b.IsOwner(Topic) || c.IsOwner(Topic));
        var records = await Collect(c.GetTopic(Topic), 0, 60);
        Assert.Equal(Enumerable.Range(0, 60).Select(i => (ulong)i), records.Select(r => r.Offset));
        for (var i = 0; i < 60; i++) Assert.Equal(Bytes((int)offsets.Prepend(0ul).ToList().IndexOf((ulong)i)), records[i].Payload.ToArray());
    }

    [Fact]
    public async Task PipelinedPublicationsOfOneNodeCommitInCallOrder()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        await Publish(a, -1);
        var topic = b.GetTopic(Topic);
        var tasks = Enumerable.Range(0, 200).Select(i => topic.PublishAsync(Bytes(i)).AsTask()).ToArray();
        var offsets = await Task.WhenAll(tasks).WaitAsync(Timeout);
        Assert.Equal(offsets.OrderBy(o => o), offsets);
    }

    [Fact]
    public async Task FollowerReceivesLiveRecordsWithoutReadingStorage()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        await Publish(a, 0);
        var topic = b.GetTopic(Topic);
        var bounds = await topic.GetBoundsAsync();
        Assert.Equal(new EventLogBounds(0, 1), bounds);
        using var cancel = new CancellationTokenSource(Timeout);
        var received = new List<ulong>();
        var reading = Task.Run(async () =>
        {
            await foreach (var record in topic.ReadAsync(1, 41, cancel.Token)) received.Add(record.Offset);
        });
        await Task.Delay(100);
        var before = cluster.Nodes["b"].Storage.Requests;
        for (var i = 1; i <= 40; i++) await Publish(i % 2 == 0 ? a : b, i);
        await Task.Delay(200); // idle heartbeats
        await reading.WaitAsync(Timeout);
        Assert.Equal(Enumerable.Range(1, 40).Select(i => (ulong)i), received);
        Assert.Equal(before, cluster.Nodes["b"].Storage.Requests);
    }

    [Fact]
    public async Task UnreachableOwnerIsTakenOverAndEveryRecordCommitsOnce()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        for (var i = 0; i < 5; i++) await Publish(a, i);
        cluster.Transport.Partition("a");
        var offsets = new List<ulong>();
        for (var i = 5; i < 10; i++) offsets.Add(await Publish(b, i));
        Assert.Equal([5ul, 6, 7, 8, 9], offsets);
        Assert.True(b.IsOwner(Topic));
        cluster.Transport.Heal("a");
        // The deposed owner is fenced on its next write and forwards to the new owner.
        Assert.Equal(10ul, await Publish(a, 10));
        Assert.False(a.IsOwner(Topic));
        var records = await new EventLogStorageReader(cluster.Storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, 11).Select(i => Bytes(i)), records.Select(r => r.Payload.ToArray()));
    }

    [Fact]
    public async Task IdleDeposedOwnerAndItsSubscribersDiscoverTheTakeover()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        var c = cluster.Start("c");
        await Publish(a, 0);
        using var cancel = new CancellationTokenSource(Timeout);
        var received = new List<ulong>();
        var reading = Task.Run(async () =>
        {
            await foreach (var record in c.GetTopic(Topic).ReadAsync(1, 3, cancel.Token)) received.Add(record.Offset);
        });
        await Task.Delay(100);
        // B takes over while A stays alive and idle: only A's heartbeat validation can notice.
        var lane = await EventLogOwnerLane.CreateCandidateAsync(cluster.Storage, new(cluster.Storage, Topic), Topic,
            EventLogCluster.FastOptions, cluster.Scheduler, new("manual", "b"), CancellationToken.None);
        await lane.InstallAsync();
        await Task.Delay(300);
        Assert.False(a.IsOwner(Topic));
        lane.Stop();
        Assert.Equal(1ul, await Publish(b, 1));
        Assert.Equal(2ul, await Publish(a, 2));
        await reading.WaitAsync(Timeout);
        Assert.Equal([1ul, 2], received);
    }

    [Fact]
    public async Task BoundsFromAFollowerCoverEveryCompletedReceipt()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        Assert.Equal(new EventLogBounds(0, 0), await b.GetTopic(Topic).GetBoundsAsync());
        for (var i = 0; i < 7; i++)
        {
            var offset = await Publish(i % 2 == 0 ? a : b, i);
            Assert.Equal(offset + 1, (await b.GetTopic(Topic).GetBoundsAsync()).Next);
        }
        cluster.Transport.Partition("a");
        Assert.Equal(7ul, (await b.GetTopic(Topic).GetBoundsAsync()).Next); // storage fallback
    }

    [Fact]
    public async Task RestartedNodeTakesOverItsPreviousSession()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        await Publish(a, 0);
        await cluster.Stop("a");
        var restarted = cluster.Start("a");
        Assert.Equal(1ul, await Publish(restarted, 1));
        Assert.True(restarted.IsOwner(Topic));
    }

    [Fact]
    public async Task ReadStartingBeyondTheDurableEndFails()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        await Publish(a, 0);
        await Assert.ThrowsAsync<EventLogOffsetNotAvailableException>(async () =>
        {
            await foreach (var _ in a.GetTopic(Topic).ReadAsync(5)) { }
        });
    }

    [Fact]
    public async Task ReplayFromStorageThenLiveIsGapFree()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a", EventLogCluster.FastOptions with { RecentCacheBytes = 200 });
        var b = cluster.Start("b");
        for (var i = 0; i < 100; i++) await Publish(a, i);
        using var cancel = new CancellationTokenSource(Timeout);
        var received = new List<ulong>();
        var reading = Task.Run(async () =>
        {
            await foreach (var record in b.GetTopic(Topic).ReadAsync(0, 150, cancel.Token)) received.Add(record.Offset);
        });
        for (var i = 100; i < 150; i++) await Publish(a, i);
        await reading.WaitAsync(Timeout);
        Assert.Equal(Enumerable.Range(0, 150).Select(i => (ulong)i), received);
    }
}
