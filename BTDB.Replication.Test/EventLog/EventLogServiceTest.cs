using System;
using System.Collections.Concurrent;
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
    int _lists;
    public int Requests => Volatile.Read(ref _requests);
    public int Lists => Volatile.Read(ref _lists);
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
    public IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken c)
    { Count(); Interlocked.Increment(ref _lists); return inner.ListAsync(prefix, c); }
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

    /// <summary>Owner-side view of submissions: counts them and can lose a request or its response.</summary>
    sealed class SubmitInterceptor(IEventLogPeerHandler inner) : IEventLogPeerHandler
    {
        int _submits;
        public int Submits => Volatile.Read(ref _submits);
        public Func<int, SubmitLoss>? Loss;
        public readonly ConcurrentQueue<EventLogSubmitRequest> Requests = new();

        public async ValueTask<EventLogSubmitResponse> SubmitAsync(EventLogSubmitRequest request,
            CancellationToken cancellation)
        {
            Requests.Enqueue(request);
            var index = Interlocked.Increment(ref _submits);
            var loss = Loss?.Invoke(index) ?? SubmitLoss.None;
            if (loss == SubmitLoss.Request) throw new PeerUnavailableException("lost request");
            var response = await inner.SubmitAsync(request, cancellation);
            if (loss == SubmitLoss.Response) throw new PeerUnavailableException("lost response");
            return response;
        }

        public ValueTask<EventLogBoundsResponse> GetBoundsAsync(string topic, CancellationToken cancellation) =>
            inner.GetBoundsAsync(topic, cancellation);

        public IAsyncEnumerable<EventLogLiveMessage> SubscribeAsync(string topic, ulong from,
            CancellationToken cancellation) => inner.SubscribeAsync(topic, from, cancellation);
    }

    public enum SubmitLoss { None, Request, Response }

    static (EventLogService Owner, EventLogService Follower, SubmitInterceptor Interceptor) StartPair(
        IEventLogStorage storage, EventLogOptions options)
    {
        var transport = new InProcessEventLogTransport();
        var scheduler = new SystemReplicationScheduler();
        var owner = new EventLogService(storage, transport.From("a"), "a", scheduler, options);
        var interceptor = new SubmitInterceptor(owner);
        transport.Register("a", interceptor);
        var follower = new EventLogService(storage, transport.From("b"), "b", scheduler, options);
        transport.Register("b", follower);
        return (owner, follower, interceptor);
    }

    [Fact]
    public async Task FollowerDispatchesRecordsWhileTheOwnerWriteIsInFlight()
    {
        var storage = new FaultyEventLogStorage();
        var (a, b, interceptor) = StartPair(storage, EventLogCluster.FastOptions);
        await using var _ = a;
        await using var __ = b;
        await Publish(a, -1);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        storage.BeforeMutation = operation =>
        {
            if (operation.Kind != "write") return default;
            writing.TrySetResult();
            return new(gate.Task);
        };
        var first = Publish(b, 1);
        await writing.Task.WaitAsync(Timeout);
        var second = Publish(b, 2);
        // The second record reaches the owner while the first one's storage write is still blocked.
        var deadline = DateTime.UtcNow + Timeout;
        while (interceptor.Submits < 2 && DateTime.UtcNow < deadline) await Task.Delay(5);
        Assert.Equal(2, interceptor.Submits);
        Assert.False(first.IsCompleted);
        gate.SetResult();
        var offsets = await Task.WhenAll(first, second);
        Assert.Equal(new[] { 1ul, 2ul }, offsets);
    }

    [Fact]
    public async Task RemoteRoundsCarryAtMostMaxRecordSizeBytes()
    {
        var storage = new FaultyEventLogStorage();
        var options = EventLogCluster.FastOptions with { MaxRecordSize = 100, MaxPublishRoundsInFlight = 1 };
        var (a, b, interceptor) = StartPair(storage, options);
        await using var _ = a;
        await using var __ = b;
        await Publish(a, -1);
        // Hold the owner's writes so the follower's records queue up behind its single in-flight round.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        storage.BeforeMutation = operation => operation.Kind == "write" ? new(gate.Task) : default;
        var topic = b.GetTopic(Topic);
        var publishing = Enumerable.Range(0, 40).Select(i => topic.PublishAsync(new byte[40]).AsTask()).ToArray();
        var deadline = DateTime.UtcNow + Timeout;
        while (interceptor.Submits < 1 && DateTime.UtcNow < deadline) await Task.Delay(5);
        gate.SetResult();
        var offsets = await Task.WhenAll(publishing).WaitAsync(Timeout);
        Assert.Equal(Enumerable.Range(1, 40).Select(i => (ulong)i), offsets);
        var rounds = interceptor.Requests.ToArray();
        Assert.All(rounds, r => Assert.True(r.Records.Sum(record => record.Length) <= 100));
        Assert.Contains(rounds, r => r.Records.Count == 2); // records still share rounds up to the limit
    }

    [Theory]
    [InlineData(SubmitLoss.Request)]
    [InlineData(SubmitLoss.Response)]
    public async Task PipelinedRoundsSurviveALostSubmissionExactlyOnceInCallOrder(SubmitLoss loss)
    {
        var storage = new InMemoryEventLogStorage();
        var (a, b, interceptor) = StartPair(storage, EventLogCluster.FastOptions);
        await using var _ = a;
        await using var __ = b;
        await Publish(a, -1);
        interceptor.Loss = index => index == 2 ? loss : SubmitLoss.None;
        var topic = b.GetTopic(Topic);
        var tasks = new List<Task<ulong>>();
        for (var i = 0; i < 300; i++)
        {
            tasks.Add(topic.PublishAsync(Bytes(i)).AsTask());
            if (i % 30 == 0) await Task.Delay(1); // spread the calls over several pipelined rounds
        }
        var offsets = await Task.WhenAll(tasks).WaitAsync(Timeout);
        Assert.True(interceptor.Submits > 2);
        Assert.Equal(offsets.OrderBy(o => o), offsets);
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(-1, 301).Select(i => Bytes(i)), records.Select(r => r.Payload.ToArray()));
        Assert.Equal(Enumerable.Range(0, 300).Select(i => (ulong)i + 1), offsets);
    }

    [Fact]
    public async Task SubscriberFallingBehindByBytesIsDisconnected()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a", EventLogCluster.FastOptions with { SubscriberBufferBytes = 1000 });
        await Publish(a, 0);
        await using var live = ((IEventLogPeerHandler)a).SubscribeAsync(Topic, 1, CancellationToken.None)
            .GetAsyncEnumerator();
        var first = live.MoveNextAsync().AsTask(); // registers the subscriber and takes the first message
        var topic = a.GetTopic(Topic);
        // A message above the limit still fits an empty queue.
        await topic.PublishAsync(new byte[5000]).AsTask().WaitAsync(Timeout);
        Assert.True(await first.WaitAsync(Timeout));
        var offsets = new List<ulong>(live.Current.Kind == EventLogLiveKind.Batch ? [live.Current.Offset] : []);
        // Three queued 300-byte records fit 1000 bytes; the fourth disconnects the subscriber.
        for (var i = 0; i < 4; i++) await topic.PublishAsync(new byte[300]).AsTask().WaitAsync(Timeout);
        while (await live.MoveNextAsync().AsTask().WaitAsync(Timeout))
            if (live.Current.Kind == EventLogLiveKind.Batch) offsets.Add(live.Current.Offset);
        Assert.Equal([1ul, 2, 3, 4], offsets);
    }

    [Fact]
    public async Task FollowerReceivesLiveRecordsWithoutReadingStorage()
    {
        await using var cluster = new EventLogCluster();
        // No owner change is under test: a scheduling stall longer than the fast 250 ms timeout would end the live
        // subscription and correctly make the follower catch up from storage.
        var options = EventLogCluster.FastOptions with { OwnerTimeout = TimeSpan.FromSeconds(10) };
        var a = cluster.Start("a", options);
        var b = cluster.Start("b", options);
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

    public enum IdleOwner { Live, RestartedAndFollowing, RestartedOnlyServing, Vanished }

    /// <summary>An idle topic costs one validation per heartbeat on its owner and nothing on followers, also after its
    /// owner restarted at the same endpoint or vanished: a follower or the restarted node takes the topic over instead
    /// of every follower polling storage.</summary>
    [Theory]
    [InlineData(IdleOwner.Live)]
    [InlineData(IdleOwner.RestartedAndFollowing)]
    [InlineData(IdleOwner.RestartedOnlyServing)]
    [InlineData(IdleOwner.Vanished)]
    public async Task IdleTopicCostsOneStorageRequestPerHeartbeat(IdleOwner scenario)
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        await Publish(a, 0);
        using var cancel = new CancellationTokenSource(Timeout);
        Task Follow(EventLogService node) => Task.Run(async () =>
        {
            try { await foreach (var _ in node.GetTopic(Topic).ReadAsync(0, null, cancel.Token)) { } }
            catch (OperationCanceledException) { }
        });
        var following = new List<Task> { Follow(b) };
        await Task.Delay(200);
        if (scenario != IdleOwner.Live) await cluster.Stop("a");
        if (scenario is IdleOwner.RestartedAndFollowing or IdleOwner.RestartedOnlyServing) a = cluster.Start("a");
        if (scenario == IdleOwner.RestartedAndFollowing) following.Add(Follow(a));
        await Task.Delay(1000); // ownership settles
        var before = cluster.Nodes.Values.Sum(n => n.Storage.Requests);
        var window = TimeSpan.FromSeconds(1);
        await Task.Delay(window);
        var requests = cluster.Nodes.Values.Sum(n => n.Storage.Requests) - before;
        var heartbeats = window / EventLogCluster.FastOptions.HeartbeatInterval;
        Assert.True(requests <= heartbeats * 1.5 + 5, $"{requests} storage requests in {heartbeats} heartbeats");
        Assert.Single(cluster.Nodes.Values, n => n.Service.IsOwner(Topic));
        Assert.Equal(1ul, await Publish(b, 1));
        await cancel.CancelAsync();
        await Task.WhenAll(following);
    }

    [Fact]
    public async Task OwnerTimeoutMustExceedHeartbeatInterval()
    {
        await using var cluster = new EventLogCluster();
        var node = cluster.Start("a", EventLogCluster.FastOptions with { OwnerTimeout = TimeSpan.FromMilliseconds(40) });
        Assert.Throws<ArgumentOutOfRangeException>(() => node.GetTopic(Topic));
    }

    [Fact]
    public async Task RemotePublicationReachesAnIdleRestartedOwner()
    {
        await using var cluster = new EventLogCluster();
        var a = cluster.Start("a");
        var b = cluster.Start("b");
        await Publish(a, 0);
        await cluster.Stop("a");
        cluster.Start("a");
        Assert.Equal(1ul, await Publish(b, 1));
        Assert.True(cluster["a"].IsOwner(Topic));
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
