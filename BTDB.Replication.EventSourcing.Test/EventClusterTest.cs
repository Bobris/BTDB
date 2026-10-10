using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class EventClusterTest
{
    internal sealed class Leaders(string clusterId) : IReplicationLeaderStorage
    {
        internal string? NodeId;
        public ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            return ValueTask.FromResult(new LeaderRecord("version",
                System.Text.Json.JsonSerializer.Serialize(new { format = 1, clusterId, nodeId = NodeId })));
        }
        public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation) => throw new NotSupportedException();
        public ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation) => throw new NotSupportedException();
        public ValueTask TransferAsync(string currentHandle, string proposedHandle, CancellationToken cancellation) => throw new NotSupportedException();
        public ValueTask<LeaderWriteOutcome> WriteAsync(string leaseHandle, string expectedToken, string json,
            CancellationToken cancellation) => throw new NotSupportedException();
    }

    sealed class Clock : TimeProvider
    {
        readonly object _lock = new();
        readonly List<Timer> _timers = new();
        DateTimeOffset _now = new(2026, 10, 4, 12, 0, 0, TimeSpan.Zero);
        internal DateTimeOffset Now { get { lock (_lock) return _now; } set { lock (_lock) _now = value; } }
        internal int PendingHeartbeats { get { lock (_lock) return _timers.Count(t => t.Due == _now + TimeSpan.FromHours(1)); } }
        public override DateTimeOffset GetUtcNow() => Now;
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new Timer(this, callback, state);
            lock (_lock) _timers.Add(timer);
            timer.Change(dueTime, period);
            return timer;
        }
        internal void Advance(TimeSpan duration)
        {
            Timer[] due;
            lock (_lock)
            {
                Now += duration;
                due = _timers.Where(t => t.Due <= Now).ToArray();
                foreach (var timer in due) timer.Due = DateTimeOffset.MaxValue;
            }
            foreach (var timer in due) timer.Fire();
        }
        sealed class Timer(Clock clock, TimerCallback callback, object? state) : ITimer
        {
            internal DateTimeOffset Due = DateTimeOffset.MaxValue;
            bool _disposed;
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                if (period != Timeout.InfiniteTimeSpan) throw new NotSupportedException("Tests use one-shot timers.");
                lock (clock._lock)
                {
                    if (_disposed) return false;
                    Due = dueTime == Timeout.InfiniteTimeSpan ? DateTimeOffset.MaxValue : clock.Now + dueTime;
                    return true;
                }
            }
            internal void Fire() => callback(state);
            public void Dispose()
            {
                lock (clock._lock) { _disposed = true; clock._timers.Remove(this); }
            }
            public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
        }
    }

    static EventClusterOptions Options(Clock clock) => new()
    {
        TimeProvider = clock, HeartbeatInterval = TimeSpan.FromHours(1), NodeTimeout = TimeSpan.FromHours(2)
    };

    sealed class Storage(IEventLogStorage inner) : IEventLogStorage
    {
        internal bool LoseWriteResponses;
        internal Func<ReadOnlyMemory<byte>, Task>? BeforeWrite;
        public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation) => inner.ReadAsync(key, cancellation);
        public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation) =>
            inner.GetPropertiesAsync(key, cancellation);
        public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination, CancellationToken cancellation) =>
            inner.ReadRangeAsync(key, version, offset, destination, cancellation);
        public async ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
            EventLogOwner? owner, string? sha256, CancellationToken cancellation)
        {
            if (BeforeWrite != null) await BeforeWrite(content).WaitAsync(cancellation);
            var result = await inner.WriteAsync(key, expectedVersion, content, owner, sha256, cancellation);
            return LoseWriteResponses && result.Outcome == EventLogWriteOutcome.Applied ? EventLogWriteResult.Ambiguous : result;
        }
        public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner, CancellationToken cancellation) =>
            inner.SetOwnerAsync(key, expectedVersion, owner, cancellation);
        public IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken cancellation) => inner.ListAsync(prefix, cancellation);
        public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay, CancellationToken cancellation) =>
            inner.ScheduleDeletionAsync(key, version, delay, cancellation);
        public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation) => inner.DeleteAsync(key, version, cancellation);
    }

    [Fact]
    public async Task DiscoveryIncludesOnlyCaughtUpConsumersAndTracksLeaderAndGracefulShutdown()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var storage = new InMemoryEventLogStorage();
        var leaders = new Leaders("cluster") { NodeId = "a" };
        var ac = new EventCluster("a", "cluster", storage, leaders);
        var bc = new EventCluster("b", "cluster", storage, leaders);
        await using var ap = a.Publisher(data, metadata, new Handler(), ac);
        await ap.StartAsync();
        Assert.Equal(new[] { "a" }, ap.Cluster.LiveNodeIds);
        Assert.Equal("a", ap.Cluster.SelfNodeId);
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
        await delayed.Waiting.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(starting.IsCompleted);
        Assert.Equal(new[] { "a" }, (await ac.RefreshAsync()).LiveNodeIds);
        delayed.Release.SetResult();
        await starting.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "a", "b" }, (await ac.RefreshAsync()).LiveNodeIds);
        using (ap.StartQuery()) Assert.Equal("a", Ambient.Cluster.SelfNodeId);
        leaders.NodeId = "b";
        Assert.Equal("b", (await ac.RefreshAsync()).LeaderNodeId);
        await bp.DisposeAsync();
        Assert.Equal(new[] { "a" }, (await ac.RefreshAsync()).LiveNodeIds);
        Assert.Equal("b", ac.Current.LeaderNodeId); // Selected leader is not inferred from membership.
    }

    [Fact]
    public async Task ExpiredSessionsDisappearAndOldShutdownCannotUnregisterTheirReplacement()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        var clock = new Clock();
        var storage = new InMemoryEventLogStorage(clock);
        var leaders = new Leaders("cluster");
        var oldCluster = new EventCluster("node", "cluster", storage, leaders, Options(clock));
        await using var old = a.Publisher(new MemoryTopic("a"), new MemoryTopic("am"), new Handler(), oldCluster);
        await old.StartAsync();
        Assert.Null(old.Cluster.LeaderNodeId);
        Assert.Equal(new[] { "node" }, old.Cluster.LiveNodeIds);
        clock.Now += TimeSpan.FromHours(3);
        Assert.Empty(old.Cluster.LiveNodeIds); // No refresh needed to expire a cached node.
        var replacementCluster = new EventCluster("node", "cluster", storage, leaders, Options(clock));
        await using var replacement = b.Publisher(new MemoryTopic("b"), new MemoryTopic("bm"), new Handler(), replacementCluster);
        await replacement.StartAsync();
        await old.DisposeAsync();
        Assert.Equal(new[] { "node" }, (await replacementCluster.RefreshAsync()).LiveNodeIds);
    }

    [Fact]
    public async Task DuplicateLiveNodeIdsFailWithoutRemovingTheOriginalAndClustersStayIsolated()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        var storage = new InMemoryEventLogStorage();
        var leaders = new Leaders("one");
        var ac = new EventCluster("node", "one", storage, leaders);
        await using var ap = a.Publisher(new MemoryTopic("a"), new MemoryTopic("am"), new Handler(), ac);
        await ap.StartAsync();
        var duplicate = b.Publisher(new MemoryTopic("b"), new MemoryTopic("bm"), new Handler(),
            new EventCluster("node", "one", storage, leaders));
        await Assert.ThrowsAsync<InvalidOperationException>(() => duplicate.StartAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => duplicate.DisposeAsync().AsTask());
        Assert.Equal(new[] { "node" }, (await ac.RefreshAsync()).LiveNodeIds);
        var other = new EventCluster("other", "two", storage, new Leaders("two"));
        Assert.Empty((await other.RefreshAsync()).LiveNodeIds);
        Assert.Throws<InvalidOperationException>(() => b.Publisher(new MemoryTopic("c"), new MemoryTopic("cm"), new Handler(), ac));
    }

    [Fact]
    public async Task FailedConsumersUnregisterAndReplicationLeaderMustBelongToTheSameCluster()
    {
        using var db = await TestDatabase.OpenAsync();
        var storage = new InMemoryEventLogStorage();
        var leaders = new Leaders("cluster") { NodeId = "node" };
        var cluster = new EventCluster("node", "cluster", storage, leaders);
        var publisher = db.Publisher(new MemoryTopic("data"), new MemoryTopic("metadata"), new Handler { Fail = true }, cluster);
        await publisher.StartAsync();
        var publishing = publisher.PublishAsync(new AddPure { Amount = -1 }).AsTask();
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.Completion);
        await Assert.ThrowsAnyAsync<Exception>(() => publishing);
        Assert.Empty((await cluster.RefreshAsync()).LiveNodeIds);
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.DisposeAsync().AsTask());
        var wrong = new EventCluster("node", "wrong", storage, leaders);
        await Assert.ThrowsAsync<InvalidDataException>(() => wrong.RefreshAsync().AsTask());
    }

    [Fact]
    public async Task HeartbeatsAutomaticallyRefreshMembershipAndLeaderAndExtendLiveSessions()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        var clock = new Clock();
        var storage = new InMemoryEventLogStorage(clock);
        var leaders = new Leaders("cluster") { NodeId = "a" };
        var options = Options(clock);
        var ac = new EventCluster("a", "cluster", storage, leaders, options);
        var bc = new EventCluster("b", "cluster", storage, leaders, options);
        await using var ap = a.Publisher(new MemoryTopic("a"), new MemoryTopic("am"), new Handler(), ac);
        await using var bp = b.Publisher(new MemoryTopic("b"), new MemoryTopic("bm"), new Handler(), bc);
        await Task.WhenAll(ap.StartAsync(), bp.StartAsync());
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        for (var i = 0; i < 4; i++)
        {
            leaders.NodeId = "b";
            while (clock.PendingHeartbeats != 2) await Task.Delay(1, timeout.Token);
            clock.Advance(TimeSpan.FromHours(1));
            while (ac.Current.SampledAt < clock.Now || bc.Current.SampledAt < clock.Now)
                await Task.Delay(1, timeout.Token);
            Assert.Equal(new[] { "a", "b" }, ap.Cluster.LiveNodeIds);
            Assert.Equal("b", ap.Cluster.LeaderNodeId);
        }
        await bp.DisposeAsync();
        while (clock.PendingHeartbeats != 1) await Task.Delay(1, timeout.Token);
        clock.Advance(TimeSpan.FromHours(1));
        while (ac.Current.SampledAt < clock.Now) await Task.Delay(1, timeout.Token);
        Assert.Equal(new[] { "a" }, ap.Cluster.LiveNodeIds);
    }

    [Fact]
    public async Task ADelayedOldUnregisterCannotOverwriteAReplacementAndAmbiguousHeartbeatsAreReconciled()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        var clock = new Clock();
        var storage = new Storage(new InMemoryEventLogStorage(clock)) { LoseWriteResponses = true };
        var leaders = new Leaders("cluster");
        var ac = new EventCluster("node", "cluster", storage, leaders, Options(clock));
        await using var ap = a.Publisher(new MemoryTopic("a"), new MemoryTopic("am"), new Handler(), ac);
        await ap.StartAsync();
        Assert.Equal(new[] { "node" }, ap.Cluster.LiveNodeIds);
        clock.Now += TimeSpan.FromHours(3); // Simulate an expired session without running its heartbeat timers.
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        storage.BeforeWrite = bytes =>
        {
            using var json = System.Text.Json.JsonDocument.Parse(bytes);
            if (json.RootElement.GetProperty("ExpiresAt").GetDateTimeOffset() != DateTimeOffset.MinValue)
                return Task.CompletedTask;
            stopping.TrySetResult();
            return release.Task;
        };
        var stop = ap.DisposeAsync().AsTask();
        await stopping.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var bc = new EventCluster("node", "cluster", storage, leaders, Options(clock));
        await using var bp = b.Publisher(new MemoryTopic("b"), new MemoryTopic("bm"), new Handler(), bc);
        try
        {
            await bp.StartAsync().WaitAsync(TimeSpan.FromSeconds(10));
            release.SetResult();
            await stop.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(new[] { "node" }, (await bc.RefreshAsync()).LiveNodeIds);
        }
        finally { release.TrySetResult(); }
        await bp.DisposeAsync();
        Assert.Empty((await bc.RefreshAsync()).LiveNodeIds);
    }
}
