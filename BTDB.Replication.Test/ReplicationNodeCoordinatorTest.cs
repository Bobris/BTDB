using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using Xunit;
using NativeNode = BTDB.Replication.Test.TrlPrefixComparerTest.Node;
using TrlStorage = BTDB.Replication.Test.CanonicalTrlPublisherTest.Storage;
using Fault = BTDB.Replication.Test.CanonicalTrlPublisherTest.Fault;

namespace BTDB.Replication.Test;

public class ReplicationNodeCoordinatorTest
{
    sealed class Cluster : IAsyncDisposable
    {
        public readonly DeterministicScheduler Clock = new(801);
        public readonly TrlStorage Trls = new();
        public readonly InProcessReplicationPeerTransport Transport = new();
        public readonly HashSet<string> Isolated = new(StringComparer.Ordinal);
        public readonly List<Host> Nodes = new();
        public LeaderRecord Record = new("1", """
            {"format":1,"clusterId":"cluster","term":1,"revision":1,"applicationGeneration":1,
             "databaseNames":["main"],"sessionId":"seed","peerEndpoint":"seed","apiKey":"seed"}
            """);
        string? _lease;
        long _expiry;
        int _leaseNumber;
        public static string Genesis(uint id) => $"genesis/{id}";

        public static async Task<Cluster> Create()
        {
            var cluster = new Cluster();
            using var seed = await NativeNode.Create(false);
            await seed.Write(1, 1);
            var authority = new LeaseAuthority(cluster.Clock.CreateScope("seed"), 0, TimeSpan.Zero);
            authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromSeconds(1));
            using var publisher = new CanonicalTrlPublisher(seed.Db, seed.Capture, cluster.Trls, authority, 1, Genesis);
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
            return cluster;
        }

        public Host Start(string name, bool unavailable = false)
        {
            var host = new Host(this, name);
            host.Storage.Unavailable = unavailable;
            Nodes.Add(host);
            host.Start();
            return host;
        }

        // The injected scheduler owns callbacks, not xUnit's synchronization context. All test providers finish
        // synchronously except explicitly held responses, keeping each virtual-time pump deterministic.
        public void Advance(long ticks)
        {
            var previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);
            try { Clock.AdvanceBy(TimeSpan.FromTicks(ticks)); }
            finally { SynchronizationContext.SetSynchronizationContext(previous); }
        }

        public async Task<ulong> RestoreEvent()
        {
            using var files = new InMemoryReplicationFileStorage();
            var inventory = await CanonicalTrlInventory.DiscoverAsync(Trls, new(Genesis(1), 1));
            await using var collection = new ReplicationFileSet(files, inventory);
            await collection.InitializeAsync();
            using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = collection, Compression = new NoCompressionStrategy(), CompactorScheduler = null
            });
            using var read = db.StartReadOnlyTransaction();
            return read.GetCommitUlong();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var node in Nodes) node.Cancellation.Cancel();
            foreach (var node in Nodes)
            {
                try { await node.Run; }
                catch (OperationCanceledException) { }
                node.Db?.Dispose();
                if (node.Collection != null) await node.Collection.DisposeAsync();
                node.Files.Dispose();
                node.Cancellation.Dispose();
            }
        }

        public sealed class Store(Cluster cluster) : IReplicationLeaseStorage, ILeaderRecordStorage, ICanonicalTrlStorage
        {
            public bool Unavailable;
            public TaskCompletionSource? HoldWrites;
            public int Acquires;
            void Check(CancellationToken cancellation)
            {
                cancellation.ThrowIfCancellationRequested();
                if (Unavailable) throw new IOException("Node cannot reach Blob storage.");
            }
            public ValueTask<LeaseGrant?> AcquireAsync(CancellationToken cancellation)
            {
                Check(cancellation);
                Acquires++;
                if (cluster._lease != null && cluster._expiry > cluster.Clock.Elapsed.Ticks) return ValueTask.FromResult<LeaseGrant?>(null);
                cluster._lease = (++cluster._leaseNumber).ToString();
                cluster._expiry = cluster.Clock.Elapsed.Ticks + 100;
                return ValueTask.FromResult<LeaseGrant?>(new(cluster._lease, TimeSpan.FromTicks(100)));
            }
            public ValueTask<TimeSpan?> RenewAsync(string handle, CancellationToken cancellation)
            {
                Check(cancellation);
                if (handle != cluster._lease || cluster._expiry <= cluster.Clock.Elapsed.Ticks) return ValueTask.FromResult<TimeSpan?>(null);
                cluster._expiry = cluster.Clock.Elapsed.Ticks + 100;
                return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromTicks(100));
            }
            public ValueTask<LeaderRecord> ReadAsync(CancellationToken cancellation)
            { Check(cancellation); return ValueTask.FromResult(cluster.Record); }
            public ValueTask<LeaderWriteOutcome> WriteAsync(string handle, string token, string json, CancellationToken cancellation)
            {
                Check(cancellation);
                if (handle != cluster._lease || cluster._expiry <= cluster.Clock.Elapsed.Ticks || token != cluster.Record.Token)
                    return ValueTask.FromResult(LeaderWriteOutcome.Rejected);
                cluster.Record = new((int.Parse(token) + 1).ToString(), json);
                return ValueTask.FromResult(LeaderWriteOutcome.Applied);
            }
            public ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation)
            { Check(cancellation); return cluster.Trls.ReadAsync(key, cancellation); }
            public ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination, CancellationToken cancellation)
            { Check(cancellation); return cluster.Trls.ReadRangeAsync(key, token, offset, destination, cancellation); }
            public async ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation)
            {
                Check(cancellation);
                if (HoldWrites != null) await HoldWrites.Task.WaitAsync(cancellation).ConfigureAwait(false);
                Check(cancellation);
                return await cluster.Trls.WriteAsync(write, cancellation).ConfigureAwait(false);
            }
        }
    }

    sealed class Host : IReplicationNodeHost
    {
        readonly Cluster _cluster;
        readonly string _name;
        readonly DeterministicScheduler.Scope _scope;
        readonly object _progressLock = new();
        LeaderTrlProgress? _progress;
        int _session;
        public readonly InMemoryReplicationFileStorage Files = new();
        public readonly TransactionLogCapture Capture = new();
        public readonly CancellationTokenSource Cancellation = new();
        public readonly Cluster.Store Storage;
        public readonly PeerTransport Peers;
        public BTreeKeyValueDB? Db;
        public ReplicationFileSet? Collection;
        public ReplicationNodeCoordinator Coordinator = null!;
        public Task Run = null!;
        public int Restarts, Applied;

        public Host(Cluster cluster, string name)
        {
            _cluster = cluster;
            _name = name;
            _scope = cluster.Clock.CreateScope(name);
            Storage = new(cluster);
            Peers = new(cluster, name);
        }
        public void Start()
        {
            var leases = new LeaseSessionController(Storage, _scope, 0, TimeSpan.Zero);
            Coordinator = new(new("cluster", _name, TimeSpan.FromTicks(10), TimeSpan.FromTicks(20),
                TimeSpan.FromTicks(15), TimeSpan.FromTicks(10)), this, Storage, leases, Peers, _scope);
            Run = Coordinator.RunAsync(Cancellation.Token);
        }
        public async ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation)
        {
            var inventory = await CanonicalTrlInventory.DiscoverAsync(Storage, new(Cluster.Genesis(1), 1), cancellation);
            Collection = new(Files, inventory);
            await Collection.InitializeAsync(cancellation);
            Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = Collection, TransactionLogCapture = Capture,
                Compression = new NoCompressionStrategy(), CompactorScheduler = null
            }, cancellation);
            return [new("main", Db, Capture, Storage, new(Cluster.Genesis(1), 1),
                new(inventory.Tail.FileId, inventory.Tail.State.Length), id => $"{_name}-{_session}/{id}")];
        }
        public LeaderCandidate CreateCandidate() => new("cluster", _name, $"{_name}-{++_session}", 1,
            ["main"], _name, $"secret-{_name}-{_session}");
        public LeaderTrlProgress? GetProgress(string database) { lock (_progressLock) return _progress; }
        public void RequestRestart(string reason) => Restarts++;
        public void ReportStatus(ReplicationNodeRole role) { }
        public async Task Write(ulong id, byte value)
        {
            using (var transaction = await Db!.StartWritingTransaction(id))
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([(byte)id], [value]);
                transaction.Commit();
            }
            var end = Capture.Completed;
            lock (_progressLock) _progress = new(id, end.FileId, end.Offset);
            Applied++;
        }
    }

    sealed class PeerTransport(Cluster cluster, string caller) : IReplicationPeerTransport
    {
        public int Reads;
        public TaskCompletionSource? HoldReplies;
        public IDisposable Listen(string endpoint, Func<ReplicationPeerIdentity, IReplicationPeerSession> accept) => cluster.Transport.Listen(endpoint, accept);
        void Check(string endpoint, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            if (cluster.Isolated.Contains(caller) || cluster.Isolated.Contains(endpoint)) throw new IOException("Peer partition.");
        }
        public async ValueTask<IReplicationPeerSession> ConnectAsync(ReplicationPeerIdentity identity, CancellationToken cancellation)
        {
            Check(identity.Endpoint, cancellation);
            return new Session(this, identity.Endpoint, await cluster.Transport.ConnectAsync(identity, cancellation));
        }
        sealed class Session(PeerTransport owner, string endpoint, IReplicationPeerSession inner) : IReplicationPeerSession
        {
            public void Dispose() => inner.Dispose();
            public async ValueTask<ReplicationPeerProgress> PollAsync(string database, long challenge, TimeSpan duration, CancellationToken cancellation)
            {
                owner.Check(endpoint, cancellation);
                var result = await inner.PollAsync(database, challenge, duration, cancellation).ConfigureAwait(false);
                if (owner.HoldReplies != null) await owner.HoldReplies.Task.WaitAsync(cancellation).ConfigureAwait(false);
                owner.Check(endpoint, cancellation);
                return result;
            }
            public ILeaderTrlReader Reader(string database) => new ReaderProxy(owner, endpoint, inner.Reader(database));
        }
        sealed class ReaderProxy(PeerTransport owner, string endpoint, ILeaderTrlReader inner) : ILeaderTrlReader
        {
            public ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
            {
                owner.Check(endpoint, cancellation);
                owner.Reads++;
                return inner.ReadAsync(fileId, offset, destination, cancellation);
            }
        }
    }

    [Fact]
    public async Task ThreeNodesRestoreFollowAndFailOverAutomaticallyWithDelayedOldPublication()
    {
        await using var cluster = await Cluster.Create();
        var first = cluster.Start("first");
        var second = cluster.Start("second");
        var third = cluster.Start("third");
        cluster.Advance(10);
        Assert.Equal(ReplicationNodeRole.Leader, first.Coordinator.Role);
        var oldTerm = JsonNode.Parse(cluster.Record.Json)!["term"]!.GetValue<ulong>();
        cluster.Trls.Inject = n => cluster.Trls.Requests[n - 1].Write.Metadata.Term == oldTerm &&
            cluster.Trls.Requests[n - 1].Write.ExpectedToken != null ? Fault.DelayEffect : Fault.None;
        foreach (var node in cluster.Nodes) await node.Write(2, 2);
        cluster.Advance(10);
        Assert.True(second.Peers.Reads > 0 && third.Peers.Reads > 0);
        Assert.Equal(second.Capture.Completed, second.Capture.Acknowledged);
        Assert.Equal(1ul, await cluster.RestoreEvent()); // Followers matched leader-only bytes.
        first.Storage.Unavailable = true;
        cluster.Isolated.Add("first");
        cluster.Advance(140);
        var replacement = Assert.Single(cluster.Nodes, n => n.Coordinator.Role == ReplicationNodeRole.Leader);
        Assert.NotSame(first, replacement);
        Assert.True(JsonNode.Parse(cluster.Record.Json)!["term"]!.GetValue<ulong>() > oldTerm);
        Assert.Equal(2ul, await cluster.RestoreEvent());
        var applied = cluster.Trls.Applied;
        cluster.Trls.CompleteDelayed();
        Assert.Equal(applied, cluster.Trls.Applied); // Old-token append cannot land after adoption.
        first.Storage.Unavailable = false;
        cluster.Isolated.Clear();
        cluster.Advance(30);
        Assert.Equal(ReplicationNodeRole.Follower, first.Coordinator.Role);
        foreach (var node in cluster.Nodes) await node.Write(3, 3);
        cluster.Advance(20);
        Assert.Equal(3ul, await cluster.RestoreEvent());
        Assert.All(cluster.Nodes, n => { Assert.Equal(2, n.Applied); Assert.Equal(0, n.Restarts); Assert.False(n.Run.IsCompleted); });
    }

    [Fact]
    public async Task RenewalIsIndependentOfBlockedPublicationAndLatePeerRepliesCannotAcknowledge()
    {
        await using var cluster = await Cluster.Create();
        var first = cluster.Start("first");
        var second = cluster.Start("second");
        cluster.Advance(10);
        first.Storage.HoldWrites = new();
        second.Peers.HoldReplies = new();
        await first.Write(2, 2);
        await second.Write(2, 2);
        var acknowledged = second.Capture.Acknowledged;
        var term = JsonNode.Parse(cluster.Record.Json)!["term"]!.GetValue<ulong>();
        cluster.Advance(300);
        Assert.Equal(term, JsonNode.Parse(cluster.Record.Json)!["term"]!.GetValue<ulong>());
        Assert.Equal(ReplicationNodeRole.Leader, first.Coordinator.Role);
        Assert.Equal(acknowledged, second.Capture.Acknowledged);
        Assert.Equal(1ul, await cluster.RestoreEvent());
        first.Storage.HoldWrites.SetResult();
        first.Storage.HoldWrites = null;
        second.Peers.HoldReplies.SetResult();
        second.Peers.HoldReplies = null;
        cluster.Advance(30);
        Assert.Equal(2ul, await cluster.RestoreEvent());
        Assert.Equal(second.Capture.Completed, second.Capture.Acknowledged);
        Assert.Equal(0, second.Restarts);
    }

    [Fact]
    public async Task PeerAuthenticationRejectsWrongKeyAndOldSessionIdentity()
    {
        await using var cluster = await Cluster.Create();
        var first = cluster.Start("first");
        cluster.Advance(10);
        var json = JsonNode.Parse(cluster.Record.Json)!;
        var identity = new ReplicationPeerIdentity("cluster", json["term"]!.GetValue<ulong>(),
            json["sessionId"]!.GetValue<string>(), "first", json["apiKey"]!.GetValue<string>());
        await Assert.ThrowsAsync<IOException>(() => cluster.Transport.ConnectAsync(identity with { ApiKey = "wrong" }, default).AsTask());
        await Assert.ThrowsAsync<IOException>(() => cluster.Transport.ConnectAsync(identity with { SessionId = "previous" }, default).AsTask());
        using var accepted = await cluster.Transport.ConnectAsync(identity, default);
        Assert.True((await accepted.PollAsync("main", 1, TimeSpan.FromTicks(10), default)).Granted);
        Assert.DoesNotContain(identity.ApiKey, identity.ToString());
    }

    [Fact]
    public async Task StartupOutagePreventsElectionAndDivergenceRequestsRestartWithoutStoppingLocalWork()
    {
        await using var cluster = await Cluster.Create();
        var first = cluster.Start("first");
        var second = cluster.Start("second", true);
        cluster.Advance(20);
        Assert.Equal(ReplicationNodeRole.Restoring, second.Coordinator.Role);
        Assert.Equal(0, second.Storage.Acquires);
        second.Storage.Unavailable = false;
        cluster.Advance(10);
        await first.Write(2, 2);
        await second.Write(2, 9);
        cluster.Advance(20);
        Assert.Equal(1, second.Restarts);
        Assert.Equal(ReplicationNodeRole.RestartRequired, second.Coordinator.Role);
        await second.Run;
        await second.Write(3, 3);
        Assert.Equal(2, second.Applied);
        Assert.Equal(2ul, await cluster.RestoreEvent());
    }
}
