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

        public Host Start(string name, bool unavailable = false, ulong generation = 1, bool coordinatesMain = true, ulong inputEnd = 42, long? compactionTicks = null, long? progressTimeoutTicks = null, bool blockActivation = false, long restartDelayTicks = 1, bool failCheckpoint = false, bool maintain = false, long checkpointDelayTicks = 0, bool smallLogs = false)
        {
            var host = new Host(this, name);
            host.Generation = generation;
            host.InputEnd = inputEnd;
            host.CompactionTicks = compactionTicks;
            host.ProgressTimeoutTicks = progressTimeoutTicks;
            host.RestartDelayTicks = restartDelayTicks;
            host.FailCheckpoint = failCheckpoint;
            host.Maintain = maintain;
            host.SmallLogs = smallLogs;
            host.Storage.SimulateCheckpoints = maintain;
            host.Storage.CheckpointDelayTicks = checkpointDelayTicks;
            host.Storage.FailCheckpoint = failCheckpoint;
            if (blockActivation) host.Storage.HoldWrites = new();
            host.CoordinatesMain = coordinatesMain;
            host.Storage.Unavailable = unavailable;
            Nodes.Add(host);
            host.Start();
            return host;
        }

        // The injected scheduler owns callbacks, not xUnit's synchronization context. All test providers finish
        // synchronously except explicitly held responses, keeping each virtual-time pump deterministic.
        public void Advance(long ticks) => WithoutContext(() => Clock.AdvanceBy(TimeSpan.FromTicks(ticks)));

        // Releasing held responses or cancelling from xUnit's context would queue node continuations to the
        // thread pool, racing the deterministic pump. Without a context they run inline, before the next Advance.
        public void WithoutContext(Action action)
        {
            var previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);
            try { action(); }
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
            foreach (var node in Nodes)
            {
                node.Cancellation.Cancel();
                node.Storage.HoldWrites?.TrySetResult();
            }
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

        public sealed class Store(Cluster cluster) : IReplicationLeaseStorage, IReplicationLeaseTransferStorage, ILeaderRecordStorage, IReplicationStorage
        {
            public IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation) =>
                FailCheckpoint || SimulateCheckpoints ? AsyncEnumerable.Empty<RemoteFile>() : cluster.Trls.EnumerateAsync(cancellation);
            public ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation) => cluster.Trls.ReadAsync(file, offset, buffer, cancellation);
            public ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) =>
                SimulateCheckpoints ? ValueTask.CompletedTask : cluster.Trls.EnsurePureValuesAsync(id, source, cancellation);
            public bool FailCheckpoint;
            public int CheckpointAttempts, CheckpointsPublished;
            public long CheckpointDelayTicks, WriteDelayTicks;
            // The canonical TRL fixture has no immutable-file store; record published KVI identities instead.
            public bool SimulateCheckpoints;
            readonly List<uint> _keyIndexes = new();
            public async ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map, CancellationToken cancellation)
            {
                CheckpointAttempts++;
                if (FailCheckpoint) throw new IOException("Checkpoint upload unavailable.");
                if (CheckpointDelayTicks != 0) await DelayAsync(CheckpointDelayTicks, cancellation).ConfigureAwait(false);
                if (SimulateCheckpoints) _keyIndexes.Add(id);
                else await cluster.Trls.PublishKeyIndexAsync(id, snapshot, map, cancellation).ConfigureAwait(false);
                CheckpointsPublished++;
            }
            // Virtual-time transfer duration: completes from the deterministic scheduler, cancellable like a real request.
            DeterministicScheduler.Scope? _delays;
            static int _delayScopes;
            async ValueTask DelayAsync(long ticks, CancellationToken cancellation)
            {
                var scope = _delays ??= cluster.Clock.CreateScope($"storage-delay-{Interlocked.Increment(ref _delayScopes)}");
                var done = new TaskCompletionSource();
                using var timer = scope.Schedule(TimeSpan.FromTicks(ticks), () => done.TrySetResult(), "storage transfer");
                // Synchronous continuations keep the rest of the operation inside the deterministic pump.
                using var registration = cancellation.Register(() => done.TrySetCanceled(cancellation));
                await done.Task.ConfigureAwait(false);
            }
            public IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync(CancellationToken cancellation) => SimulateCheckpoints
                ? _keyIndexes.Select(id => new RemoteMaintenanceFile($"files/{id}.kvi", id, KVFileType.KeyIndex, "1")).ToAsyncEnumerable()
                : cluster.Trls.EnumerateMaintenanceAsync(cancellation);
            public ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation) => cluster.Trls.DeleteAsync(file, cancellation);
            public ValueTask<bool> ProtectPureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) =>
                SimulateCheckpoints ? ValueTask.FromResult(true) : cluster.Trls.ProtectPureValuesAsync(id, source, cancellation);

            public bool Unavailable;
            public TaskCompletionSource? HoldWrites;
            public bool IgnoreWriteCancellation;
            public int Renews;
            public int Acquires, Transfers;
            void Check(CancellationToken cancellation)
            {
                cancellation.ThrowIfCancellationRequested();
                if (Unavailable) throw new IOException("Node cannot reach Blob storage.");
            }
            public ValueTask TransferAsync(string currentHandle, string proposedHandle, CancellationToken cancellation)
            {
                Check(cancellation);
                if (cluster._lease != currentHandle || cluster._expiry <= cluster.Clock.Elapsed.Ticks) throw new IOException("Stale transfer.");
                Transfers++;
                cluster._lease = proposedHandle;
                return ValueTask.CompletedTask;
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
                Renews++;
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
                if (WriteDelayTicks != 0) await DelayAsync(WriteDelayTicks, cancellation).ConfigureAwait(false);
                if (HoldWrites != null)
                {
                    if (IgnoreWriteCancellation) await HoldWrites.Task.ConfigureAwait(false);
                    else await HoldWrites.Task.WaitAsync(cancellation).ConfigureAwait(false);
                }
                Check(cancellation);
                return await cluster.Trls.WriteAsync(write, cancellation).ConfigureAwait(false);
            }
        }
    }

    sealed class Host : IReplicationNodeHost, IKeyValueDBLogger, IReplicationFatalRecovery
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
        public readonly ReplicationStatus Status = new();
        public ReplicationNodeCoordinator Coordinator = null!;
        public Task Run = null!;
        public int Restarts, Applied, Initializations, Compactions, LocalKvis;
        public long? CompactionTicks, ProgressTimeoutTicks;
        public long RestartDelayTicks;
        public bool FailCheckpoint, Maintain, SmallLogs;
        public int FatalRestarts;
        public string? FatalReason;
        public LeaseSessionController Leases = null!;
        public ulong InputEnd = 42;
        public bool SchemaOnActivation;
        public PreparedHandoff? PreparedUpgrade { get; set; }
        public readonly List<string> Detached = new();
        public bool Paused { set => _scope.Paused = value; }
        public ulong Generation = 1;
        public bool CoordinatesMain = true;
        public readonly List<string> Removed = new();

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
            var leases = Leases = new LeaseSessionController(Storage, _scope, 0, TimeSpan.Zero);
            Coordinator = new(new("cluster", _name, TimeSpan.FromTicks(10), TimeSpan.FromTicks(20),
                TimeSpan.FromTicks(15), TimeSpan.FromTicks(10), Generation, CompactionTicks is { } ticks ? TimeSpan.FromTicks(ticks) : null,
                ProgressTimeoutTicks is { } timeout ? new(TimeSpan.FromTicks(timeout), TimeSpan.FromTicks(timeout), TimeSpan.FromTicks(RestartDelayTicks), TimeSpan.FromTicks(timeout)) : null), this, Storage, leases, Peers, _scope, Status);
            Run = Coordinator.RunAsync(Cancellation.Token);
        }
        public async ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation)
        {
            if (await Storage.ReadAsync(Cluster.Genesis(1), cancellation) == null)
            {
                Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
                {
                    FileCollection = new LocalReplicatedCollection(Files), TransactionLogCapture = Capture,
                    Compression = new NoCompressionStrategy(), CompactorScheduler = null
                }, cancellation);
                if (!CoordinatesMain) return [];
                return [new("main", Db, Capture, Storage, new(Cluster.Genesis(1), 1), default,
                    id => $"{_name}-{_session}/{id}")];
            }
            var inventory = await CanonicalTrlInventory.DiscoverAsync(Storage, new(Cluster.Genesis(1), 1), cancellation);
            Collection = new(Files, inventory);
            await Collection.InitializeAsync(cancellation);
            Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = Collection, TransactionLogCapture = Capture, Logger = this,
                Compression = new NoCompressionStrategy(), CompactorScheduler = null,
                TransactionLogSizeStrategy = SmallLogs ? new SmallTransactionLogs() : null
            }, cancellation);
            if (!CoordinatesMain) return [];
            return [new("main", Db, Capture, Storage, new(Cluster.Genesis(1), 1),
                new(inventory.Tail.FileId, inventory.Tail.State.Length), id => $"{_name}-{_session}/{id}")];
        }
        public LeaderCandidate CreateCandidate() => new("cluster", _name, $"{_name}-{++_session}", Generation,
            CoordinatesMain ? ["main"] : [], _name, $"secret-{_name}-{_session}");
        public LeaderTrlProgress? GetProgress(string database) { lock (_progressLock) return _progress; }
        public void RequestRestart(string reason) => Restarts++;
        public ReplicationMaintenance? CreateMaintenance(ActivationDatabase database, CanonicalTrlPublisher publisher, LeaseAuthority authority) =>
            FailCheckpoint || Maintain ? new(Db!, Collection!, publisher, Storage, authority, _scope,
                TimeSpan.FromTicks(1000), TimeSpan.FromTicks(1000)) : null;

        public void RequestFatalRestart(string reason)
        {
            Assert.Null(Leases.Current);
            Assert.False(Status.Current.Ready);
            FatalRestarts++;
            FatalReason = reason;
        }
        public void ReportStatus(ReplicationNodeRole role) { }
        public void CompactionStart(ulong totalWaste) => Compactions++;
        public void KeyValueIndexCreated(uint id, long count, ulong size, TimeSpan elapsed, ulong beforeCompressionSize) => LocalKvis++;
        public void ReportTransactionLeak(IKeyValueDBTransaction transaction) { }
        public void CompactionCreatedPureValueFile(uint id, ulong size, uint count, ulong memory) { }
        public void TransactionLogCreated(uint id) { }
        public void FileMarkedForDelete(uint id) { }

        public void DatabaseRemoved(string database) => Removed.Add(database);
        public void SchemaDetached(string database) => Detached.Add(database);
        public ValueTask<ulong> CaptureInitializationCursorAsync(string database, CancellationToken cancellation)
        { Initializations++; return ValueTask.FromResult(InputEnd); }
        public async ValueTask PrepareSchemaAsync(ActivationDatabase database, LeaseAuthority authority, CancellationToken cancellation)
        {
            if (SchemaOnActivation)
            {
                SchemaOnActivation = false;
                await Schema();
            }
            else if (Capture.Completed.FileId != 0)
            {
                using var read = Db!.StartReadOnlyTransaction();
                var end = Capture.Completed;
                lock (_progressLock) _progress = new(read.GetCommitUlong(), end.FileId, end.Offset);
            }
        }
        public async Task Schema(bool rollback = false)
        {
            using (var transaction = await Db!.StartWritingTransaction())
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([99], [9]);
                if (!rollback) transaction.Commit();
            }
            using var read = Db.StartReadOnlyTransaction();
            var end = Capture.Completed;
            lock (_progressLock) _progress = new(read.GetCommitUlong(), end.FileId, end.Offset);
        }
        public async Task Write(ulong id, byte value, int size = 1, byte? key = null)
        {
            using (var transaction = await Db!.StartWritingTransaction(id))
            {
                using var cursor = transaction.CreateCursor();
                cursor.CreateOrUpdateKeyValue([key ?? (byte)id], Enumerable.Repeat(value, size).ToArray());
                transaction.Commit();
            }
            var end = Capture.Completed;
            lock (_progressLock) _progress = new(id, end.FileId, end.Offset);
            Applied++;
        }
    }

    sealed class SmallTransactionLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint fileId) => new(1024, 1536);
    }

    sealed class PeerTransport(Cluster cluster, string caller) : IReplicationPeerTransport
    {
        public int Reads, Polls;
        public long InlineBytes;
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
            public async ValueTask<ReplicationPeerPoll> PollAsync(IReadOnlyList<ReplicationPeerPollRequest> databases, long challenge,
                TimeSpan duration, int inlineBudget, CancellationToken cancellation)
            {
                owner.Check(endpoint, cancellation);
                owner.Polls++;
                var result = await inner.PollAsync(databases, challenge, duration, inlineBudget, cancellation).ConfigureAwait(false);
                owner.InlineBytes += result.Databases.Sum(d => d.Chunks?.Sum(c => (long)c.Bytes.Length) ?? 0);
                if (owner.HoldReplies != null) await owner.HoldReplies.Task.WaitAsync(cancellation).ConfigureAwait(false);
                owner.Check(endpoint, cancellation);
                return result;
            }
            public ValueTask OfferHandoffAsync(PreparedHandoff offer, CancellationToken cancellation)
            { owner.Check(endpoint, cancellation); return inner.OfferHandoffAsync(offer, cancellation); }
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
    public async Task StatusSeparatesLocalComparedAndPublishedCutsAndKeepsPriorSnapshotsImmutable()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        var follower = cluster.Start("follower");
        cluster.Advance(20);
        var previous = follower.Status.Current;
        Assert.True(previous.Ready);
        Assert.Null(Assert.Single(previous.Databases).LocalCommitted);
        cluster.Trls.Inject = _ => Fault.DelayEffect;
        await leader.Write(2, 2);
        await follower.Write(2, 2);
        cluster.Advance(10);
        var following = Assert.Single(follower.Status.Current.Databases);
        Assert.True(follower.Status.Current.Ready);
        Assert.Equal(2ul, following.LocalCommitted!.Value.EventId);
        Assert.Equal(following.LocalCommitted, following.Compared);
        Assert.Null(following.Published); // Comparison says nothing about Blob durability.
        Assert.Equal(1ul, await cluster.RestoreEvent());
        Assert.Null(Assert.Single(previous.Databases).LocalCommitted);
        cluster.Trls.Inject = null;
        cluster.Trls.CompleteDelayed();
        cluster.Advance(30);
        var publishing = Assert.Single(leader.Status.Current.Databases);
        Assert.Equal(publishing.LocalCommitted!.Value.Position, publishing.Published);
        follower.Cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => follower.Run);
        Assert.False(follower.Status.Current.Ready);
        Assert.Equal(ReplicationNodeRole.Stopped, follower.Status.Current.Role);
    }

    [Fact]
    public async Task ApplicationDataSurvivesRealNodeTakeoverWithoutChangingTrlComparison()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        var follower = cluster.Start("follower");
        cluster.Advance(10);
        var leaderData = new ReplicationApplicationData(leader.Storage, "cluster", leader.Leases,
            leader.Coordinator.ApplicationDataLeadership);
        var followerData = new ReplicationApplicationData(follower.Storage, "cluster", follower.Leases,
            follower.Coordinator.ApplicationDataLeadership);
        Assert.Equal(LeaderWriteOutcome.Rejected,
            await followerData.TryWriteAsync(await followerData.ReadAsync(), JsonValue.Create("follower")));
        var term = JsonNode.Parse(cluster.Record.Json)!["term"]!.GetValue<ulong>();
        Assert.Equal(LeaderWriteOutcome.Applied,
            await leaderData.TryWriteAsync(await leaderData.ReadAsync(), JsonValue.Create("retained")));
        Assert.Equal(term, JsonNode.Parse(cluster.Record.Json)!["term"]!.GetValue<ulong>());
        await leader.Write(2, 2);
        await follower.Write(2, 2);
        cluster.Advance(20);
        Assert.Equal(follower.Capture.Completed, follower.Capture.Acknowledged);
        leader.Paused = true;
        cluster.Advance(140);
        Assert.Equal(ReplicationNodeRole.Leader, follower.Coordinator.Role);
        Assert.Equal("retained", (await followerData.ReadAsync()).Value!.GetValue<string>());
        Assert.Equal(LeaderWriteOutcome.Rejected,
            await leaderData.TryWriteAsync(await leaderData.ReadAsync(), JsonValue.Create("expired")));
        Assert.Equal(LeaderWriteOutcome.Applied,
            await followerData.TryWriteAsync(await followerData.ReadAsync(), JsonValue.Create("replacement")));
        Assert.Equal(2ul, await cluster.RestoreEvent());
        Assert.Equal(0, follower.Restarts);
    }

    [Fact]
    public async Task ScheduledLocalCompactionContinuesOnFollowersAndDuringPublicationFailure()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader", compactionTicks: 10);
        var follower = cluster.Start("follower", compactionTicks: 10);
        leader.Storage.HoldWrites = new();
        await leader.Write(2, 2);
        await follower.Write(2, 2);
        cluster.Advance(100);
        Assert.True(leader.Compactions >= 5);
        Assert.True(follower.Compactions >= 5);
        Assert.Equal(0, leader.LocalKvis);
        Assert.Equal(0, follower.LocalKvis);
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        Assert.Equal(1ul, await cluster.RestoreEvent());
        await follower.Write(3, 3);
    }

    [Fact]
    public async Task PreparedHighestGenerationDrainsAndReceivesLeaseWithoutWaitingForExpiry()
    {
        await using var cluster = await Cluster.Create();
        var old = cluster.Start("old");
        var next = cluster.Start("next", generation: 2);
        var newest = cluster.Start("newest", generation: 3);
        next.PreparedUpgrade = new(2, "10000000-0000-0000-0000-000000000002");
        newest.SchemaOnActivation = true;
        newest.PreparedUpgrade = new(3, "10000000-0000-0000-0000-000000000003");
        cluster.Advance(10);
        Assert.Equal(0, old.Storage.Transfers);
        Assert.Null(old.Coordinator.ApplicationDataLeadership()); // Writes stop as soon as handoff drain begins.
        cluster.Advance(50);
        Assert.Equal(1, old.Storage.Transfers);
        Assert.Equal(ReplicationNodeRole.Leader, newest.Coordinator.Role);
        Assert.Equal(3ul, JsonNode.Parse(cluster.Record.Json)!["applicationGeneration"]!.GetValue<ulong>());
        Assert.NotEqual(ReplicationNodeRole.Leader, old.Coordinator.Role);
        Assert.Equal(new[] { "main" }, old.Detached);
        Assert.Equal(1ul, await cluster.RestoreEvent());
        await old.Write(2, 2);
        Assert.Equal(1, old.Applied);
        Assert.Equal(0, old.Restarts);
    }

    [Fact]
    public async Task UpgradeAddingDatabaseHandsOffFromOlderLeaderThatDoesNotKnowIt()
    {
        await using var cluster = new Cluster();
        cluster.Record = new("1", """
            {"format":1,"clusterId":"cluster","term":1,"revision":1,"applicationGeneration":1,
             "databaseNames":[],"sessionId":"seed","peerEndpoint":"seed","apiKey":"seed"}
            """);
        var old = cluster.Start("old", coordinatesMain: false);
        cluster.Advance(30);
        Assert.Equal(ReplicationNodeRole.Leader, old.Coordinator.Role);
        var next = cluster.Start("next", generation: 2);
        next.PreparedUpgrade = new(2, "10000000-0000-0000-0000-000000000002");
        cluster.Advance(60);
        Assert.Equal(1, old.Storage.Transfers);
        Assert.Equal(ReplicationNodeRole.Leader, next.Coordinator.Role);
        Assert.Equal(1, next.Initializations);
        Assert.Equal(0, next.Restarts);
        Assert.Equal(2ul, JsonNode.Parse(cluster.Record.Json)!["applicationGeneration"]!.GetValue<ulong>());
    }

    [Theory]
    [InlineData(0ul)]
    [InlineData(42ul)]
    public async Task NewDatabaseInitializesOnlyOnLeaderAndLostPublicationKeepsCapturedCursor(ulong inputEnd)
    {
        await using var cluster = new Cluster();
        cluster.Trls.Inject = _ => Fault.DelayEffect;
        var leader = cluster.Start("leader", inputEnd: inputEnd);
        var follower = cluster.Start("follower");
        cluster.Advance(30);
        Assert.Equal(1, leader.Initializations);
        Assert.Equal(0, follower.Initializations);
        Assert.Equal(default, follower.Capture.Completed);
        Assert.Equal(ReplicationNodeRole.Activating, leader.Coordinator.Role);
        leader.InputEnd = 99;
        cluster.Trls.CompleteDelayed();
        cluster.Trls.Inject = null;
        cluster.Advance(30);
        Assert.Equal(inputEnd, await cluster.RestoreEvent());
        Assert.Equal(1, leader.Initializations);
        Assert.Equal(1, follower.Restarts);
        var restored = cluster.Start("restored");
        Assert.Equal(0, restored.Initializations);
        using var read = restored.Db!.StartReadOnlyTransaction();
        Assert.Equal(inputEnd, read.GetCommitUlong());
    }

    [Fact]
    public async Task UnpublishedInitializationCanBeRecreatedAtNewInputEndAfterLeaderLoss()
    {
        await using var cluster = new Cluster();
        cluster.Trls.Inject = _ => Fault.DelayEffect;
        var old = cluster.Start("old");
        var next = cluster.Start("next");
        next.InputEnd = 99;
        old.Storage.Unavailable = true;
        cluster.Trls.Inject = null;
        cluster.Advance(140);
        Assert.Equal(ReplicationNodeRole.Leader, next.Coordinator.Role);
        Assert.Equal(1, next.Initializations);
        Assert.Equal(99ul, await cluster.RestoreEvent());
        cluster.Trls.CompleteDelayed();
        Assert.Equal(99ul, await cluster.RestoreEvent());
    }

    [Fact]
    public async Task FollowerRestoredAfterPublishedSchemaTreatsItAsDuplicate()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        cluster.Advance(20);
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        await leader.Schema();
        await leader.Write(2, 2);
        cluster.Advance(20);
        var follower = cluster.Start("follower");
        cluster.Advance(20);
        await leader.Write(3, 3);
        await follower.Write(3, 3);
        cluster.Advance(20);
        Assert.Empty(follower.Detached);
        Assert.Equal(0, follower.Restarts);
        Assert.Equal(3ul, Assert.Single(follower.Status.Current.Databases).Compared?.EventId);
    }

    [Fact]
    public async Task CoalescedSchemaDetachesLaggingFollowerAndNeverPromotesItsLocalWork()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        var follower = cluster.Start("follower");
        await leader.Write(2, 2);
        await leader.Schema();
        await leader.Write(3, 3);
        cluster.Advance(20);
        Assert.Equal(new[] { "main" }, follower.Detached);
        Assert.False(follower.Status.Current.Ready);
        Assert.True(Assert.Single(follower.Status.Current.Databases).Detached);
        Assert.Equal(0, follower.Restarts);
        await follower.Write(2, 9);
        cluster.Isolated.Add("follower");
        cluster.Advance(30);
        cluster.Isolated.Clear();
        cluster.Advance(30);
        Assert.Single(follower.Detached);
        Assert.Equal(0, follower.Restarts);
        var attempts = follower.Storage.Acquires;
        leader.Paused = true;
        follower.Paused = true;
        cluster.Advance(TimeSpan.FromMinutes(15).Ticks + 100);
        follower.Paused = false;
        cluster.Advance(10);
        Assert.Equal(1, follower.Restarts);
        Assert.Equal(attempts, follower.Storage.Acquires);
        await follower.Write(3, 9);
        Assert.Equal(2, follower.Applied);
    }

    [Fact]
    public async Task OlderGenerationFollowsButNeverContendsEvenAfterNewLeaderDisappears()
    {
        await using var cluster = await Cluster.Create();
        var current = cluster.Start("current", generation: 2);
        var older = cluster.Start("older");
        await current.Write(2, 2);
        await older.Write(2, 2);
        cluster.Advance(30);
        Assert.Equal(ReplicationNodeRole.Leader, current.Coordinator.Role);
        Assert.Equal(older.Capture.Completed, older.Capture.Acknowledged);
        Assert.Equal(0, older.Storage.Acquires);
        current.Storage.Unavailable = true;
        cluster.Isolated.Add("current");
        cluster.Advance(300);
        Assert.Equal(0, older.Storage.Acquires);
        Assert.Equal(ReplicationNodeRole.Follower, older.Coordinator.Role);
        await older.Write(3, 3);
        Assert.Equal(2, older.Applied);
        Assert.Equal(0, older.Restarts);
    }

    [Fact]
    public async Task RemovedDatabaseContinuesLocallyWithoutPeerReadsOrElection()
    {
        await using var cluster = await Cluster.Create();
        var current = cluster.Start("current", generation: 2, coordinatesMain: false);
        var older = cluster.Start("older");
        cluster.Advance(300);
        Assert.Equal(ReplicationNodeRole.Leader, current.Coordinator.Role);
        Assert.Equal(new[] { "main" }, older.Removed);
        Assert.Equal(0, older.Storage.Acquires);
        Assert.Equal(0, older.Peers.Reads);
        await older.Write(2, 9);
        Assert.Equal(1, older.Applied);
        Assert.Equal(1ul, await cluster.RestoreEvent());
        Assert.Equal(0, older.Restarts);
        Assert.False(older.Run.IsCompleted);
    }

    [Fact]
    public async Task RunningOldGenerationLosesEligibilityWhenItDiscoversUpgrade()
    {
        await using var cluster = await Cluster.Create();
        var older = cluster.Start("older");
        cluster.Advance(10);
        Assert.Equal(ReplicationNodeRole.Leader, older.Coordinator.Role);
        older.Storage.Unavailable = true;
        cluster.Isolated.Add("older");
        var current = cluster.Start("current", generation: 2);
        cluster.Advance(140);
        Assert.Equal(ReplicationNodeRole.Leader, current.Coordinator.Role);
        older.Storage.Unavailable = false;
        cluster.Isolated.Clear();
        cluster.Advance(30);
        await current.Write(2, 2);
        await older.Write(2, 2);
        cluster.Advance(30);
        Assert.Equal(older.Capture.Completed, older.Capture.Acknowledged);
        var attempts = older.Storage.Acquires;
        current.Storage.Unavailable = true;
        cluster.Isolated.Add("current");
        cluster.Advance(300);
        Assert.Equal(attempts, older.Storage.Acquires);
        Assert.Equal(0, older.Restarts);
        Assert.False(older.Run.IsCompleted);
    }

    [Fact]
    public async Task LaggingFollowerUnderContinuousLoadComparesItsLocalPrefixAndReceivesEachByteOnce()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        var follower = cluster.Start("follower");
        cluster.Advance(30);
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        var acknowledged = follower.Capture.Acknowledged;
        var inline = follower.Peers.InlineBytes;
        for (ulong id = 2; id <= 20; id++)
        {
            await leader.Write(id, (byte)id, size: 1000);
            if (id > 2) await follower.Write(id - 1, (byte)(id - 1), size: 1000); // Always one event behind.
            cluster.Advance(10);
        }
        // The local prefix the leader's cut covers is compared although the leader is always ahead.
        var compared = follower.Status.Current.Databases[0].Compared!.Value;
        Assert.Equal((19ul, follower.Capture.Completed), (compared.EventId, compared.Position));
        Assert.True(follower.Capture.Acknowledged > acknowledged); // Canonical base and local retention advance.
        // One TRL file holds the whole run, so its length bounds the bytes a follower may receive once.
        Assert.Equal(follower.Capture.Completed.FileId, leader.Capture.Completed.FileId);
        Assert.InRange(follower.Peers.InlineBytes - inline, 1, leader.Capture.Completed.Offset); // Never resent.
    }

    [Fact]
    public async Task CaughtUpFollowerComparesInlinePollBytesWithoutRangeReads()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        var follower = cluster.Start("follower");
        cluster.Advance(30);
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        foreach (var node in cluster.Nodes) await node.Write(2, 2);
        cluster.Advance(10); // The first scan in a file validates its header once per leader session.
        var reads = follower.Peers.Reads;
        for (ulong id = 3; id <= 5; id++)
        {
            foreach (var node in cluster.Nodes) await node.Write(id, (byte)id, size: 2000);
            cluster.Advance(10);
            Assert.Equal(follower.Capture.Completed, follower.Status.Current.Databases[0].Compared!.Value.Position);
        }
        Assert.Equal(reads, follower.Peers.Reads);
    }

    [Fact]
    public async Task FollowerSendsOnePollPerStepForProgressAndGrant()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        var follower = cluster.Start("follower");
        cluster.Advance(30);
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        Assert.Equal(ReplicationNodeRole.Follower, follower.Coordinator.Role);
        var polls = follower.Peers.Polls;
        cluster.Advance(100); // Ten poll intervals.
        Assert.InRange(follower.Peers.Polls - polls, 9, 11);
        Assert.True(follower.Status.Current.Ready);
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
        Assert.True(second.Peers.InlineBytes > 0 && third.Peers.InlineBytes > 0);
        Assert.Equal(second.Capture.Completed, second.Status.Current.Databases[0].Compared!.Value.Position);
        Assert.Equal(1ul, await cluster.RestoreEvent()); // Followers matched leader-only bytes.
        // Unpublished matches are not canonical: local retention keeps them for a recheck against the next leader.
        Assert.NotEqual(second.Capture.Completed, second.Capture.Acknowledged);
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
    public async Task LaggingFollowerTakesOverAfterCatchingUpWithoutRestart()
    {
        await using var cluster = await Cluster.Create();
        var first = cluster.Start("first");
        var second = cluster.Start("second");
        cluster.Advance(10);
        Assert.Equal(ReplicationNodeRole.Leader, first.Coordinator.Role);
        await first.Write(2, 2);
        await first.Write(3, 3);
        cluster.Advance(10);
        Assert.Equal(3ul, await cluster.RestoreEvent()); // Published beyond the follower's local execution.
        first.Storage.Unavailable = true;
        cluster.Isolated.Add("first");
        cluster.Advance(140);
        Assert.Equal(ReplicationNodeRole.Activating, second.Coordinator.Role);
        Assert.NotNull(second.Leases.Current); // Keeps the lease while the host catches up.
        await second.Write(2, 2);
        cluster.Advance(20);
        Assert.Equal(ReplicationNodeRole.Activating, second.Coordinator.Role);
        await second.Write(3, 3);
        cluster.Advance(20);
        Assert.Equal(ReplicationNodeRole.Leader, second.Coordinator.Role);
        await second.Write(4, 4);
        cluster.Advance(20);
        Assert.Equal(4ul, await cluster.RestoreEvent());
        Assert.Equal(0, second.Restarts);
    }

    [Fact]
    public async Task LocalCompactionOfStartupTrlsDoesNotBreakFailoverOrFollowerRecheck()
    {
        await using var cluster = await Cluster.Create();
        var first = cluster.Start("first", compactionTicks: 7, smallLogs: true);
        var second = cluster.Start("second", compactionTicks: 7, smallLogs: true);
        var third = cluster.Start("third", compactionTicks: 7, smallLogs: true);
        cluster.Advance(10);
        Assert.Equal(ReplicationNodeRole.Leader, first.Coordinator.Role);
        ulong id = 2;
        for (; id < 40; id++)
        {
            // Overwritten values create waste, so compaction moves live data and deletes old TRLs.
            foreach (var node in cluster.Nodes) await node.Write(id, (byte)id, 300, (byte)(id % 3));
            cluster.Advance(3);
        }
        cluster.Advance(50);
        Assert.Null(second.Files.GetFile(1)); // The restored startup TRL is gone on the followers.
        Assert.Null(third.Files.GetFile(1));
        first.Storage.Unavailable = true;
        cluster.Isolated.Add("first");
        cluster.Advance(200);
        var leader = Assert.Single(cluster.Nodes, n => n.Coordinator.Role == ReplicationNodeRole.Leader);
        var follower = Assert.Single(cluster.Nodes, n => n != first && n != leader);
        Assert.Equal(ReplicationNodeRole.Follower, follower.Coordinator.Role);
        foreach (var node in new[] { leader, follower }) await node.Write(id, 7, 300, 1);
        cluster.Advance(30);
        Assert.Equal(id, await cluster.RestoreEvent());
        Assert.Equal(leader.Capture.Completed, follower.Status.Current.Databases[0].Compared!.Value.Position);
        Assert.All(new[] { leader, follower }, n => Assert.Equal(0, n.Restarts));
    }

    [Fact]
    public async Task MalformedLeaderRecordRequestsRestartInsteadOfCrashingTheNode()
    {
        await using var cluster = await Cluster.Create();
        // A newer generation keeps this node from contending, so it must follow a leader that has no session.
        cluster.Record = new("1", """{"format":1,"clusterId":"cluster","term":3,"applicationGeneration":2,"databaseNames":["main"]}""");
        var follower = cluster.Start("follower");
        cluster.Advance(30);
        Assert.Equal(1, follower.Restarts);
        await follower.Run.WaitAsync(TimeSpan.FromSeconds(10)); // Completes normally; a crash would rethrow here.
        Assert.Equal(ReplicationNodeRole.RestartRequired, follower.Coordinator.Role);
    }

    [Fact]
    public async Task CanonicalWritesSlowerThanRequestTimeoutStillPublish()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader");
        cluster.Advance(10);
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        leader.Storage.WriteDelayTicks = 40; // RequestTimeout is 15 ticks.
        await leader.Write(2, 2);
        cluster.Advance(80);
        Assert.Equal(2ul, await cluster.RestoreEvent());
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        Assert.Equal(0, leader.Restarts);
    }

    [Fact]
    public async Task SlowCheckpointCompletesWithoutHoldingBackPublication()
    {
        await using var cluster = await Cluster.Create();
        // The upload spans several request timeouts and poll intervals.
        var leader = cluster.Start("leader", maintain: true, checkpointDelayTicks: 60);
        cluster.Advance(10);
        Assert.Equal(ReplicationNodeRole.Leader, leader.Coordinator.Role);
        Assert.Equal(1, leader.Storage.CheckpointAttempts);
        await leader.Write(2, 2);
        cluster.Advance(25);
        Assert.Equal(0, leader.Storage.CheckpointsPublished);
        Assert.Equal(2ul, await cluster.RestoreEvent()); // Published while the upload was still in flight.
        cluster.Advance(60);
        Assert.Equal(1, leader.Storage.CheckpointsPublished);
        Assert.Equal(1, leader.Storage.CheckpointAttempts); // Never cancelled and restarted from scratch.
        Assert.Equal(0, leader.Restarts);
    }

    [Fact]
    public async Task CheckpointRetriesFenceAndDelayFatalRestartDespiteHealthyRenewalsAndIdleInput()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader", progressTimeoutTicks: 60, restartDelayTicks: 100, failCheckpoint: true);
        cluster.Advance(80);
        Assert.True(leader.Storage.CheckpointAttempts > 1);
        Assert.True(leader.Storage.Renews > 0);
        Assert.Null(leader.Leases.Current);
        Assert.False(leader.Status.Current.Ready);
        Assert.Equal(0, leader.FatalRestarts);
        cluster.Advance(100);
        Assert.Equal(1, leader.FatalRestarts);
        Assert.Contains("checkpoint maintenance", leader.FatalReason);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FatalRecoveryFencesImmediatelyAndDelaysRestartWhileHealthyFollowerTakesOver(bool ignoreCancellation)
    {
        await using var cluster = await Cluster.Create();
        var failed = cluster.Start("failed", progressTimeoutTicks: 60, blockActivation: true, restartDelayTicks: 200);
        failed.Storage.IgnoreWriteCancellation = ignoreCancellation;
        var healthy = cluster.Start("healthy");
        cluster.Advance(100);
        Assert.Null(failed.Leases.Current);
        Assert.False(failed.Status.Current.Ready);
        Assert.Equal(0, failed.FatalRestarts);
        Assert.False(failed.Run.IsCompleted);
        var renewals = failed.Storage.Renews;
        var acquisitions = failed.Storage.Acquires;
        // Even shutdown or a late successful response must not cancel or bypass an already chosen fatal delay.
        cluster.WithoutContext(() =>
        {
            failed.Cancellation.Cancel();
            failed.Storage.HoldWrites!.TrySetResult();
        });
        cluster.Advance(150);
        Assert.Equal(ReplicationNodeRole.Leader, healthy.Coordinator.Role);
        Assert.Equal(renewals, failed.Storage.Renews);
        Assert.Equal(acquisitions, failed.Storage.Acquires);
        Assert.Equal(0, failed.FatalRestarts);
        Assert.False(failed.Run.IsCompleted);
        cluster.Advance(20);
        Assert.Equal(1, failed.FatalRestarts);
        cluster.Advance(200);
        Assert.Equal(1, failed.FatalRestarts);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublicationWatchdogFencesDespiteSuccessfulRenewalsAndUncooperativeStorage(bool ignoreCancellation)
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader", progressTimeoutTicks: 60);
        cluster.Advance(10);
        var authority = leader.Leases.Current!;
        leader.Storage.HoldWrites = new();
        leader.Storage.IgnoreWriteCancellation = ignoreCancellation;
        await leader.Write(2, 2);
        cluster.Advance(100);
        Assert.Equal(1, leader.FatalRestarts);
        Assert.Contains("publication", leader.FatalReason);
        Assert.True(leader.Storage.Renews > 0);
        Assert.True(authority.IsFenced);
        Assert.False(leader.Status.Current.Ready);
        Assert.Equal(0, leader.Restarts);
        Assert.Equal(1ul, await cluster.RestoreEvent());
        var renewals = leader.Storage.Renews;
        cluster.Advance(200);
        Assert.Equal(renewals, leader.Storage.Renews);
        Assert.Equal(1, leader.FatalRestarts);
        cluster.WithoutContext(leader.Storage.HoldWrites.SetResult);
        leader.Storage.HoldWrites = null;
        await leader.Run;
        Assert.Equal(1ul, await cluster.RestoreEvent()); // The late write did not become canonical.
    }

    [Fact]
    public async Task RepeatedActivationRetriesDoNotResetTheProgressDeadline()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader", progressTimeoutTicks: 60, blockActivation: true);
        cluster.Advance(100);
        Assert.Equal(1, leader.FatalRestarts);
        Assert.Contains("activation", leader.FatalReason);
        Assert.Null(leader.Leases.Current);
        Assert.Equal(1ul, await cluster.RestoreEvent());
        Assert.False(leader.Status.Current.Ready);
        Assert.Equal(0, leader.Applied);
    }

    [Fact]
    public async Task IdleLeaderAndUnavailableFollowerRestoreDoNotTriggerProgressRecovery()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader", progressTimeoutTicks: 60);
        var follower = cluster.Start("follower", unavailable: true, progressTimeoutTicks: 60);
        cluster.Advance(500);
        Assert.True(leader.Status.Current.Ready);
        Assert.Equal(0, leader.FatalRestarts);
        Assert.Equal(0, follower.FatalRestarts);
        Assert.Equal(0, follower.Storage.Acquires);
        Assert.Equal(ReplicationNodeRole.Restoring, follower.Coordinator.Role);
    }

    [Fact]
    public async Task SuccessfulPublicationClearsDeadlineAndShutdownCannotTriggerFatalRecovery()
    {
        await using var cluster = await Cluster.Create();
        var leader = cluster.Start("leader", progressTimeoutTicks: 60);
        cluster.Advance(10);
        cluster.Trls.Inject = _ => Fault.DelayEffect;
        await leader.Write(2, 2);
        cluster.Advance(20);
        cluster.Trls.CompleteDelayed();
        cluster.Trls.Inject = null;
        cluster.Advance(200);
        Assert.Equal(2ul, await cluster.RestoreEvent());
        Assert.Equal(0, leader.FatalRestarts);
        leader.Storage.HoldWrites = new();
        leader.Storage.IgnoreWriteCancellation = true;
        await leader.Write(3, 3);
        cluster.Advance(10);
        cluster.WithoutContext(leader.Cancellation.Cancel);
        cluster.Advance(200);
        Assert.Equal(0, leader.FatalRestarts);
        cluster.WithoutContext(leader.Storage.HoldWrites.SetResult);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leader.Run);
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
        cluster.WithoutContext(() =>
        {
            first.Storage.HoldWrites.SetResult();
            first.Storage.HoldWrites = null;
            second.Peers.HoldReplies.SetResult();
            second.Peers.HoldReplies = null;
        });
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
        Assert.True((await accepted.PollAsync([new("main")], 1, TimeSpan.FromTicks(10), 0, default)).Granted);
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
        Assert.False(second.Status.Current.Ready);
        second.Storage.Unavailable = false;
        cluster.Advance(10);
        await first.Write(2, 2);
        await second.Write(2, 9);
        cluster.Advance(20);
        Assert.Equal(1, second.Restarts);
        Assert.Equal(ReplicationNodeRole.RestartRequired, second.Coordinator.Role);
        Assert.False(second.Status.Current.Ready);
        await second.Run;
        await second.Write(3, 3);
        Assert.Equal(2, second.Applied);
        Assert.Equal(2ul, await cluster.RestoreEvent());
    }
}
