using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json.Nodes;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Azure.Test;

public class AzureReplicationTest(AzuriteFixture fixture) : IClassFixture<AzuriteFixture>
{
    const string Initial = """{"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """;

    sealed class Clock : IReplicationScheduler
    {
        readonly System.Diagnostics.Stopwatch _clock = System.Diagnostics.Stopwatch.StartNew();
        public TimeSpan Elapsed => _clock.Elapsed;
        public IDisposable Schedule(TimeSpan delay, Action callback, string description) => throw new NotSupportedException();
    }

    static async Task<BTreeKeyValueDB> Open(InMemoryReplicationFileStorage files, TransactionLogCapture capture, uint splitSize = int.MaxValue)
    {
        var file = files.AddFile("trl", FileIdParity.Odd);
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock("BTDB3"u8);
        writer.WriteGuid(new Guid("0ea12e80-f23f-4d46-b23e-abd3f8288f60"));
        writer.WriteUInt8((byte)KVFileType.TransactionLog);
        writer.WriteVInt64(1);
        writer.WriteVInt32(0);
        writer.Flush();
        file.HardFlush();
        return await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new BTDB.Replication.Test.LocalReplicatedCollection(files), TransactionLogCapture = capture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null, FileSplitSize = splitSize
        });
    }

    static async Task Write(BTreeKeyValueDB db, ulong id)
    {
        using var transaction = await db.StartWritingTransaction(id);
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([(byte)id], [(byte)id]);
        transaction.Commit();
    }

    [Fact]
    public async Task ApplicationDataUsesAzureLeaseAndEtagAndReconcilesLostReply()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var blob = container.GetBlobClient("cluster/leader.json");
        var storage = new AzureLeaderStorage(blob, TimeSpan.FromSeconds(15), Initial);
        var leases = new LeaseSessionController(storage, new Clock(), 0, TimeSpan.FromMilliseconds(10));
        var authority = (await leases.MaintainAsync())!;
        var selected = (await new LeaderSelection(storage, leases, authority,
            new("cluster", "node", "session", 1, [], "local://node", "secret")).SelectAsync())!;
        var data = new ReplicationApplicationData(storage, "cluster", leases, () => selected);
        var snapshot = await data.ReadAsync();
        faults.LoseSelection = true;
        Assert.Equal(LeaderWriteOutcome.Applied, await data.TryWriteAsync(snapshot, new JsonObject { ["counter"] = 42 }));
        Assert.Equal(42, (await data.ReadAsync()).Value!["counter"]!.GetValue<int>());
        Assert.Equal(LeaderWriteOutcome.Rejected, await data.TryWriteAsync(snapshot, JsonValue.Create("stale")));
        var latest = await data.ReadAsync();
        // The local authority still looks valid, but the actual server lease has changed.
        await blob.GetBlobLeaseClient(leases.GetHandle(authority)).ChangeAsync(Guid.NewGuid().ToString());
        Assert.Equal(LeaderWriteOutcome.Rejected, await data.TryWriteAsync(latest, JsonValue.Create("old lease")));
        Assert.Equal(42, (await data.ReadAsync()).Value!["counter"]!.GetValue<int>());
        leases.Close();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AcquiredAzureLeaseSelectsTermValidatesAdoptsAndPublishesOnlyAfterActivation(bool loseAdoptionReply)
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var blob = container.GetBlobClient("cluster/leader.json");
        var storage = new AzureLeaderStorage(blob, TimeSpan.FromSeconds(15), Initial);
        var canonical = new AzureReplicationStorage(container, "main");
        var leases = new LeaseSessionController(storage, new Clock(), 0, TimeSpan.FromMilliseconds(10));
        var authority = (await leases.MaintainAsync())!;
        var selection = new LeaderSelection(storage, leases, authority,
            new("cluster", "first", "session1", 1, ["main"], "local://first", "key1"));
        var selected = (await selection.SelectAsync())!;
        using var firstFiles = new InMemoryReplicationFileStorage();
        using var nextFiles = new InMemoryReplicationFileStorage();
        var firstCapture = new TransactionLogCapture();
        var nextCapture = new TransactionLogCapture();
        using var first = await Open(firstFiles, firstCapture);
        using var next = await Open(nextFiles, nextCapture);
        using var oldPublisher = new CanonicalTrlPublisher(first, firstCapture, canonical, authority, selected.Term,
            id => $"term1/{id}");
        await Write(first, 1);
        await Write(next, 1);
        Assert.Equal(TrlPublishResult.Published, await oldPublisher.PublishNextAsync());
        var published = oldPublisher.PublishedPosition;
        await Write(first, 2);
        await Write(next, 2);
        var oldHandle = leases.GetHandle(authority);
        leases.Close();
        await blob.GetBlobLeaseClient(oldHandle).ReleaseAsync();
        var nextLeases = new LeaseSessionController(storage, new Clock(), 0, TimeSpan.FromMilliseconds(10));
        var nextAuthority = (await nextLeases.MaintainAsync())!;
        var nextSelection = new LeaderSelection(storage, nextLeases, nextAuthority,
            new("cluster", "next", "session2", 1, ["main"], "local://next", "key2"));
        var session = new LeadershipSession(nextSelection,
            [new("main", next, nextCapture, canonical, new("term1/1", 1), new(1, 0), id => $"term2/{id}")]);
        faults.LoseCommit = loseAdoptionReply;
        var publishers = (await session.ActivateAsync())!;
        using var publisher = Assert.Single(publishers);
        Assert.Equal(published, publisher.PublishedPosition);
        Assert.Equal(2ul, publisher.Tail!.State.Metadata.Term);
        Assert.Same(publishers, await session.ActivateAsync());
        Assert.Equal(TrlPublishResult.AuthorityLost, await oldPublisher.PublishNextAsync());
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        using var restoredFiles = new InMemoryReplicationFileStorage();
        var inventory = await CanonicalTrlInventory.DiscoverAsync(canonical, new("term1/1", 1));
        var checkpoints = canonical.Bind(inventory, nextAuthority);
        using (var snapshot = next.CaptureKeyIndexSnapshot())
        {
            faults.LoseCommit = true;
            await checkpoints.PublishKeyIndexAsync(2, snapshot, new System.Collections.Generic.Dictionary<uint, uint>(), default);
            await checkpoints.PublishKeyIndexAsync(2, snapshot, new System.Collections.Generic.Dictionary<uint, uint>(), default);
        }
        await using var collection = new ReplicationFileSet(restoredFiles, checkpoints);
        await collection.InitializeAsync();
        using var restored = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = collection, Compression = new NoCompressionStrategy(), CompactorScheduler = null
        });
        using var read = restored.StartReadOnlyTransaction();
        Assert.Equal(2ul, read.GetCommitUlong());
    }

    sealed class CleanupTime : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.Parse("2026-09-22T00:00:00Z");
        public override DateTimeOffset GetUtcNow() => Now;
    }

    [Theory]
    [InlineData("files/2.pvl", KVFileType.PureValues)]
    [InlineData("files/2.kvi", KVFileType.KeyIndex)]
    [InlineData("obsolete/2", KVFileType.TransactionLog)]
    public async Task DeletionDeadlineSurvivesNewAdapterAndDoesNotMoveOnRetry(string key, KVFileType type)
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var time = new CleanupTime();
        var raw = new AzureReplicationStorage(container, "db", time);
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var files = new InMemoryFileCollection();
        var source = files.AddFile("trl");
        var writer = new MemWriter(source.GetAppenderWriter());
        writer.WriteUInt8(1); writer.Flush();
        await raw.WriteAsync(new(1, "root/1", null, 0, 1, new(1), source), default);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("root/1", 1));
        var storage = raw.Bind(inventory, authority);
        var blob = container.GetBlobClient("db/" + key);
        var metadata = type == KVFileType.TransactionLog
            ? new Dictionary<string, string>(new TrlMetadata(1).Encode()) { ["btdb_file_id"] = "2" }
            : new Dictionary<string, string> { ["btdb_sha256"] = "unchanged" };
        await blob.UploadAsync(BinaryData.FromBytes(new byte[] { 1 }), new BlobUploadOptions { Metadata = metadata });
        var before = (await blob.GetPropertiesAsync()).Value;
        faults.LoseSelection = true;
        await Assert.ThrowsAsync<IOException>(() => storage.ScheduleDeletionAsync(new(key, 2, type, before.ETag.ToString()), TimeSpan.FromHours(24), default).AsTask());
        var persisted = new List<RemoteMaintenanceFile>();
        await foreach (var file in storage.EnumerateMaintenanceAsync(default)) persisted.Add(file);
        var marked = Assert.Single(persisted, file => file.Key == key);
        Assert.Equal(time.Now + TimeSpan.FromHours(24), marked.DeleteAfter);
        var deadline = marked.DeleteAfter;
        await storage.DeleteAsync(marked, default);
        Assert.True((await blob.ExistsAsync()).Value);
        time.Now += TimeSpan.FromHours(23);
        var restartedRaw = new AzureReplicationStorage(container, "db", time);
        var restarted = restartedRaw.Bind(await CanonicalTrlInventory.DiscoverAsync(restartedRaw, new("root/1", 1)), authority);
        var retried = await restarted.ScheduleDeletionAsync(marked, TimeSpan.FromHours(24), default);
        Assert.Equal(deadline, retried.DeleteAfter);
        await restarted.DeleteAsync(retried, default);
        Assert.True((await blob.ExistsAsync()).Value);
        time.Now += TimeSpan.FromHours(1);
        await restarted.DeleteAsync(retried, default);
        Assert.False((await blob.ExistsAsync()).Value);
    }

    [Fact]
    public async Task LegacyCheckpointWithMissingGenesisCannotBeMisclassifiedAsUnpublishedDatabase()
    {
        var container = await fixture.ContainerAsync();
        await container.GetBlobClient("db/files/2.kvi").UploadAsync(BinaryData.FromBytes(new byte[] { 1 }));
        var storage = new AzureReplicationStorage(container, "db");
        await Assert.ThrowsAsync<IOException>(() => storage.ResolveRecoveryRootAsync(new("trl/1", 1), default).AsTask());
    }

    [Fact]
    public async Task CheckpointRootRestoresNativeDatabaseAfterObsoleteGenesisTrlsAreDeleted()
    {
        var container = await fixture.ContainerAsync();
        var raw = new AzureReplicationStorage(container, "db");
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await Open(local, capture, 1024);
        for (ulong i = 1; i <= 400; i++) await Write(db, i);
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"trl/{id}");
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("trl/1", 1));
        var storage = raw.Bind(inventory, authority);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        Assert.True(snapshot.TransactionLogFileId > 1);
        await storage.PublishKeyIndexAsync(10000, snapshot, new Dictionary<uint, uint>(), default);
        var gc = new RemoteGarbageCollector(storage, authority, TimeSpan.Zero);
        await gc.CollectAsync(new(10000, snapshot.TransactionLogFileId,
            snapshot.Sources.Select(s => s.FileId).ToHashSet()), default);
        Assert.Null(await raw.ReadAsync("trl/1", default));
        var fresh = new AzureReplicationStorage(container, "db");
        var recovered = await CanonicalTrlInventory.DiscoverAsync(fresh, new("trl/1", 1));
        using var cache = new InMemoryReplicationFileStorage();
        await using var collection = new ReplicationFileSet(cache, fresh.Bind(recovered));
        await collection.InitializeAsync();
        using var restored = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        { FileCollection = collection, CompactorScheduler = null, Compression = new NoCompressionStrategy() });
        using var transaction = restored.StartReadOnlyTransaction();
        Assert.Equal(400ul, transaction.GetCommitUlong());
        using var cursor = transaction.CreateCursor();
        Assert.Equal(256, cursor.GetKeyValueCount([]));
    }

    sealed class Faults : HttpPipelineSynchronousPolicy
    {
        public bool Outage, Throttle, LoseAcquire, LoseSelection, LoseCommit, LoseTransfer;
        public override void OnSendingRequest(HttpMessage message)
        {
            if (Outage) throw new IOException("Injected storage outage.");
            if (Throttle) throw new RequestFailedException(503, "Injected server throttling.");
        }
        public override void OnReceivedResponse(HttpMessage message)
        {
            if (message.Response.Status is < 200 or >= 300) return;
            if (LoseAcquire && message.Request.Headers.TryGetValue("x-ms-lease-action", out var action) && action == "acquire")
            {
                LoseAcquire = false;
                throw new IOException("Lost acquire response after Azure applied it.");
            }
            if (LoseTransfer && message.Request.Headers.TryGetValue("x-ms-lease-action", out var transferAction) && transferAction == "change")
            {
                LoseTransfer = false;
                throw new IOException("Lost lease transfer response after Azure applied it.");
            }
            if (LoseSelection && message.Request.Headers.TryGetValue("If-Match", out _))
            {
                LoseSelection = false;
                throw new IOException("Lost selection response after Azure applied it.");
            }
            if (LoseCommit && message.Request.Method == RequestMethod.Put &&
                message.Request.Uri.ToUri().Query.Contains("comp=blocklist", StringComparison.Ordinal))
            {
                LoseCommit = false;
                throw new IOException("Lost canonical commit response after Azure applied it.");
            }
        }
    }

    [Fact]
    public async Task ServerThrottlingSurfacesAsRetryableIOExceptionFromListingsAndReads()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var raw = new AzureReplicationStorage(container, "db");
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await Open(local, capture, 1024);
        await Write(db, 1);
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"trl/{id}");
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        var kvi = (await container.GetBlobClient("db/files/2.kvi").UploadAsync(BinaryData.FromBytes(new byte[] { 1 }))).Value;
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("trl/1", 1));
        var storage = raw.Bind(inventory, authority);
        faults.Throttle = true;
        // Transient provider failures are ordinary retryable I/O for the coordinator, not host-stopping exceptions.
        await Assert.ThrowsAsync<IOException>(() => raw.ResolveRecoveryRootAsync(new("trl/1", 1), default).AsTask());
        await Assert.ThrowsAsync<IOException>(async () => { await foreach (var _ in storage.EnumerateAsync(default)) { } });
        await Assert.ThrowsAsync<IOException>(async () => { await foreach (var _ in storage.EnumerateMaintenanceAsync(default)) { } });
        await Assert.ThrowsAsync<IOException>(() => storage.ReadAsync(
            new RemoteFile(2, KVFileType.KeyIndex, 1, kvi.ETag.ToString(), true, null), 0, new byte[1], default).AsTask());
        faults.Throttle = false;
        Assert.Equal(1, await storage.ReadAsync(new RemoteFile(2, KVFileType.KeyIndex, 1, kvi.ETag.ToString(), true, null), 0,
            new byte[1], default));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PreparedLeaseTransferIsConfirmedByTargetRenewalEvenAfterLostResponse(bool loseResponse)
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var storage = new AzureLeaderStorage(container.GetBlobClient("cluster/leader.json"), TimeSpan.FromSeconds(15), Initial);
        Assert.Contains("\"term\":0", (await storage.ReadAsync(default)).Json); // Discovery bootstraps before acquisition.
        var old = new LeaseSessionController(storage, new Clock(), 0, TimeSpan.Zero);
        var authority = (await old.MaintainAsync())!;
        var handle = old.GetHandle(authority);
        var targetStorage = new AzureLeaderStorage(container.GetBlobClient("cluster/leader.json"), TimeSpan.FromSeconds(60), Initial);
        var targetClock = new Clock();
        var target = new LeaseSessionController(targetStorage, targetClock, 0, TimeSpan.Zero);
        var proposed = "b0000000-0000-0000-0000-000000000001";
        target.ProposeTransfer(proposed);
        faults.LoseTransfer = loseResponse;
        if (loseResponse) await Assert.ThrowsAsync<IOException>(() => old.TransferAsync(proposed, default).AsTask());
        else await old.TransferAsync(proposed, default);
        Assert.False(authority.IsValid);
        Assert.Null(await old.MaintainAsync());
        var received = await target.MaintainAsync();
        Assert.NotNull(received);
        Assert.Equal(proposed, target.GetHandle(received));
        Assert.True(received.Deadline - targetClock.Elapsed <= TimeSpan.FromSeconds(15));
        Assert.Null(await storage.RenewAsync(handle, default));
    }

    [Fact]
    public async Task SameNodeAcquiresAFreshLeaseAfterAnOutageLongerThanTheFiniteLease()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var storage = new AzureLeaderStorage(container.GetBlobClient("cluster/leader.json"), TimeSpan.FromSeconds(15), Initial);
        var leases = new LeaseSessionController(storage, new Clock(), 0, TimeSpan.FromMilliseconds(10));
        var first = (await leases.MaintainAsync())!;
        var firstHandle = leases.GetHandle(first);
        faults.Outage = true;
        await Assert.ThrowsAsync<IOException>(() => leases.MaintainAsync().AsTask());
        await Task.Delay(TimeSpan.FromSeconds(16));
        Assert.Null(leases.Current);
        faults.Outage = false;
        var next = await leases.MaintainAsync();
        Assert.NotNull(next);
        Assert.NotSame(first, next);
        Assert.NotEqual(firstHandle, leases.GetHandle(next));
        Assert.False(first.IsValid);
        Assert.True(next.IsValid);
        Assert.Null(await storage.RenewAsync(firstHandle, default));
    }

    [Fact]
    public async Task AcquireReconcilesLostResponseAndSelectionRequiresLeaseAndEtag()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var blob = container.GetBlobClient("cluster/leader.json");
        var storage = new AzureLeaderStorage(blob, TimeSpan.FromSeconds(15), Initial);
        faults.LoseAcquire = true;
        var lease = await storage.AcquireAsync(default);
        Assert.NotNull(lease);
        var competing = new AzureLeaderStorage(blob, TimeSpan.FromSeconds(15), Initial);
        Assert.Null(await competing.AcquireAsync(default));
        var record = await storage.ReadAsync(default);
        Assert.Equal(LeaderWriteOutcome.Rejected, await storage.WriteAsync(Guid.NewGuid().ToString(), record.Token, Initial, default));
        faults.LoseSelection = true;
        const string selected = """{"term":1,"sessionId":"selected"}""";
        Assert.Equal(LeaderWriteOutcome.Ambiguous, await storage.WriteAsync(lease.Handle, record.Token, selected, default));
        Assert.Equal(selected, (await storage.ReadAsync(default)).Json);
        Assert.Equal(LeaderWriteOutcome.Rejected, await storage.WriteAsync(lease.Handle, record.Token, Initial, default));
        faults.Outage = true;
        await Assert.ThrowsAsync<IOException>(() => storage.RenewAsync(lease.Handle, default).AsTask());
        faults.Outage = false;
        Assert.NotNull(await storage.RenewAsync(lease.Handle, default));
        await blob.GetBlobLeaseClient(lease.Handle).ReleaseAsync();
        var next = await competing.AcquireAsync(default);
        Assert.NotNull(next);
        Assert.NotEqual(lease.Handle, next.Handle);
        Assert.Null(await storage.RenewAsync(lease.Handle, default));
    }

    [Fact]
    public async Task UnifiedStorageBindingsKeepInventoryAndAuthorityIndependent()
    {
        var container = await fixture.ContainerAsync();
        var storage = new AzureReplicationStorage(container, "db");
        using var local = new InMemoryFileCollection();
        var trl = local.AddFile("trl");
        var writer = new MemWriter(trl.GetAppenderWriter());
        writer.WriteUInt8(1);
        writer.Flush();
        var created = await storage.WriteAsync(new(trl.Index, "trl/1", null, 0, 1, new(1), trl), default);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, new("trl/1", trl.Index));
        var restore = storage.Bind(inventory);
        Assert.Throws<ArgumentException>(() => new AzureReplicationStorage(container, "other").Bind(inventory));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var unused in storage.EnumerateAsync(default)) { }
        });
        var selected = new List<RemoteFile>();
        await foreach (var file in restore.EnumerateAsync(default)) selected.Add(file);
        var original = Assert.Single(selected);
        var bytes = new byte[1];
        Assert.Equal(1, await restore.ReadAsync(original, 0, bytes, default));
        Assert.Equal(1, bytes[0]);

        var oldAuthority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        oldAuthority.AcceptSuccess(oldAuthority.BeginRequest(), TimeSpan.FromMinutes(1));
        var oldSession = storage.Bind(inventory, oldAuthority);
        var source = new KeyIndexFileSource(trl.Index, KVFileType.PureValues, trl.GetSize(), 0, trl);
        await Assert.ThrowsAsync<InvalidOperationException>(() => restore.EnsurePureValuesAsync(2, source, default).AsTask());
        await oldSession.EnsurePureValuesAsync(2, source, default);
        oldAuthority.Fence();
        var newAuthority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        newAuthority.AcceptSuccess(newAuthority.BeginRequest(), TimeSpan.FromMinutes(1));
        IReplicationStorage newSession = storage.Bind(inventory, newAuthority);
        await newSession.EnsurePureValuesAsync(4, source, default);
        await Assert.ThrowsAsync<InvalidOperationException>(() => oldSession.EnsurePureValuesAsync(6, source, default).AsTask());
        Assert.False((await container.GetBlobClient("db/files/6.pvl").ExistsAsync()).Value);

        // Advancing canonical storage never silently advances an already selected restore snapshot.
        writer = new MemWriter(trl.GetAppenderWriter());
        writer.WriteUInt8(2);
        writer.Flush();
        Assert.Equal(TrlWriteOutcome.Applied, (await newSession.WriteAsync(
            new(trl.Index, "trl/1", created.State!.Token, 1, 2, new(1), trl), default)).Outcome);
        await Assert.ThrowsAsync<IOException>(() => restore.ReadAsync(original, 0, bytes, default).AsTask());
        var fresh = storage.Bind(await CanonicalTrlInventory.DiscoverAsync(storage, new("trl/1", trl.Index)));
        selected.Clear();
        await foreach (var file in fresh.EnumerateAsync(default)) selected.Add(file);
        var tail = Assert.Single(selected, file => file.FileType == KVFileType.TransactionLog);
        Assert.Equal(2ul, tail.Length);
        Assert.Equal(1, await fresh.ReadAsync(tail, 1, bytes, default));
        Assert.Equal(2, bytes[0]);
    }

    [Fact]
    public async Task ImmutablePureValuesReconcileSameShaAndFenceOnDifferentContent()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var leader = new AzureLeaderStorage(container.GetBlobClient("leader"), TimeSpan.FromSeconds(15), Initial);
        var leases = new LeaseSessionController(leader, new Clock(), 0, TimeSpan.Zero);
        var authority = (await leases.MaintainAsync())!;
        var canonical = new AzureReplicationStorage(container, "db");
        using var local = new InMemoryFileCollection();
        var trl = local.AddFile("trl");
        var writer = new MemWriter(trl.GetAppenderWriter());
        writer.WriteBlock(new byte[] { 1 });
        writer.Flush();
        await canonical.WriteAsync(new(trl.Index, "trl/1", null, 0, 1, new(1), trl), default);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(canonical, new("trl/1", trl.Index));
        var storage = canonical.Bind(inventory, authority);
        var pvl = local.AddFile("pvl");
        writer = new MemWriter(pvl.GetAppenderWriter());
        writer.WriteBlock(new byte[] { 2, 3, 4 });
        writer.Flush();
        var source = new KeyIndexFileSource(pvl.Index, KVFileType.PureValues, pvl.GetSize(), 0, pvl);
        faults.LoseCommit = true;
        await storage.EnsurePureValuesAsync(100, source, default);
        await storage.EnsurePureValuesAsync(100, source, default);
        RemoteFile? file = null;
        await foreach (var item in storage.EnumerateAsync(default)) if (item.FileId == 100) file = item;
        Assert.NotNull(file);
        Assert.NotNull(file.Sha256);
        var bytes = new byte[3];
        Assert.Equal(3, await storage.ReadAsync(file, 0, bytes, default));
        Assert.Equal(new byte[] { 2, 3, 4 }, bytes);
        var physical = new List<RemoteMaintenanceFile>();
        await foreach (var item in storage.EnumerateMaintenanceAsync(default)) physical.Add(item);
        Assert.Contains(physical, item => item.FileType == KVFileType.TransactionLog && !item.RetainForDiscovery);
        var oldVersion = await storage.ScheduleDeletionAsync(Assert.Single(physical, item => item.FileId == 100), TimeSpan.Zero, default);
        faults.LoseSelection = true;
        await Assert.ThrowsAsync<IOException>(() => storage.ProtectPureValuesAsync(100, source, default).AsTask());
        Assert.True(await storage.ProtectPureValuesAsync(100, source, default));
        Assert.DoesNotContain("btdb_delete_after", (await container.GetBlobClient("db/files/100.pvl").GetPropertiesAsync()).Value.Metadata.Keys);
        await storage.DeleteAsync(oldVersion, default);
        await storage.ScheduleDeletionAsync(oldVersion, TimeSpan.Zero, default); // Late marking cannot resurrect eligibility.
        Assert.DoesNotContain("btdb_delete_after", (await container.GetBlobClient("db/files/100.pvl").GetPropertiesAsync()).Value.Metadata.Keys);
        Assert.True((await container.GetBlobClient("db/files/100.pvl").ExistsAsync()).Value);
        physical.Clear();
        await foreach (var item in storage.EnumerateMaintenanceAsync(default)) physical.Add(item);
        var protectedVersion = Assert.Single(physical, item => item.FileId == 100);
        Assert.NotEqual(oldVersion.Version, protectedVersion.Version);
        protectedVersion = await storage.ScheduleDeletionAsync(protectedVersion, TimeSpan.Zero, default);
        await storage.DeleteAsync(protectedVersion, default);
        Assert.False(await storage.ProtectPureValuesAsync(100, source, default));
        // Recreate only for the independent content-conflict assertion below.
        await storage.EnsurePureValuesAsync(100, source, default);
        await foreach (var item in storage.EnumerateAsync(default)) if (item.FileId == 100) file = item;
        writer = new MemWriter(pvl.GetAppenderWriter());
        writer.WriteUInt8(5);
        writer.Flush();
        await Assert.ThrowsAsync<RemoteFileConflictException>(() => storage.EnsurePureValuesAsync(100,
            source with { Length = pvl.GetSize() }, default).AsTask());
        Assert.True(authority.IsFenced);
        await Assert.ThrowsAsync<InvalidOperationException>(() => storage.DeleteAsync(protectedVersion, default).AsTask());
        Assert.Equal(3, await storage.ReadAsync(file, 0, bytes, default));
        Assert.Equal(new byte[] { 2, 3, 4 }, bytes);
    }

    [Fact]
    public async Task CanonicalAppendAdoptionAndVersionBoundReadsUseAtomicAzureConditions()
    {
        var container = await fixture.ContainerAsync();
        var storage = new AzureReplicationStorage(container, "database");
        using var local = new InMemoryFileCollection();
        var file = local.AddFile("trl");
        var bytes = Enumerable.Range(0, 5 * 1024 * 1024).Select(i => (byte)i).ToArray();
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(bytes);
        writer.Flush();
        var first = await storage.WriteAsync(new(file.Index, "trl/1", null, 0, 4 * 1024 * 1024 + 17, new(1), file), default);
        Assert.Equal(TrlWriteOutcome.Applied, first.Outcome);
        var firstBlocks = await container.GetBlockBlobClient("database/trl/1").GetBlockListAsync(BlockListTypes.Committed);
        Assert.Equal(first.State!.Token.Trim('"'), firstBlocks.GetRawResponse().Headers.ETag?.ToString().Trim('"'));
        Assert.Equal((long)first.State.Length, firstBlocks.Value.CommittedBlocks.Sum(b => b.SizeLong));
        var second = await storage.WriteAsync(new(file.Index, "trl/1", first.State!.Token, first.State.Length,
            (uint)bytes.Length, new(1), file), default);
        Assert.Equal(TrlWriteOutcome.Applied, second.Outcome);
        var buffer = new byte[bytes.Length];
        await storage.ReadRangeAsync("trl/1", second.State!.Token, 0, buffer, default);
        Assert.Equal(bytes, buffer);
        await Assert.ThrowsAsync<IOException>(() => storage.ReadRangeAsync("trl/1", first.State.Token, 0, new byte[1], default).AsTask());
        var blocks = await container.GetBlockBlobClient("database/trl/1").GetBlockListAsync(BlockListTypes.Committed);
        Assert.Equal(2, blocks.Value.CommittedBlocks.Count());
        var adoption = await storage.WriteAsync(new(file.Index, "trl/1", second.State.Token, second.State.Length,
            second.State.Length, new(2), file), default);
        Assert.Equal(TrlWriteOutcome.Applied, adoption.Outcome);
        Assert.Equal(2ul, (await storage.ReadAsync("trl/1", default))!.Metadata.Term);
        Assert.Equal(TrlWriteOutcome.Rejected, (await storage.WriteAsync(new(file.Index, "trl/1", second.State.Token,
            second.State.Length, second.State.Length, new(1), file), default)).Outcome);
    }
}
