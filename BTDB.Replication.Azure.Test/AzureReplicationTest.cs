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

public class AzureReplicationTest(BlobStorageFixture fixture) : IClassFixture<BlobStorageFixture>
{
    internal const string Initial = """{"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """;

    internal sealed class Clock : IReplicationScheduler
    {
        readonly System.Diagnostics.Stopwatch _clock = System.Diagnostics.Stopwatch.StartNew();
        public TimeSpan Elapsed => _clock.Elapsed;
        public IDisposable Schedule(TimeSpan delay, Action callback, string description) => throw new NotSupportedException();
    }

    internal static async Task<BTreeKeyValueDB> Open(InMemoryReplicationFileStorage files, TransactionLogCapture capture, uint splitSize = int.MaxValue)
    {
        var file = files.AddFile("trl", FileIdParity.Odd);
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock("BTDB3"u8);
        writer.WriteGuid(new Guid("0ea12e80-f23f-4d46-b23e-abd3f8288f60"));
        writer.WriteUInt8((byte)KVFileType.TransactionLog);
        writer.WriteVInt64(0); // Native replication header.
        writer.WriteVInt32(0);
        writer.Flush();
        file.HardFlush();
        return await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new BTDB.Replication.Test.LocalReplicatedCollection(files), TransactionLogCapture = capture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null, FileSplitSize = splitSize
        });
    }

    internal static async Task Write(BTreeKeyValueDB db, ulong id)
    {
        using var transaction = await db.StartWritingTransaction(id);
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([(byte)id], [(byte)id]);
        transaction.Commit();
    }

    [Fact]
    public async Task SuccessorNameResolvesItsIdAcrossALegacyTransitionWithoutIdMetadata()
    {
        var container = await fixture.ContainerAsync(new Faults());
        var storage = new AzureReplicationStorage(container, "metadata");
        using var files = new InMemoryReplicationFileStorage();
        var source = files.ImportFile(1, "trl");
        var writer = new MemWriter(source.GetAppenderWriter());
        writer.WriteUInt8(1);
        writer.Flush();
        Assert.Equal(TrlWriteOutcome.Applied,
            (await storage.WriteAsync(new(103, "103.trl", null, 0, 1, source), default)).Outcome);
        Assert.Equal(TrlWriteOutcome.Applied,
            (await storage.WriteAsync(new(1, "1.trl", null, 0, 1, source), default)).Outcome);
        var root = (await container.GetBlobClient("metadata/1.trl").GetPropertiesAsync()).Value;
        Assert.Empty(root.Metadata);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", 1));
        Assert.Equal(103u, inventory.Tail.FileId);
        Assert.Equal("103.trl", inventory.Tail.Key);
        var target = (await container.GetBlobClient("metadata/103.trl").GetPropertiesAsync()).Value;
        Assert.Empty(target.Metadata);
        var listed = new List<RemoteMaintenanceFile>();
        await foreach (var file in storage.EnumerateMaintenanceAsync(default)) listed.Add(file);
        Assert.Equal(new uint[] { 1, 103 }, listed.Select(f => f.FileId).Order());
        await Assert.ThrowsAsync<FormatException>(() =>
            storage.WriteAsync(new(105, "103.trl", null, 0, 1, source), default).AsTask());
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
            id => $"{id}.trl");
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
            [new("main", next, nextCapture, canonical, new("1.trl", 1), new(1, 0), id => $"{id}.trl")]);
        faults.LoseCommit = loseAdoptionReply;
        var publishers = (await session.ActivateAsync())!;
        using var publisher = Assert.Single(publishers);
        Assert.Equal(published, publisher.PublishedPosition);
        Assert.NotNull(publisher.Tail);
        Assert.Same(publishers, await session.ActivateAsync());
        Assert.Equal(TrlPublishResult.AuthorityLost, await oldPublisher.PublishNextAsync());
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        using var restoredFiles = new InMemoryReplicationFileStorage();
        var inventory = await CanonicalTrlInventory.DiscoverAsync(canonical, new("1.trl", 1));
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
    [InlineData("2.pvl", KVFileType.PureValues)]
    [InlineData("2.kvi", KVFileType.KeyIndex)]
    [InlineData("2.trl", KVFileType.TransactionLog)]
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
        await raw.WriteAsync(new(1, "1.trl", null, 0, 1, source), default);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1));
        var storage = raw.Bind(inventory, authority);
        var blob = container.GetBlobClient("db/" + key);
        var metadata = type == KVFileType.TransactionLog
            ? new Dictionary<string, string>()
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
        var restarted = restartedRaw.Bind(await CanonicalTrlInventory.DiscoverAsync(restartedRaw, new("1.trl", 1)), authority);
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
        await container.GetBlobClient("db/2.kvi").UploadAsync(BinaryData.FromBytes(new byte[] { 1 }));
        var storage = new AzureReplicationStorage(container, "db");
        await Assert.ThrowsAsync<IOException>(() => storage.ResolveRecoveryRootAsync(new("1.trl", 1), default).AsTask());
    }

    [Fact]
    public async Task CheckpointRootRestoresNativeDatabaseAfterObsoleteGenesisTrlsAreDeleted()
    {
        var container = await fixture.ContainerAsync();
        var time = new CleanupTime();
        var raw = new AzureReplicationStorage(container, "db", time);
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await Open(local, capture, 1024);
        for (ulong i = 1; i <= 400; i++) await Write(db, i);
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"{id}.trl");
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1));
        var storage = raw.Bind(inventory, authority);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        Assert.True(snapshot.TransactionLogFileId > 1);
        await storage.PublishKeyIndexAsync(10000, snapshot, new Dictionary<uint, uint>(), default);
        var properties = (await container.GetBlobClient("db/10000.kvi").GetPropertiesAsync()).Value;
        Assert.Equal("btdb_sha256", Assert.Single(properties.Metadata).Key);
        var gc = new RemoteGarbageCollector(storage, authority, TimeSpan.FromDays(1));
        var checkpoint = new PublishedCheckpoint(10000, snapshot.TransactionLogFileId,
            snapshot.Sources.Select(s => s.FileId).ToHashSet());
        await gc.CollectAsync(checkpoint, default);
        Assert.NotNull(await raw.ReadAsync("1.trl", default)); // Marked, not yet due.
        time.Now += TimeSpan.FromDays(1);
        await gc.CollectAsync(checkpoint, default);
        Assert.Null(await raw.ReadAsync("1.trl", default));
        var fresh = new AzureReplicationStorage(container, "db");
        var recovered = await CanonicalTrlInventory.DiscoverAsync(fresh, new("1.trl", 1));
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
        public long StagedBytes;
        public int BlockListReads;
        public override void OnSendingRequest(HttpMessage message)
        {
            var query = message.Request.Uri.ToUri().Query;
            if (message.Request.Method == RequestMethod.Get && query.Contains("comp=blocklist", StringComparison.Ordinal))
                BlockListReads++;
            if (message.Request.Method == RequestMethod.Put && query.Contains("comp=block", StringComparison.Ordinal) &&
                !query.Contains("comp=blocklist", StringComparison.Ordinal) && message.Request.Content != null &&
                message.Request.Content.TryComputeLength(out var length))
                StagedBytes += length;
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
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"{id}.trl");
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        var kvi = (await container.GetBlobClient("db/2.kvi").UploadAsync(BinaryData.FromBytes(new byte[] { 1 }))).Value;
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1));
        var storage = raw.Bind(inventory, authority);
        faults.Throttle = true;
        // Transient provider failures are ordinary retryable I/O for the coordinator, not host-stopping exceptions.
        await Assert.ThrowsAsync<IOException>(() => raw.ResolveRecoveryRootAsync(new("1.trl", 1), default).AsTask());
        await Assert.ThrowsAsync<IOException>(async () => { await foreach (var _ in storage.EnumerateAsync(default)) { } });
        await Assert.ThrowsAsync<IOException>(async () => { await foreach (var _ in storage.EnumerateMaintenanceAsync(default)) { } });
        await Assert.ThrowsAsync<IOException>(() => storage.ReadAsync(
            new RemoteFile(2, KVFileType.KeyIndex, 1, kvi.ETag.ToString(), true, null), 0, new byte[1], default).AsTask());
        faults.Throttle = false;
        Assert.Equal(1, await storage.ReadAsync(new RemoteFile(2, KVFileType.KeyIndex, 1, kvi.ETag.ToString(), true, null), 0,
            new byte[1], default));
    }

    [Fact]
    public async Task EmptyPrefixIsRejectedBecauseCleanupListsTheWholeNamespace()
    {
        var container = await fixture.ContainerAsync();
        Assert.Throws<ArgumentException>(() => new AzureReplicationStorage(container, ""));
        Assert.Throws<ArgumentException>(() => new AzureReplicationStorage(container, "/"));
    }

    [Fact]
    public async Task RepeatedAppendsReuseTheirOwnCommittedBlockList()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var raw = new AzureReplicationStorage(container, "db");
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await Open(local, capture);
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"{id}.trl");
        for (ulong id = 1; id <= 5; id++)
        {
            await Write(db, id);
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        }
        Assert.Equal(0, faults.BlockListReads); // Each append expects this adapter's own last commit.
        var remote = (await container.GetBlockBlobClient("db/1.trl").DownloadContentAsync()).Value.Content.ToArray();
        var expected = new byte[publisher.PublishedPosition.Offset];
        local.GetFile(publisher.PublishedPosition.FileId)!.RandomRead(expected, 0, false);
        Assert.Equal(expected, remote);
    }

    [Fact]
    public async Task CheckpointClearsDeletionMarksWithoutChangingUnmarkedTrlVersions()
    {
        var container = await fixture.ContainerAsync();
        var raw = new AzureReplicationStorage(container, "db");
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await Open(local, capture, 1024);
        // Values above the inline limit keep every TRL referenced, so the whole sealed chain is a dependency.
        for (ulong i = 1; i <= 40; i++)
        {
            using var transaction = await db.StartWritingTransaction(i);
            using var cursor = transaction.CreateCursor();
            cursor.CreateOrUpdateKeyValue([(byte)i], new byte[100]);
            transaction.Commit();
        }
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"{id}.trl");
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1));
        var storage = raw.Bind(inventory, authority);
        var trls = new List<RemoteMaintenanceFile>();
        await foreach (var file in storage.EnumerateMaintenanceAsync(default))
            if (file.FileType == KVFileType.TransactionLog) trls.Add(file);
        Assert.True(trls.Count > 3, $"{trls.Count} TRLs");
        var marked = await storage.ScheduleDeletionAsync(trls[1], TimeSpan.FromDays(1), default);
        using var snapshot = db.CaptureKeyIndexSnapshot();
        await storage.PublishKeyIndexAsync(10000, snapshot, new Dictionary<uint, uint>(), default);
        var properties = (await container.GetBlobClient("db/10000.kvi").GetPropertiesAsync()).Value;
        Assert.Equal("btdb_sha256", Assert.Single(properties.Metadata).Key);
        await foreach (var file in storage.EnumerateMaintenanceAsync(default))
        {
            if (file.FileType != KVFileType.TransactionLog) continue;
            Assert.Null(file.DeleteAfter);
            if (file.Key == marked.Key) Assert.NotEqual(marked.Version, file.Version);
            else Assert.Equal(trls.Single(t => t.Key == file.Key).Version, file.Version);
        }
    }

    [Fact]
    public async Task SmallTrlAppendsStageOnlyTheirSuffixAndMergeTrailingBlocksOccasionally()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var raw = new AzureReplicationStorage(container, "db");
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await Open(local, capture);
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"{id}.trl");
        await Write(db, 1);
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        var blob = container.GetBlockBlobClient("db/1.trl");
        var merges = 0;
        for (ulong id = 2; id <= 140; id++)
        {
            var before = publisher.PublishedPosition.Offset;
            await Write(db, id);
            faults.StagedBytes = 0;
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
            var appended = publisher.PublishedPosition.Offset - before;
            if (faults.StagedBytes != appended)
            {
                merges++;
                Assert.Equal(publisher.PublishedPosition.Offset, faults.StagedBytes); // One merged block restaged.
            }
            var blocks = (await blob.GetBlockListAsync(BlockListTypes.Committed)).Value.CommittedBlocks.Count();
            Assert.InRange(blocks, 1, 64);
        }
        Assert.InRange(merges, 1, 3);
        var remote = (await blob.DownloadContentAsync()).Value.Content.ToArray();
        var expected = new byte[publisher.PublishedPosition.Offset];
        local.GetFile(publisher.PublishedPosition.FileId)!.RandomRead(expected, 0, false);
        Assert.Equal(expected, remote);
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
        var created = await storage.WriteAsync(new(trl.Index, "1.trl", null, 0, 1, trl), default);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", trl.Index));
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
        Assert.False((await container.GetBlobClient("db/6.pvl").ExistsAsync()).Value);

        // Advancing canonical storage never silently advances an already selected restore snapshot, but the snapshot
        // keeps reading its selected prefix from the appended (append-only) version.
        writer = new MemWriter(trl.GetAppenderWriter());
        writer.WriteUInt8(2);
        writer.Flush();
        Assert.Equal(TrlWriteOutcome.Applied, (await newSession.WriteAsync(
            new(trl.Index, "1.trl", created.State!.Token, 1, 2, trl), default)).Outcome);
        bytes[0] = 0;
        Assert.Equal(1, await restore.ReadAsync(original, 0, bytes, default));
        Assert.Equal(1, bytes[0]);
        Assert.Equal(0, await restore.ReadAsync(original, 1, bytes, default));
        var fresh = storage.Bind(await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", trl.Index)));
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
        await canonical.WriteAsync(new(trl.Index, "1.trl", null, 0, 1, trl), default);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(canonical, new("1.trl", trl.Index));
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
        Assert.DoesNotContain("btdb_delete_after", (await container.GetBlobClient("db/100.pvl").GetPropertiesAsync()).Value.Metadata.Keys);
        await storage.DeleteAsync(oldVersion, default);
        await storage.ScheduleDeletionAsync(oldVersion, TimeSpan.Zero, default); // Late marking cannot resurrect eligibility.
        Assert.DoesNotContain("btdb_delete_after", (await container.GetBlobClient("db/100.pvl").GetPropertiesAsync()).Value.Metadata.Keys);
        Assert.True((await container.GetBlobClient("db/100.pvl").ExistsAsync()).Value);
        // Unmarked: protection keeps the version, so restores reading it by ETag are not interrupted.
        var unmarked = (await container.GetBlobClient("db/100.pvl").GetPropertiesAsync()).Value.ETag;
        Assert.True(await storage.ProtectPureValuesAsync(100, source, default));
        Assert.Equal(unmarked, (await container.GetBlobClient("db/100.pvl").GetPropertiesAsync()).Value.ETag);
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
        var first = await storage.WriteAsync(new(file.Index, "1.trl", null, 0, 4 * 1024 * 1024 + 17, file), default);
        Assert.Equal(TrlWriteOutcome.Applied, first.Outcome);
        var firstBlocks = await container.GetBlockBlobClient("database/1.trl").GetBlockListAsync(BlockListTypes.Committed);
        Assert.Equal(first.State!.Token.Trim('"'), firstBlocks.GetRawResponse().Headers.ETag?.ToString().Trim('"'));
        Assert.Equal((long)first.State.Length, firstBlocks.Value.CommittedBlocks.Sum(b => b.SizeLong));
        var second = await storage.WriteAsync(new(file.Index, "1.trl", first.State!.Token, first.State.Length,
            (uint)bytes.Length, file), default);
        Assert.Equal(TrlWriteOutcome.Applied, second.Outcome);
        var buffer = new byte[bytes.Length];
        await storage.ReadRangeAsync("1.trl", second.State!.Token, 0, buffer, default);
        Assert.Equal(bytes, buffer);
        await Assert.ThrowsAsync<IOException>(() => storage.ReadRangeAsync("1.trl", first.State.Token, 0, new byte[1], default).AsTask());
        var blocks = await container.GetBlockBlobClient("database/1.trl").GetBlockListAsync(BlockListTypes.Committed);
        // The partial 17-byte block is kept and only the appended suffix is staged.
        Assert.Equal(new long[] { 4 * 1024 * 1024, 17, bytes.Length - 4 * 1024 * 1024 - 17 },
            blocks.Value.CommittedBlocks.Select(b => b.SizeLong));
        var adoption = await storage.WriteAsync(new(file.Index, "1.trl", second.State.Token, second.State.Length,
            second.State.Length, file), default);
        Assert.Equal(TrlWriteOutcome.Applied, adoption.Outcome);
        Assert.NotEqual(second.State.Token, (await storage.ReadAsync("1.trl", default))!.Token);
        Assert.Equal(TrlWriteOutcome.Rejected, (await storage.WriteAsync(new(file.Index, "1.trl", second.State.Token,
            second.State.Length, second.State.Length, file), default)).Outcome);
    }

    [Fact]
    public async Task SealingWriteStoresTheTrlChecksumAndAnyLaterWriteClearsIt()
    {
        var container = await fixture.ContainerAsync();
        var storage = new AzureReplicationStorage(container, "database");
        using var local = new InMemoryFileCollection();
        var file = local.AddFile("trl");
        var bytes = Enumerable.Range(0, 1000).Select(i => (byte)i).ToArray();
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(bytes);
        writer.Flush();
        var sha = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(bytes));
        var created = await storage.WriteAsync(new(file.Index, "1.trl", null, 0, 600, file), default);
        Assert.Null(created.State!.Sha256);
        var sealedWrite = await storage.WriteAsync(new(file.Index, "1.trl", created.State.Token, 600, 1000, file, sha), default);
        Assert.Equal(sha, sealedWrite.State!.Sha256);
        Assert.Equal(sha, (await storage.ReadAsync("1.trl", default))!.Sha256);
        await foreach (var head in storage.EnumerateTrlsAsync(default)) Assert.Equal(sha, head.State.Sha256);
        // An adoption that does not seal again leaves no checksum behind.
        var adopted = await storage.WriteAsync(new(file.Index, "1.trl", sealedWrite.State.Token, 1000, 1000, file), default);
        Assert.Equal(TrlWriteOutcome.Applied, adopted.Outcome);
        Assert.Null((await storage.ReadAsync("1.trl", default))!.Sha256);
    }
}
