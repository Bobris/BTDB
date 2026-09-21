using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

    static async Task<BTreeKeyValueDB> Open(InMemoryReplicationFileStorage files, TransactionLogCapture capture)
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
            Compression = new NoCompressionStrategy(), CompactorScheduler = null
        });
    }

    static async Task Write(BTreeKeyValueDB db, ulong id)
    {
        using var transaction = await db.StartWritingTransaction(id);
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([(byte)id], [(byte)id]);
        transaction.Commit();
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
        var canonical = new AzureCanonicalTrlStorage(container, "main");
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
        var checkpoints = new AzureCheckpointStorage(container, "main", inventory, nextAuthority);
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

    sealed class Faults : HttpPipelineSynchronousPolicy
    {
        public bool Outage, LoseAcquire, LoseSelection, LoseCommit;
        public override void OnSendingRequest(HttpMessage message)
        {
            if (Outage) throw new IOException("Injected storage outage.");
        }
        public override void OnReceivedResponse(HttpMessage message)
        {
            if (message.Response.Status is < 200 or >= 300) return;
            if (LoseAcquire && message.Request.Headers.TryGetValue("x-ms-lease-action", out var action) && action == "acquire")
            {
                LoseAcquire = false;
                throw new IOException("Lost acquire response after Azure applied it.");
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
    public async Task ImmutablePureValuesReconcileSameShaAndFenceOnDifferentContent()
    {
        var faults = new Faults();
        var container = await fixture.ContainerAsync(faults);
        var leader = new AzureLeaderStorage(container.GetBlobClient("leader"), TimeSpan.FromSeconds(15), Initial);
        var leases = new LeaseSessionController(leader, new Clock(), 0, TimeSpan.Zero);
        var authority = (await leases.MaintainAsync())!;
        var canonical = new AzureCanonicalTrlStorage(container, "db");
        using var local = new InMemoryFileCollection();
        var trl = local.AddFile("trl");
        var writer = new MemWriter(trl.GetAppenderWriter());
        writer.WriteBlock(new byte[] { 1 });
        writer.Flush();
        await canonical.WriteAsync(new(trl.Index, "trl/1", null, 0, 1, new(1), trl), default);
        var inventory = await CanonicalTrlInventory.DiscoverAsync(canonical, new("trl/1", trl.Index));
        var storage = new AzureCheckpointStorage(container, "db", inventory, authority);
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
        writer = new MemWriter(pvl.GetAppenderWriter());
        writer.WriteUInt8(5);
        writer.Flush();
        await Assert.ThrowsAsync<RemoteFileConflictException>(() => storage.EnsurePureValuesAsync(100,
            source with { Length = pvl.GetSize() }, default).AsTask());
        Assert.True(authority.IsFenced);
        Assert.Equal(3, await storage.ReadAsync(file, 0, bytes, default));
        Assert.Equal(new byte[] { 2, 3, 4 }, bytes);
    }

    [Fact]
    public async Task CanonicalAppendAdoptionAndVersionBoundReadsUseAtomicAzureConditions()
    {
        var container = await fixture.ContainerAsync();
        var storage = new AzureCanonicalTrlStorage(container, "database");
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
