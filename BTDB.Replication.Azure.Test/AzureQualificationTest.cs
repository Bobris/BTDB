using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Core.Pipeline;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;
using Xunit.Abstractions;
using static BTDB.Replication.Azure.Test.AzureReplicationTest;

namespace BTDB.Replication.Azure.Test;

/// <summary>Provider qualification of lease expiry, delayed requests and publication/cleanup/restore races. The same
/// tests run against Azurite and, with BTDB_AZURE_BLOB_ENDPOINT set, against live Azure, whose results are the
/// provider evidence (see Testing.md).</summary>
public class AzureQualificationTest(BlobStorageFixture fixture, ITestOutputHelper output) : IClassFixture<BlobStorageFixture>
{
    static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(15);
    const int DriftPpm = 1000;
    static readonly TimeSpan Margin = TimeSpan.FromMilliseconds(250);

    /// <summary>Holds armed requests before they reach the service until released, like a request stuck in the network
    /// or behind a paused process, and optionally delays delivery of matching responses.</summary>
    sealed class Hold : HttpPipelinePolicy
    {
        public sealed class Request(Func<HttpMessage, bool> match)
        {
            internal readonly Func<HttpMessage, bool> Match = match;
            readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
            readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task Reached => _reached.Task;
            public void Release() => _released.TrySetResult();
            internal Task Wait() { _reached.TrySetResult(); return _released.Task; }
        }

        readonly List<Request> _armed = new();
        public Func<HttpMessage, bool>? DelayResponse;
        public TimeSpan ResponseDelay;

        public Request Arm(Func<HttpMessage, bool> match)
        {
            var request = new Request(match);
            lock (_armed) _armed.Add(request);
            return request;
        }

        public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            Request? held;
            lock (_armed)
            {
                held = _armed.FirstOrDefault(r => r.Match(message));
                if (held != null) _armed.Remove(held);
            }
            if (held != null) await held.Wait();
            await ProcessNextAsync(message, pipeline);
            if (DelayResponse?.Invoke(message) == true) await Task.Delay(ResponseDelay);
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline) =>
            throw new NotSupportedException("The adapters use only asynchronous requests.");
    }

    static Func<HttpMessage, bool> LeaseAction(string action) => message =>
        message.Request.Headers.TryGetValue("x-ms-lease-action", out var value) && value == action;

    static bool Query(HttpMessage message, string component) =>
        message.Request.Uri.ToUri().Query.Contains("comp=" + component, StringComparison.Ordinal);

    static bool IsBlockListCommit(HttpMessage message) =>
        message.Request.Method == RequestMethod.Put && Query(message, "blocklist");

    static Func<HttpMessage, bool> Path(string suffix, RequestMethod method) => message =>
        message.Request.Method == method && message.Request.Uri.ToUri().AbsolutePath.EndsWith(suffix, StringComparison.Ordinal);

    static LeaseAuthority Held(IReplicationScheduler clock)
    {
        var authority = new LeaseAuthority(clock, 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        return authority;
    }

    /// <summary>B1 lease margins: once a holder stops renewing, its conservative local deadline passes while the
    /// service still reports the lease held; a delayed acquire response never moves that deadline later. The output
    /// records how long the service kept each lease after the holder dispatched its acquire.</summary>
    [Fact]
    public async Task LocalLeaseDeadlineEndsWhileTheServiceStillHoldsTheLease()
    {
        var trials = await Task.WhenAll(Enumerable.Range(0, 6).Select(i =>
            ExpiryTrial(i % 2 == 0 ? TimeSpan.Zero : TimeSpan.FromSeconds(3))));
        foreach (var (delay, held, released) in trials)
            output.WriteLine($"acquire reply delay {delay.TotalSeconds:0} s: service lease held at least {held.TotalMilliseconds:0} ms " +
                             $"and at most {released.TotalMilliseconds:0} ms after dispatch");
    }

    async Task<(TimeSpan Delay, TimeSpan Held, TimeSpan Released)> ExpiryTrial(TimeSpan acquireReplyDelay)
    {
        var hold = new Hold { DelayResponse = LeaseAction("acquire"), ResponseDelay = acquireReplyDelay };
        var container = await fixture.ContainerAsync(hold);
        var storage = new AzureLeaderStorage(container.GetBlobClient("leader.json"), LeaseDuration, Initial);
        await storage.ReadAsync(default); // Create the record first, so dispatch measures only the acquire.
        var clock = new Clock();
        var leases = new LeaseSessionController(storage, clock, DriftPpm, Margin);
        var dispatched = clock.Elapsed;
        var authority = (await leases.MaintainAsync())!;
        var handle = leases.GetHandle(authority);
        Assert.True(authority.Deadline - dispatched <= LeaseDuration - Margin);
        Assert.True(authority.Deadline - clock.Elapsed <= LeaseDuration - acquireReplyDelay - Margin);
        var competing = new AzureLeaderStorage(fixture.Connect(container.Name).GetBlobClient("leader.json"),
            LeaseDuration, Initial);
        var lastRejected = TimeSpan.Zero;
        while (true)
        {
            var attempt = clock.Elapsed;
            var grant = await competing.AcquireAsync(default);
            if (grant != null) break;
            lastRejected = attempt;
            await Task.Delay(50);
        }
        var released = clock.Elapsed;
        // The service answered "held" to a request dispatched at lastRejected, so its lease was still held then.
        Assert.True(authority.Deadline <= lastRejected,
            $"Local deadline {authority.Deadline} passed after the service lease, last seen held at {lastRejected}.");
        Assert.False(authority.IsValid);
        Assert.Null(leases.Current);
        Assert.Null(await storage.RenewAsync(handle, default));
        return (acquireReplyDelay, lastRejected - dispatched, released - dispatched);
    }

    /// <summary>Hands out tokens that look almost expired, so the SDK acquires a fresh token for nearly every request:
    /// what renewals see around every managed-identity token refresh, compressed into one lease period.</summary>
    sealed class ShortLivedTokens(TokenCredential inner) : TokenCredential
    {
        public int Acquisitions;
        public override AccessToken GetToken(TokenRequestContext context, CancellationToken cancellation) =>
            Shorten(inner.GetToken(context, cancellation));
        public override async ValueTask<AccessToken> GetTokenAsync(TokenRequestContext context, CancellationToken cancellation) =>
            Shorten(await inner.GetTokenAsync(context, cancellation));
        AccessToken Shorten(AccessToken token)
        {
            Interlocked.Increment(ref Acquisitions);
            return new(token.Token, DateTimeOffset.UtcNow.AddSeconds(30));
        }
    }

    /// <summary>Live Azure only: lease renewals keep one authority for several lease periods while the SDK refreshes
    /// the managed-identity token continuously on the renewal path.</summary>
    [Fact]
    public async Task LeaseAuthoritySurvivesContinuousTokenRefresh()
    {
        if (!fixture.IsLive) return;
        var created = await fixture.ContainerAsync();
        ShortLivedTokens? tokens = null;
        var container = fixture.Connect(created.Name, wrapCredential: inner => tokens = new(inner));
        var storage = new AzureLeaderStorage(container.GetBlobClient("leader.json"), LeaseDuration, Initial);
        var leases = new LeaseSessionController(storage, new SystemReplicationScheduler(), DriftPpm, Margin);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(50));
        LeaseAuthority? first = null;
        var lost = false;
        var run = leases.RunAsync(TimeSpan.FromMilliseconds(250), current =>
        {
            if (first == null) first = current;
            else if (!ReferenceEquals(first, current)) lost = true;
        }, stop.Token, TimeSpan.FromSeconds(2));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.NotNull(first);
        Assert.False(lost, "Lease authority changed while tokens were refreshed.");
        output.WriteLine($"{tokens!.Acquisitions} token acquisitions during about 3 lease periods");
        Assert.True(tokens.Acquisitions > 3);
    }

    /// <summary>B5 stale in-flight requests: a leader dispatches a TRL append, a renewal and an application-data
    /// write while its authority is valid, then stalls past lease expiry. A successor acquires, selects and adopts;
    /// every delayed request then reaches Azure and is rejected by its lease or ETag condition.</summary>
    [Fact]
    public async Task DelayedPredecessorRequestsAreRejectedAfterTakeover()
    {
        var hold = new Hold();
        var container = await fixture.ContainerAsync(hold);
        var successorContainer = fixture.Connect(container.Name);
        var storage = new AzureLeaderStorage(container.GetBlobClient("leader.json"), LeaseDuration, Initial);
        var leases = new LeaseSessionController(storage, new Clock(), DriftPpm, Margin);
        var authority = (await leases.MaintainAsync())!;
        var selected = (await new LeaderSelection(storage, leases, authority,
            new("cluster", "first", "session1", 1, ["main"], "local://first", "key1")).SelectAsync())!;
        using var firstFiles = new InMemoryReplicationFileStorage();
        using var nextFiles = new InMemoryReplicationFileStorage();
        var firstCapture = new TransactionLogCapture();
        var nextCapture = new TransactionLogCapture();
        using var first = await Open(firstFiles, firstCapture);
        using var next = await Open(nextFiles, nextCapture);
        using var oldPublisher = new CanonicalTrlPublisher(first, firstCapture,
            new AzureReplicationStorage(container, "main"), authority, selected.Term, id => $"{id}.trl");
        await Write(first, 1);
        await Write(next, 1);
        Assert.Equal(TrlPublishResult.Published, await oldPublisher.PublishNextAsync());
        var published = oldPublisher.PublishedPosition;
        var data = new ReplicationApplicationData(storage, "cluster", leases, () => selected);
        var snapshot = await data.ReadAsync();
        await Write(first, 2);
        await Write(next, 2);

        var append = hold.Arm(IsBlockListCommit);
        var staleAppend = oldPublisher.PublishNextAsync().AsTask();
        await append.Reached;
        var renewal = hold.Arm(LeaseAction("renew"));
        var staleRenewal = storage.RenewAsync(leases.GetHandle(authority), default).AsTask();
        await renewal.Reached;
        var write = hold.Arm(message => Path("/leader.json", RequestMethod.Put)(message) && !Query(message, "lease"));
        var staleWrite = data.TryWriteAsync(snapshot, new JsonObject { ["stale"] = true }).AsTask();
        await write.Reached;

        var successorStorage = new AzureLeaderStorage(successorContainer.GetBlobClient("leader.json"), LeaseDuration, Initial);
        var nextLeases = new LeaseSessionController(successorStorage, new Clock(), DriftPpm, Margin);
        LeaseAuthority? nextAuthority;
        while ((nextAuthority = await nextLeases.MaintainAsync()) == null) await Task.Delay(250);
        Assert.False(authority.IsValid);
        var session = new LeadershipSession(new LeaderSelection(successorStorage, nextLeases, nextAuthority,
                new("cluster", "next", "session2", 1, ["main"], "local://next", "key2")),
            [new("main", next, nextCapture, new AzureReplicationStorage(successorContainer, "main"), new("1.trl", 1),
                new(1, 0), id => $"{id}.trl")]);
        using var publisher = Assert.Single((await session.ActivateAsync())!);
        Assert.Equal(published, publisher.PublishedPosition);

        append.Release();
        renewal.Release();
        write.Release();
        Assert.Contains(await staleAppend, new[] { TrlPublishResult.Conflict, TrlPublishResult.AuthorityLost });
        Assert.Null(await staleRenewal);
        Assert.Equal(LeaderWriteOutcome.Rejected, await staleWrite);
        var record = JsonNode.Parse((await successorStorage.ReadAsync(default)).Json)!;
        Assert.Equal("session2", record["sessionId"]!.GetValue<string>());
        Assert.Null(record["applicationData"]?["stale"]);
        var canonical = new AzureReplicationStorage(successorContainer, "main");
        Assert.Equal(published.Offset, (await canonical.ReadAsync("1.trl", default))!.Length);
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        Assert.Equal(2ul, await RestoredCommit(canonical));
        leases.Close();
        nextLeases.Close();
    }

    static async Task<ulong> RestoredCommit(AzureReplicationStorage raw)
    {
        using var cache = new InMemoryReplicationFileStorage();
        await using var files = new ReplicationFileSet(cache, raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1))));
        await files.InitializeAsync();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            { FileCollection = files, CompactorScheduler = null, Compression = new NoCompressionStrategy() });
        using var transaction = db.StartReadOnlyTransaction();
        return transaction.GetCommitUlong();
    }

    static async Task<(AzureReplicationStorage Storage, InMemoryFileCollection Local)> PureValuesNamespace(
        global::Azure.Storage.Blobs.BlobContainerClient container, LeaseAuthority authority)
    {
        var raw = new AzureReplicationStorage(container, "db");
        var local = new InMemoryFileCollection();
        var trl = local.AddFile("trl");
        var writer = new MemWriter(trl.GetAppenderWriter());
        writer.WriteUInt8(1);
        writer.Flush();
        await raw.WriteAsync(new(trl.Index, "1.trl", null, 0, 1, trl), default);
        return (raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", trl.Index)), authority), local);
    }

    static KeyIndexFileSource PureValues(InMemoryFileCollection local, byte content)
    {
        var file = local.AddFile("pvl");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(Enumerable.Repeat(content, 1000).ToArray());
        writer.Flush();
        return new(file.Index, KVFileType.PureValues, file.GetSize(), 0, file);
    }

    /// <summary>B5 stale PVL creation: a fenced predecessor's upload at the identity a successor also chose. Whichever
    /// lands second finds different content and fences its own session; the object keeps the first content.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StalePureValuesUploadFencesWhicheverSessionLandsSecond(bool predecessorFirst)
    {
        var hold = new Hold();
        var container = await fixture.ContainerAsync(hold);
        var clock = new Clock();
        var old = Held(clock);
        var (oldStorage, oldLocal) = await PureValuesNamespace(container, old);
        using var oldFiles = oldLocal;
        var successor = Held(clock);
        var raw = new AzureReplicationStorage(fixture.Connect(container.Name), "db");
        var storage = raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1)), successor);
        using var local = new InMemoryFileCollection();
        var upload = hold.Arm(message => IsBlockListCommit(message) && Path("/db/100.pvl", RequestMethod.Put)(message));
        var stale = oldStorage.EnsurePureValuesAsync(100, PureValues(oldLocal, 1), default).AsTask();
        await upload.Reached;
        old.Fence(); // The predecessor loses authority while its request is in flight.
        if (predecessorFirst)
        {
            upload.Release();
            await stale;
            await Assert.ThrowsAsync<RemoteFileConflictException>(() =>
                storage.EnsurePureValuesAsync(100, PureValues(local, 2), default).AsTask());
            Assert.True(successor.IsFenced);
        }
        else
        {
            await storage.EnsurePureValuesAsync(100, PureValues(local, 2), default);
            upload.Release();
            await Assert.ThrowsAsync<RemoteFileConflictException>(() => stale);
            Assert.True(successor.IsValid);
        }
        var content = (await container.GetBlobClient("db/100.pvl").DownloadContentAsync()).Value.Content.ToArray();
        Assert.Equal(predecessorFirst ? (byte)1 : (byte)2, Assert.Single(content.Distinct()));
    }

    /// <summary>B5 cleanup interleavings: a predecessor's delayed delete of a marked file lands after the successor
    /// protected it, and its delayed mark of an unmarked file lands after the successor listed it. Neither deletes a
    /// file the successor's checkpoint still needs.</summary>
    [Fact]
    public async Task DelayedPredecessorCleanupCannotDeleteAFileTheSuccessorProtects()
    {
        var hold = new Hold();
        var container = await fixture.ContainerAsync(hold);
        var clock = new Clock();
        var old = Held(clock);
        var (oldStorage, local) = await PureValuesNamespace(container, old);
        using var files = local;
        var successor = Held(clock);
        var raw = new AzureReplicationStorage(fixture.Connect(container.Name), "db");
        var storage = raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1)), successor);
        var marked = PureValues(local, 1);
        var unmarked = PureValues(local, 2);
        await oldStorage.EnsurePureValuesAsync(100, marked, default);
        await oldStorage.EnsurePureValuesAsync(102, unmarked, default);
        await container.GetBlobClient("db/200.kvi").UploadAsync(BinaryData.FromBytes([1]));
        var listed = await Maintenance(oldStorage);
        var mark = await oldStorage.ScheduleDeletionAsync(listed.Single(f => f.FileId == 100), TimeSpan.FromMilliseconds(1), default);
        await Task.Delay(10);

        var delete = hold.Arm(Path("/db/100.pvl", RequestMethod.Delete));
        var staleDelete = oldStorage.DeleteAsync(mark, default).AsTask();
        await delete.Reached;
        var lateMark = hold.Arm(message => message.Request.Method == RequestMethod.Put && Query(message, "metadata") &&
                                           message.Request.Uri.ToUri().AbsolutePath.EndsWith("/db/102.pvl", StringComparison.Ordinal));
        var staleMark = oldStorage.ScheduleDeletionAsync(listed.Single(f => f.FileId == 102), TimeSpan.FromDays(1), default).AsTask();
        await lateMark.Reached;
        old.Fence();

        // The successor's checkpoint needs both files: protection clears the mark, the unmarked file needs no request.
        Assert.True(await storage.ProtectPureValuesAsync(100, marked, default));
        delete.Release();
        await staleDelete;
        lateMark.Release();
        await staleMark;
        Assert.True((await container.GetBlobClient("db/100.pvl").ExistsAsync()).Value);
        Assert.NotNull((await Maintenance(storage)).Single(f => f.FileId == 102).DeleteAfter);
        // The successor's next cleanup keeps its dependencies and clears the late mark long before it is due.
        await new RemoteGarbageCollector(storage, successor, TimeSpan.FromDays(1))
            .CollectAsync(new PublishedCheckpoint(200, 1, new HashSet<uint> { 100, 102 }), default);
        var after = await Maintenance(storage);
        Assert.All(after, f => Assert.Null(f.DeleteAfter));
        Assert.Equal(new uint[] { 1, 100, 102, 200 }, after.Select(f => f.FileId).Order());
    }

    static async Task<List<RemoteMaintenanceFile>> Maintenance(IReplicationStorage storage)
    {
        var files = new List<RemoteMaintenanceFile>();
        await foreach (var file in storage.EnumerateMaintenanceAsync(default)) files.Add(file);
        return files;
    }

    static async Task Change(BTreeKeyValueDB db, ulong id)
    {
        using var transaction = await db.StartWritingTransaction(id);
        using var cursor = transaction.CreateCursor();
        cursor.CreateOrUpdateKeyValue([(byte)(id % 40)], Enumerable.Repeat((byte)id, 2000).ToArray());
        cursor.CreateOrUpdateKeyValue([255], BitConverter.GetBytes(id));
        transaction.Commit();
    }

    sealed class ManualTime : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.Parse("2026-09-22T00:00:00Z");
        public override DateTimeOffset GetUtcNow() => Now;
    }

    /// <summary>A leader database with canonical publication, checkpoints and cleanup on one namespace.</summary>
    sealed class Leader : IAsyncDisposable
    {
        public readonly InMemoryReplicationFileStorage Local = new();
        public readonly TransactionLogCapture Capture = new();
        public BTreeKeyValueDB Database = null!;
        public CanonicalTrlPublisher Publisher = null!;
        public ReplicationFileSet Files = null!;
        public ReplicationMaintenance Maintenance = null!;
        public ulong Last;

        public static async Task<Leader> Start(global::Azure.Storage.Blobs.BlobContainerClient container,
            TimeProvider? time = null)
        {
            var leader = new Leader();
            var clock = new Clock();
            var authority = Held(clock);
            var raw = new AzureReplicationStorage(container, "db", time);
            leader.Database = await Open(leader.Local, leader.Capture, 64 * 1024);
            leader.Publisher = new(leader.Database, leader.Capture, raw, authority, 1, id => $"{id}.trl");
            await leader.Write(1);
            var storage = raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1)), authority);
            leader.Files = new(leader.Local, storage);
            leader.Maintenance = new(leader.Database, leader.Files, leader.Publisher, storage, authority, clock,
                TimeSpan.FromTicks(1), TimeSpan.FromHours(1));
            return leader;
        }

        public async Task Write(int count)
        {
            for (var i = 0; i < count; i++) await Change(Database, ++Last);
            Assert.Equal(TrlPublishResult.Published, await Publisher.PublishNextAsync());
        }

        public async Task Checkpoint()
        {
            await Database.Compact(CancellationToken.None);
            await Maintenance.RunDueAsync(default);
        }

        public async ValueTask DisposeAsync()
        {
            Maintenance.Dispose();
            await Files.DisposeAsync();
            Publisher.Dispose();
            Database.Dispose();
            Local.Dispose();
        }
    }

    /// <summary>Open a restore of one discovered inventory and check that it is complete and consistent.</summary>
    static async Task<ulong> Restore(AzureReplicationStorage storage, CanonicalTrlInventory inventory)
    {
        using var cache = new InMemoryReplicationFileStorage();
        await using var set = new ReplicationFileSet(cache, storage.Bind(inventory));
        await set.InitializeAsync();
        using var copy = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            { FileCollection = set, CompactorScheduler = null, Compression = new NoCompressionStrategy() });
        using var transaction = copy.StartReadOnlyTransaction();
        var commit = transaction.GetCommitUlong();
        using var cursor = transaction.CreateCursor();
        var buffer = Span<byte>.Empty;
        Assert.True(cursor.FindExactKey([255]));
        Assert.False(cursor.IsValueCorrupted());
        Assert.Equal(commit, BitConverter.ToUInt64(cursor.GetValueSpan(ref buffer)));
        Assert.Equal(Math.Min(commit, 40) + 1, (ulong)cursor.GetKeyValueCount([]));
        Assert.True(cursor.FindFirstKey([]));
        do
        {
            if (cursor.GetKeySpan(ref buffer)[0] == 255) continue;
            Assert.False(cursor.IsValueCorrupted());
            var value = cursor.GetValueSpan(ref buffer).ToArray();
            Assert.Equal(2000, value.Length);
            Assert.Single(value.Distinct());
        } while (cursor.FindNextKey([]));
        return commit;
    }

    /// <summary>A restore that discovered TRL history before the leader appended to it, published a newer checkpoint
    /// and marked the files of the older one opens exactly the discovered state: appends and deletion marks change
    /// object versions but not selected bytes, and the newer checkpoint, which needs undiscovered history, is ignored.</summary>
    [Fact]
    public async Task RestoreOpensTheDiscoveredStateDespiteLaterPublicationCheckpointAndMarks()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Leader.Start(container);
        await leader.Write(60);
        await leader.Checkpoint();
        await leader.Write(5);
        var discovered = leader.Last;
        var storage = new AzureReplicationStorage(fixture.Connect(container.Name), "db");
        var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", 1));
        await leader.Write(60);
        await leader.Checkpoint();
        await leader.Write(5);
        Assert.Contains(await Maintenance(storage), f => f.DeleteAfter != null && f.FileType == KVFileType.KeyIndex);
        Assert.Contains(await Maintenance(storage), f => f.DeleteAfter != null && f.FileType == KVFileType.TransactionLog);
        Assert.Equal(discovered, await Restore(storage, inventory));
        Assert.Equal(leader.Last, await Restore(storage, await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", 1))));
    }

    /// <summary>Once cleanup has deleted what an old discovery selected, that attempt fails with retryable I/O and a
    /// fresh discovery restores the newest published state.</summary>
    [Fact]
    public async Task RestoreFromAnInventoryWhoseFilesWereDeletedFailsAndAFreshDiscoverySucceeds()
    {
        var container = await fixture.ContainerAsync();
        var time = new ManualTime();
        await using var leader = await Leader.Start(container, time);
        await leader.Write(60);
        await leader.Checkpoint();
        var storage = new AzureReplicationStorage(fixture.Connect(container.Name), "db");
        var inventory = await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", 1));
        await leader.Write(60);
        await leader.Checkpoint();
        time.Now += TimeSpan.FromHours(2);
        await leader.Write(1);
        await leader.Checkpoint();
        await Assert.ThrowsAnyAsync<IOException>(() => Restore(storage, inventory));
        Assert.Equal(leader.Last, await Restore(storage, await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", 1))));
    }

    /// <summary>Concurrent publication, checkpoints, compaction and cleanup marks against restores whose reads are
    /// slowed down: every restore opens a complete, consistent database at least as new as the history published when
    /// it started. Retries (printed) are only for transient failures.</summary>
    [Fact]
    public async Task RestoresCompleteConsistentlyWhileTheLeaderPublishesAndCollects()
    {
        var container = await fixture.ContainerAsync();
        await using var leader = await Leader.Start(container);
        long published = 1;
        using var stop = new CancellationTokenSource();
        var writer = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                await leader.Write(1);
                Interlocked.Exchange(ref published, (long)leader.Last);
                if (leader.Last % 20 == 0) await leader.Database.Compact(CancellationToken.None);
                if (leader.Last % 10 == 0) await leader.Maintenance.RunDueAsync(default);
            }
        });
        var slow = new Hold { DelayResponse = message => message.Request.Method == RequestMethod.Get, ResponseDelay = TimeSpan.FromMilliseconds(100) };
        var storage = new AzureReplicationStorage(fixture.Connect(container.Name, slow), "db");
        var retries = 0;
        var restored = new List<ulong>();
        try
        {
            while (restored.Count < 4)
            {
                var floor = (ulong)Interlocked.Read(ref published);
                try
                {
                    var commit = await Restore(storage, await CanonicalTrlInventory.DiscoverAsync(storage, new("1.trl", 1)));
                    Assert.True(commit >= floor, $"Restored {commit} before published {floor}.");
                    restored.Add(commit);
                }
                catch (IOException error)
                {
                    retries++;
                    output.WriteLine($"retry {retries}: {error.Message} {error.InnerException?.Message}");
                    Assert.True(retries < 10, "Restore made no progress.");
                }
                Assert.False(writer.IsCompleted, "The leader stopped.");
            }
        }
        finally
        {
            await stop.CancelAsync();
            await writer;
        }
        output.WriteLine($"restored commits {string.Join(", ", restored)} after {retries} retries; published {published}");
    }
}
