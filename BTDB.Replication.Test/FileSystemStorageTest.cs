using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.EventLog;
using BTDB.Replication.Test.EventLog;
using BTDB.StreamLayer;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Test;

public class FileSystemStorageTest : IDisposable
{
    const string Initial = """{"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """;
    readonly string _root = Path.Combine(Path.GetTempPath(), "btdb-fs-" + Guid.NewGuid().ToString("N"));

    string Dir(string name) => Path.Combine(_root, name);

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, true);
    }

    /// <summary>Manual monotonic timestamps (lease expiry) and UTC (deletion deadlines) advancing together.</summary>
    sealed class ManualClock : TimeProvider
    {
        long _ticks;
        DateTimeOffset _now = DateTimeOffset.Parse("2026-11-07T00:00:00Z");
        public override long GetTimestamp() => _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override DateTimeOffset GetUtcNow() => _now;
        public DateTimeOffset Now => _now;

        public void Advance(TimeSpan duration)
        {
            _ticks += duration.Ticks;
            _now += duration;
        }
    }

    sealed class Clock : IReplicationScheduler
    {
        readonly System.Diagnostics.Stopwatch _clock = System.Diagnostics.Stopwatch.StartNew();
        public TimeSpan Elapsed => _clock.Elapsed;
        public IDisposable Schedule(TimeSpan delay, Action callback, string description) => throw new NotSupportedException();
    }

    static LeaseAuthority Authority()
    {
        var authority = new LeaseAuthority(new Clock(), 0, TimeSpan.Zero);
        Assert.True(authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1)));
        return authority;
    }

    static async Task<BTreeKeyValueDB> Open(InMemoryReplicationFileStorage files, TransactionLogCapture capture,
        uint splitSize = int.MaxValue)
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
            FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = capture,
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

    static IFileCollectionFile Source(InMemoryFileCollection files, params byte[] content)
    {
        var file = files.AddFile("src");
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(content);
        writer.Flush();
        return file;
    }

    [Fact]
    public async Task LeaderLeaseExpiresTransfersAndGuardsTheRecordLikeAzure()
    {
        var time = new ManualClock();
        var storage = new FileSystemLeaderStorage(Dir("leader"), TimeSpan.FromSeconds(15), Initial, time);
        var record = await storage.ReadAsync(default);
        Assert.Equal(Initial, record.Json);
        var grant = (await storage.AcquireAsync(default))!;
        Assert.Equal(TimeSpan.FromSeconds(15), grant.GuaranteedDuration);
        Assert.Null(await storage.AcquireAsync(default));
        Assert.Equal(LeaderWriteOutcome.Applied, await storage.WriteAsync(grant.Handle, record.Token, "first", default));
        Assert.Equal(LeaderWriteOutcome.Rejected, await storage.WriteAsync(grant.Handle, record.Token, "stale", default));
        record = await storage.ReadAsync(default);
        Assert.Equal("first", record.Json);
        Assert.Equal(LeaderWriteOutcome.Rejected, await storage.WriteAsync("other", record.Token, "no lease", default));

        // Another instance in this process shares the lease.
        using (var shared = new FileSystemLeaderStorage(Dir("leader"), TimeSpan.FromSeconds(15), Initial, time))
            Assert.Null(await shared.AcquireAsync(default));
        time.Advance(TimeSpan.FromSeconds(14));
        Assert.Equal(TimeSpan.FromSeconds(15), await storage.RenewAsync(grant.Handle, default));
        time.Advance(TimeSpan.FromSeconds(14));
        Assert.Null(await storage.AcquireAsync(default));
        time.Advance(TimeSpan.FromSeconds(2));
        var next = (await storage.AcquireAsync(default))!;
        Assert.NotEqual(grant.Handle, next.Handle);
        Assert.Null(await storage.RenewAsync(grant.Handle, default));
        Assert.Equal(LeaderWriteOutcome.Rejected, await storage.WriteAsync(grant.Handle, record.Token, "fenced", default));

        await storage.TransferAsync(next.Handle, "prepared", default);
        Assert.Null(await storage.RenewAsync(next.Handle, default));
        Assert.Equal(TimeSpan.FromSeconds(15), await storage.RenewAsync("prepared", default));
        await Assert.ThrowsAsync<IOException>(() => storage.TransferAsync(next.Handle, "again", default).AsTask());
        Assert.Equal(LeaderWriteOutcome.Applied, await storage.WriteAsync("prepared", record.Token, "second", default));
        storage.Dispose();

        // A new process finds the lease free and the last record.
        using var restarted = new FileSystemLeaderStorage(Dir("leader"), TimeSpan.FromSeconds(15), Initial, time);
        Assert.NotNull(await restarted.AcquireAsync(default));
        Assert.Equal("second", (await restarted.ReadAsync(default)).Json);
    }

    [Fact]
    public async Task DirectoryIsLockedForOtherProcessesUntilEveryInstanceIsDisposed()
    {
        var lockFile = Path.Combine(Dir("db"), ".lock");
        var first = new FileSystemReplicationStorage(Dir("db"));
        var second = new FileSystemReplicationStorage(Dir("db"));
        // Another process opening the lock file exclusively is refused (the same check applies within a process).
        Assert.Throws<IOException>(() => new FileStream(lockFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None));
        first.Dispose();
        Assert.Throws<IOException>(() => new FileStream(lockFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None));
        Assert.Null(await second.ReadAsync("1.trl", default));
        second.Dispose();
        Assert.Throws<ObjectDisposedException>(() => second.ReadAsync("1.trl", default).AsTask().GetAwaiter().GetResult());
        new FileStream(lockFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None).Dispose();
    }

    [Fact]
    public async Task CanonicalTrlWritesAreConditionalAndOnlyTheSealingWriteKeepsAChecksum()
    {
        using var files = new InMemoryFileCollection();
        var source = Source(files, 1, 2, 3, 4, 5, 6);
        using var storage = new FileSystemReplicationStorage(Dir("db"));
        var created = await storage.WriteAsync(new(1, "1.trl", null, 0, 2, source), default);
        Assert.Equal(TrlWriteOutcome.Applied, created.Outcome);
        Assert.Equal(TrlWriteOutcome.Rejected, (await storage.WriteAsync(new(1, "1.trl", null, 0, 2, source), default)).Outcome);
        Assert.Equal(TrlWriteOutcome.Rejected,
            (await storage.WriteAsync(new(1, "1.trl", "stale", 2, 4, source), default)).Outcome);
        Assert.Equal(TrlWriteOutcome.Rejected,
            (await storage.WriteAsync(new(1, "1.trl", created.State!.Token, 1, 4, source), default)).Outcome);
        var sealedState = (await storage.WriteAsync(new(1, "1.trl", created.State.Token, 2, 4, source, "SHA"), default)).State!;
        Assert.Equal((4u, "SHA"), (sealedState.Length, sealedState.Sha256));
        Assert.Equal(sealedState, await storage.ReadAsync("1.trl", default));
        var adopted = (await storage.WriteAsync(new(1, "1.trl", sealedState.Token, 4, 4, source), default)).State!;
        Assert.NotEqual(sealedState.Token, adopted.Token);
        Assert.Null(adopted.Sha256);
        var range = new byte[3];
        await storage.ReadRangeAsync("1.trl", adopted.Token, 1, range, default);
        Assert.Equal(new byte[] { 2, 3, 4 }, range);
        await Assert.ThrowsAsync<IOException>(() =>
            storage.ReadRangeAsync("1.trl", sealedState.Token, 1, range, default).AsTask());
        await Assert.ThrowsAsync<IOException>(() =>
            storage.ReadRangeAsync("1.trl", adopted.Token, 2, range, default).AsTask());
        Assert.Equal(new TrlSuccessor("1.trl", 1), await storage.ResolveRecoveryRootAsync(new("1.trl", 1), default));
    }

    [Fact]
    public async Task CrashLeftoversAreRemovedOnOpenWithoutTouchingCommittedObjects()
    {
        using var files = new InMemoryFileCollection();
        var source = Source(files, 7, 8, 9);
        string token;
        using (var storage = new FileSystemReplicationStorage(Dir("db")))
            token = (await storage.WriteAsync(new(1, "1.trl", null, 0, 3, source), default)).State!.Token;
        var data = Directory.GetFiles(Path.Combine(Dir("db"), "data")).Single();
        await File.AppendAllTextAsync(data, "interrupted append");
        await File.WriteAllTextAsync(Path.Combine(Dir("db"), "data", "orphan"), "unreferenced");
        await File.WriteAllTextAsync(Path.Combine(Dir("db"), "tmp", "staged"), "unfinished");
        using var reopened = new FileSystemReplicationStorage(Dir("db"));
        Assert.Equal([data], Directory.GetFiles(Path.Combine(Dir("db"), "data")));
        Assert.Empty(Directory.GetFiles(Path.Combine(Dir("db"), "tmp")));
        Assert.Equal(new TrlObjectState(token, 3), await reopened.ReadAsync("1.trl", default));
        var appended = await reopened.WriteAsync(new(1, "1.trl", token, 3, 3, source), default);
        Assert.Equal(3, new FileInfo(data).Length);
        var bytes = new byte[3];
        await reopened.ReadRangeAsync("1.trl", appended.State!.Token, 0, bytes, default);
        Assert.Equal(new byte[] { 7, 8, 9 }, bytes);
    }

    [Fact]
    public async Task ImmutablePureValuesReconcileTheSameContentAndFenceOnDifferentContent()
    {
        using var files = new InMemoryFileCollection();
        var raw = new FileSystemReplicationStorage(Dir("db"));
        await raw.WriteAsync(new(1, "1.trl", null, 0, 1, Source(files, 1)), default);
        var authority = Authority();
        var storage = raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1)), authority);
        var same = new KeyIndexFileSource(2, KVFileType.PureValues, 3, 0, Source(files, 1, 2, 3));
        await storage.EnsurePureValuesAsync(2, same, default);
        await storage.EnsurePureValuesAsync(2, same, default); // A retried identity with the same content.
        Assert.True(await storage.ProtectPureValuesAsync(2, same, default));
        Assert.False(await storage.ProtectPureValuesAsync(3, same, default));
        var listed = new List<RemoteFile>();
        await foreach (var file in storage.EnumerateAsync(default)) listed.Add(file);
        var pvl = Assert.Single(listed, f => f.FileType == KVFileType.PureValues);
        Assert.Equal((2u, 3ul, true), (pvl.FileId, pvl.Length, pvl.IsSealed));
        var read = new byte[3];
        Assert.Equal(3, await storage.ReadAsync(pvl, 0, read, default));
        Assert.Equal(new byte[] { 1, 2, 3 }, read);
        await Assert.ThrowsAsync<RemoteFileConflictException>(() => storage.EnsurePureValuesAsync(2,
            new(2, KVFileType.PureValues, 3, 0, Source(files, 3, 2, 1)), default).AsTask());
        Assert.False(authority.IsValid);
        await Assert.ThrowsAsync<InvalidOperationException>(() => storage.EnsurePureValuesAsync(4, same, default).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            raw.EnsurePureValuesAsync(4, same, default).AsTask()); // An unbound adapter cannot publish.
        raw.Dispose();
    }

    [Theory]
    [InlineData("2.pvl", KVFileType.PureValues)]
    [InlineData("2.trl", KVFileType.TransactionLog)]
    public async Task DeletionDeadlineSurvivesReopeningAndDoesNotMoveOnRetry(string key, KVFileType type)
    {
        var time = new ManualClock();
        using var files = new InMemoryFileCollection();
        var authority = Authority();
        RemoteMaintenanceFile marked;
        using (var raw = new FileSystemReplicationStorage(Dir("db"), time))
        {
            await raw.WriteAsync(new(1, "1.trl", null, 0, 1, Source(files, 1)), default);
            var storage = raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1)), authority);
            if (type == KVFileType.PureValues)
                await storage.EnsurePureValuesAsync(2, new(2, type, 1, 0, Source(files, 2)), default);
            else
                await raw.WriteAsync(new(2, "2.trl", null, 0, 1, Source(files, 2)), default);
            var listed = new List<RemoteMaintenanceFile>();
            await foreach (var file in storage.EnumerateMaintenanceAsync(default)) listed.Add(file);
            var target = Assert.Single(listed, f => f.Key == key);
            Assert.Null(target.DeleteAfter);
            await storage.DeleteAsync(target with { DeleteAfter = time.Now }, default); // Unmarked: protected.
            marked = await storage.ScheduleDeletionAsync(target, TimeSpan.FromHours(24), default);
            Assert.Equal(time.Now + TimeSpan.FromHours(24), marked.DeleteAfter);
            Assert.NotEqual(target.Version, marked.Version);
        }
        time.Advance(TimeSpan.FromHours(23));
        using var reopened = new FileSystemReplicationStorage(Dir("db"), time);
        var restarted = reopened.Bind(await CanonicalTrlInventory.DiscoverAsync(reopened, new("1.trl", 1)), authority);
        var retried = await restarted.ScheduleDeletionAsync(marked, TimeSpan.FromHours(24), default);
        Assert.Equal(marked.DeleteAfter, retried.DeleteAfter);
        await restarted.DeleteAsync(retried, default);
        Assert.Contains(key, await Keys(restarted));
        time.Advance(TimeSpan.FromHours(1));
        await restarted.DeleteAsync(retried, default);
        Assert.DoesNotContain(key, await Keys(restarted));
        Assert.Single(Directory.GetFiles(Path.Combine(Dir("db"), "data"))); // Only 1.trl content remains.

        static async Task<List<string>> Keys(IReplicationStorage storage)
        {
            var keys = new List<string>();
            await foreach (var file in storage.EnumerateMaintenanceAsync(default)) keys.Add(file.Key);
            return keys;
        }
    }

    [Fact]
    public async Task PublishedHistoryAndCheckpointRestoreANativeDatabaseAfterCleanup()
    {
        var time = new ManualClock();
        var authority = Authority();
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using (var raw = new FileSystemReplicationStorage(Dir("db"), time))
        {
            using var db = await Open(local, capture, 1024);
            for (ulong i = 1; i <= 400; i++) await Write(db, i);
            using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"{id}.trl");
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
            var storage = raw.Bind(await CanonicalTrlInventory.DiscoverAsync(raw, new("1.trl", 1)), authority);
            using var snapshot = db.CaptureKeyIndexSnapshot();
            Assert.True(snapshot.TransactionLogFileId > 1);
            await storage.PublishKeyIndexAsync(10000, snapshot, new Dictionary<uint, uint>(), default);
            await storage.PublishKeyIndexAsync(10000, snapshot, new Dictionary<uint, uint>(), default); // Reconciled.
            var gc = new RemoteGarbageCollector(storage, authority, TimeSpan.FromDays(1));
            var checkpoint = new PublishedCheckpoint(10000, snapshot.TransactionLogFileId,
                snapshot.Sources.Select(s => s.FileId).ToHashSet());
            await gc.CollectAsync(checkpoint, default);
            Assert.NotNull(await raw.ReadAsync("1.trl", default)); // Marked, not yet due.
            time.Advance(TimeSpan.FromDays(1));
            await gc.CollectAsync(checkpoint, default);
            Assert.Null(await raw.ReadAsync("1.trl", default));
        }
        // A restarted node with an empty cache restores from the directory alone.
        using var fresh = new FileSystemReplicationStorage(Dir("db"), time);
        IBindableReplicationStorage bindable = fresh;
        var recovered = await CanonicalTrlInventory.DiscoverAsync(fresh, new("1.trl", 1));
        using var cache = new InMemoryReplicationFileStorage();
        await using var collection = new ReplicationFileSet(cache, bindable.Bind(recovered));
        await collection.InitializeAsync();
        using var restored = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        { FileCollection = collection, CompactorScheduler = null, Compression = new NoCompressionStrategy() });
        using var transaction = restored.StartReadOnlyTransaction();
        Assert.Equal(400ul, transaction.GetCommitUlong());
        using var cursor = transaction.CreateCursor();
        Assert.Equal(256, cursor.GetKeyValueCount([]));
    }

    static readonly EventLogOwner A = new("a", "http://node-a:5000/x?y=1");
    static readonly EventLogOwner B = new("b", "http://node-b");

    [Fact]
    public async Task EventLogWritesOwnersRangesAndListingFollowTheContract()
    {
        using var storage = new FileSystemEventLogStorage(Dir("log"));
        var created = await storage.WriteAsync("t/1", null, new byte[] { 1, 2, 3 }, A, null, default);
        Assert.Equal(EventLogWriteOutcome.Applied, created.Outcome);
        Assert.Equal(EventLogWriteOutcome.Rejected, (await storage.WriteAsync("t/1", null, new byte[] { 9 }, B, null, default)).Outcome);
        var blob = await storage.ReadAsync("t/1", default);
        Assert.Equal((created.Version, A), (blob!.Version, blob.Owner));
        Assert.Equal(new byte[] { 1, 2, 3 }, blob.Content.ToArray());
        var replaced = await storage.WriteAsync("t/1", created.Version, new byte[] { 1, 2, 3, 4 }, A, null, default);
        Assert.Equal(EventLogWriteOutcome.Applied, replaced.Outcome);
        Assert.Equal(EventLogWriteOutcome.Rejected, (await storage.WriteAsync("t/1", created.Version, new byte[] { 7 }, B, null, default)).Outcome);
        var owned = await storage.SetOwnerAsync("t/1", replaced.Version!, B, default);
        Assert.Equal(EventLogWriteOutcome.Applied, owned.Outcome);
        Assert.Equal(EventLogWriteOutcome.Rejected, (await storage.SetOwnerAsync("t/1", replaced.Version!, A, default)).Outcome);
        var properties = await storage.GetPropertiesAsync("t/1", default);
        Assert.Equal((owned.Version, 4L, B), (properties!.Version, properties.Length, properties.Owner));
        var range = new byte[2];
        await storage.ReadRangeAsync("t/1", owned.Version!, 1, range, default);
        Assert.Equal(new byte[] { 2, 3 }, range);
        await Assert.ThrowsAsync<EventLogVersionChangedException>(async () =>
            await storage.ReadRangeAsync("t/1", created.Version!, 1, range, default));
        await storage.WriteAsync("t/2", null, new byte[] { 5 }, null, "abc", default);
        await storage.WriteAsync("t/l1/3", null, new byte[] { 5 }, null, null, default);
        await storage.WriteAsync("u/1", null, new byte[] { 5 }, null, null, default);
        await storage.WriteAsync("tt/1", null, new byte[] { 5 }, null, null, default);
        var listed = new List<EventLogObjectInfo>();
        await foreach (var info in storage.ListAsync("t/", default)) listed.Add(info);
        Assert.Equal(["t/1", "t/2", "t/l1/3"], listed.Select(i => i.Key));
        Assert.Null(await storage.ReadAsync("t/3", default));
        Assert.Null(await storage.GetPropertiesAsync("t/3", default));
        Assert.Equal(EventLogWriteOutcome.Applied, (await storage.WriteAsync("t/empty", null, ReadOnlyMemory<byte>.Empty, null, null, default)).Outcome);
        Assert.Empty((await storage.ReadAsync("t/empty", default))!.Content.ToArray());
        await Assert.ThrowsAsync<FormatException>(() => storage.ReadAsync("T/1", default).AsTask());
        await Assert.ThrowsAsync<FormatException>(() => storage.ReadAsync("t/../x", default).AsTask());
    }

    [Fact]
    public async Task EventLogDeletionNeedsAnElapsedDeadlineAndTheMarkedVersion()
    {
        var time = new ManualClock();
        using var storage = new FileSystemEventLogStorage(Dir("log"), time);
        var created = await storage.WriteAsync("t/1", null, new byte[] { 1 }, null, "sha", default);
        Assert.False(await storage.DeleteAsync("t/1", created.Version!, default)); // Unmarked.
        Assert.Null(await storage.ScheduleDeletionAsync("t/1", "stale", TimeSpan.FromHours(1), default));
        var marked = await storage.ScheduleDeletionAsync("t/1", created.Version!, TimeSpan.FromHours(1), default);
        Assert.NotNull(marked);
        Assert.Equal(marked, await storage.ScheduleDeletionAsync("t/1", marked!, TimeSpan.FromHours(5), default));
        Assert.False(await storage.DeleteAsync("t/1", marked!, default)); // Not due.
        time.Advance(TimeSpan.FromHours(2));
        Assert.False(await storage.DeleteAsync("t/1", created.Version!, default)); // Stale version.
        Assert.True(await storage.DeleteAsync("t/1", marked!, default));
        Assert.Null(await storage.ReadAsync("t/1", default));
        // A recreated object never reuses a deleted version.
        var recreated = await storage.WriteAsync("t/1", null, new byte[] { 1 }, null, "sha", default);
        Assert.NotEqual(created.Version, recreated.Version);
        Assert.NotEqual(marked, recreated.Version);
    }

    [Fact]
    public async Task EventLogServiceKeepsTopicsAcrossARestartOfItsNode()
    {
        var options = new EventLogOptions
        {
            SplitCap = 1024, SealFreeSpace = 128, MergeInterval = TimeSpan.FromMilliseconds(50),
            HeartbeatInterval = TimeSpan.FromMilliseconds(50), OwnerTimeout = TimeSpan.FromMilliseconds(500)
        };
        var scheduler = new SystemReplicationScheduler();
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var expected = Enumerable.Range(0, 300).Select(i => Bytes(i, 40)).ToList();
        using (var storage = new FileSystemEventLogStorage(Dir("log")))
        {
            var transport = new InProcessEventLogTransport();
            await using var service = new EventLogService(storage, transport.From("http://127.0.0.1:5000"),
                "http://127.0.0.1:5000", scheduler, options);
            transport.Register("http://127.0.0.1:5000", service);
            var topic = service.GetTopic("events");
            for (var i = 0; i < 100; i++) Assert.Equal((ulong)i, await topic.PublishAsync(expected[i], cancel.Token));
            var offsets = await Task.WhenAll(expected.Skip(100).Select(r => topic.PublishAsync(r, cancel.Token).AsTask()));
            Assert.Equal(Enumerable.Range(100, 200).Select(i => (ulong)i), offsets.Order());
            // Concurrent publications pick their own order.
            expected = (await Collect(topic, 0, 300, cancel.Token)).Select(r => r.ToArray()).ToList();
            Assert.Equal(new EventLogBounds(0, 300), await topic.GetBoundsAsync(cancel.Token));
        }
        Assert.Contains(Directory.EnumerateFiles(Path.Combine(Dir("log"), "meta", "events")), f => f.EndsWith(".elog"));

        using (var storage = new FileSystemEventLogStorage(Dir("log")))
        {
            var transport = new InProcessEventLogTransport();
            await using var service = new EventLogService(storage, transport.From("http://127.0.0.1:5000"),
                "http://127.0.0.1:5000", scheduler, options);
            transport.Register("http://127.0.0.1:5000", service);
            var topic = service.GetTopic("events");
            Assert.Equal(new EventLogBounds(0, 300), await topic.GetBoundsAsync(cancel.Token));
            Assert.Equal(expected, (await Collect(topic, 0, 300, cancel.Token)).Select(r => r.ToArray()));
            // The restarted node takes over the topic its previous session owned at the same endpoint.
            Assert.Equal(300ul, await topic.PublishAsync(Bytes(300, 40), cancel.Token));
            Assert.Equal(Bytes(300, 40), (await Collect(topic, 300, 301, cancel.Token)).Single().ToArray());
        }

        static async Task<List<ReadOnlyMemory<byte>>> Collect(IEventTopic topic, ulong start, ulong end,
            CancellationToken cancellation)
        {
            var records = new List<ReadOnlyMemory<byte>>();
            await foreach (var record in topic.ReadAsync(start, end, cancellation))
            {
                Assert.Equal(start + (ulong)records.Count, record.Offset);
                records.Add(record.Payload.ToArray());
            }
            return records;
        }
    }
}
