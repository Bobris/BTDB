using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test.Simulation;
using Xunit;

namespace BTDB.Replication.Test;

public class RemoteMaintenanceTest
{
    internal sealed class Storage(LeaseAuthority authority, IReplicationScheduler? clock = null) : IReplicationStorage, IDisposable
    {
        public ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation) => Inner.ReadAsync(key, cancellation);
        public ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination, CancellationToken cancellation) =>
            Inner.ReadRangeAsync(key, token, offset, destination, cancellation);
        public ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation) => Inner.WriteAsync(write, cancellation);

        internal readonly CheckpointPublisherTest.Storage Inner = new();
        readonly Dictionary<uint, DateTimeOffset> _deadlines = new();
        DateTimeOffset Now => DateTimeOffset.UnixEpoch + (clock?.Elapsed ?? TimeSpan.Zero);
        readonly Dictionary<uint, int> _versions = new();
        public readonly List<uint> Deleted = new();
        public Action? DelayedDelete;
        public bool DelayDelete;
        public bool FailInventory;
        public TaskCompletionSource? HoldKvi;
        string Version(uint id) => Inner.Describe(id).Version + ":" + _versions.GetValueOrDefault(id);
        public async IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync([EnumeratorCancellation] CancellationToken cancellation)
        {
            if (FailInventory) throw new IOException("Inventory unavailable.");
            await foreach (var file in Inner.EnumerateAsync(cancellation))
                yield return new(file.FileId.ToString(), file.FileId, file.FileType, Version(file.FileId), false, _deadlines.TryGetValue(file.FileId, out var deadline) ? deadline : null);
        }
        public int Schedules;
        public ValueTask<RemoteMaintenanceFile> ScheduleDeletionAsync(RemoteMaintenanceFile file, TimeSpan delay, CancellationToken cancellation)
        {
            Schedules++;
            if (Version(file.FileId) != file.Version) return ValueTask.FromResult(file);
            if (!_deadlines.TryGetValue(file.FileId, out var deadline)) _deadlines[file.FileId] = deadline = Now + delay;
            return ValueTask.FromResult(file with { DeleteAfter = deadline });
        }
        public ValueTask CancelDeletionAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
        {
            if (Version(file.FileId) != file.Version) throw new IOException("Protection conflict.");
            _deadlines.Remove(file.FileId);
            _versions[file.FileId] = _versions.GetValueOrDefault(file.FileId) + 1;
            return ValueTask.CompletedTask;
        }
        public ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            if (!authority.IsValid) throw new InvalidOperationException();
            if (!_deadlines.TryGetValue(file.FileId, out var due) || Now < due) return ValueTask.CompletedTask;
            void Apply()
            {
                if (Inner.Files.GetFile(file.FileId) == null || Version(file.FileId) != file.Version) return;
                Inner.Files.GetFile(file.FileId).Remove();
                Inner.Types.Remove(file.FileId);
                Deleted.Add(file.FileId);
            }
            if (DelayDelete) DelayedDelete = Apply; else Apply();
            return ValueTask.CompletedTask;
        }
        public ValueTask<bool> ProtectPureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            if (!authority.IsValid) throw new InvalidOperationException();
            if (Inner.Files.GetFile(id) == null) return ValueTask.FromResult(false);
            _deadlines.Remove(id);
            _versions[id] = _versions.GetValueOrDefault(id) + 1;
            return ValueTask.FromResult(true);
        }
        public IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation) => Inner.EnumerateAsync(cancellation);
        public ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation) => Inner.ReadAsync(file, offset, buffer, cancellation);
        public ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) => Inner.EnsurePureValuesAsync(id, source, cancellation);
        public async ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map, CancellationToken cancellation)
        {
            if (HoldKvi != null) await HoldKvi.Task; // Deliberately ignore cancellation.
            await Inner.PublishKeyIndexAsync(id, snapshot, map, cancellation);
        }
        public void Dispose() => Inner.Dispose();
        public void Add(uint id, KVFileType type)
        {
            Inner.Files.ImportFile(id, type == KVFileType.PureValues ? "pvl" : "kvi");
            Inner.Types.Add(id, type);
        }
    }

    static async Task Change(BTreeKeyValueDB db, ulong eventId)
    {
        using var tr = await db.StartWritingTransaction(eventId);
        using var cursor = tr.CreateCursor();
        cursor.CreateOrUpdateKeyValue([201], [(byte)eventId]);
        tr.Commit();
    }

    static LeaseAuthority Lease(DeterministicScheduler clock)
    {
        var authority = new LeaseAuthority(clock.CreateScope("leader"), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        return authority;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepeatedCheckpointOrCleanupFailuresDoNotRenewDeadline(bool cleanup)
    {
        var clock = new DeterministicScheduler(905);
        var authority = Lease(clock);
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await CheckpointPublisherTest.OpenForPublication(local, capture);
        await CheckpointPublisherTest.Populate(db);
        using var storage = new Storage(authority, clock.CreateScope("storage"));
        await using var files = new ReplicationFileSet(local, storage);
        using var canonical = CheckpointPublisherTest.CreateCanonical(db, capture, storage.Inner, authority);
        using var maintenance = new ReplicationMaintenance(db, files, canonical, storage, authority,
            clock.CreateScope("maintenance"), TimeSpan.FromTicks(1000), TimeSpan.FromTicks(1));
        var expired = 0;
        maintenance.Watchdog = new(clock.CreateScope("watchdog"), TimeSpan.FromTicks(10), () => expired++);
        storage.Inner.FailKvi = !cleanup;
        storage.FailInventory = cleanup;
        await Assert.ThrowsAsync<IOException>(() => maintenance.RunDueAsync(default).AsTask());
        var attempts = storage.Inner.KviAttempts.Count;
        clock.AdvanceBy(TimeSpan.FromTicks(6));
        await Assert.ThrowsAsync<IOException>(() => maintenance.RunDueAsync(default).AsTask());
        if (cleanup) Assert.Equal(attempts, storage.Inner.KviAttempts.Count);
        clock.AdvanceBy(TimeSpan.FromTicks(4));
        Assert.Equal(1, expired);
    }

    [Fact]
    public async Task BlockedCheckpointCannotPreventDeadlineAndSuccessfulCycleLeavesNoIdleTimer()
    {
        var clock = new DeterministicScheduler(907);
        var authority = Lease(clock);
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await CheckpointPublisherTest.OpenForPublication(local, capture);
        await CheckpointPublisherTest.Populate(db);
        using var storage = new Storage(authority, clock.CreateScope("storage"));
        await using var files = new ReplicationFileSet(local, storage);
        using var canonical = CheckpointPublisherTest.CreateCanonical(db, capture, storage.Inner, authority);
        using var maintenance = new ReplicationMaintenance(db, files, canonical, storage, authority,
            clock.CreateScope("maintenance"), TimeSpan.FromTicks(1000), TimeSpan.FromTicks(1));
        var expired = 0;
        maintenance.Watchdog = new(clock.CreateScope("watchdog"), TimeSpan.FromTicks(10), () => expired++);
        await maintenance.RunDueAsync(default);
        clock.AdvanceBy(TimeSpan.FromTicks(999));
        await maintenance.RunDueAsync(default);
        Assert.Equal(0, expired);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        await Change(db, 100); // An unchanged database would skip the KVI export entirely.
        storage.HoldKvi = new();
        var running = maintenance.RunDueAsync(default).AsTask();
        try
        {
            clock.AdvanceBy(TimeSpan.FromTicks(10));
            Assert.False(running.IsCompleted);
            Assert.Equal(1, expired);
        }
        finally
        {
            storage.HoldKvi.SetResult();
            await running;
        }
    }

    [Fact]
    public void ForwardMaintenanceProgressExtendsDeadlineButIdleAndDisposedLanesAreNotWatched()
    {
        var clock = new DeterministicScheduler(906);
        var expired = 0;
        using var watchdog = new ReplicationMaintenanceWatchdog(clock.CreateScope("watchdog"), TimeSpan.FromTicks(10), () => expired++);
        clock.AdvanceBy(TimeSpan.FromTicks(100));
        watchdog.Observe((0, 0, 0));
        clock.AdvanceBy(TimeSpan.FromTicks(9));
        watchdog.Observe((1, 1, 1));
        clock.AdvanceBy(TimeSpan.FromTicks(9));
        watchdog.Observe((1, 1, 2));
        clock.AdvanceBy(TimeSpan.FromTicks(9));
        Assert.Equal(0, expired);
        watchdog.Observe(null);
        clock.AdvanceBy(TimeSpan.FromTicks(100));
        Assert.Equal(0, expired);
        watchdog.Observe((0, 0, 0));
        watchdog.Dispose();
        watchdog.Observe((1, 1, 1));
        clock.AdvanceBy(TimeSpan.FromTicks(100));
        Assert.Equal(0, expired);
    }

    [Fact]
    public async Task ConfirmedCheckpointCleanupDelaysDeletionPreservesClosureAndNeverReusesHighestId()
    {
        var clock = new DeterministicScheduler(901);
        var authority = Lease(clock);
        using var storage = new Storage(authority, clock.CreateScope("storage"));
        storage.Add(2, KVFileType.PureValues);
        storage.Add(4, KVFileType.PureValues);
        storage.Add(6, KVFileType.KeyIndex);
        storage.Add(8, KVFileType.KeyIndex);
        storage.Add(10, KVFileType.PureValues); // Highest allocated orphan anchors allocation.
        var gc = new RemoteGarbageCollector(storage, authority, TimeSpan.FromTicks(10));
        var checkpoint = new PublishedCheckpoint(8, 7, new HashSet<uint> { 4 });
        await gc.CollectAsync(checkpoint, default);
        Assert.Empty(storage.Deleted);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        gc = new RemoteGarbageCollector(storage, authority, TimeSpan.FromTicks(10));
        var schedules = storage.Schedules;
        await gc.CollectAsync(checkpoint, default);
        Assert.Equal(schedules, storage.Schedules); // Listed deadlines are deleted without marking them again.
        Assert.Equal(new uint[] { 2, 6 }, storage.Deleted.Order().ToArray());
        Assert.NotNull(storage.Inner.Files.GetFile(4));
        Assert.NotNull(storage.Inner.Files.GetFile(8));
        Assert.NotNull(storage.Inner.Files.GetFile(10));
        using var local = new InMemoryReplicationFileStorage();
        await using var files = new ReplicationFileSet(local, storage);
        Assert.Equal(12u, await files.AllocateRemoteFileIdAsync());
    }

    [Fact]
    public async Task VersionProtectionDefeatsDelayedDeleteAndHigherCheckpointStopsCleanup()
    {
        var clock = new DeterministicScheduler(902);
        var authority = Lease(clock);
        using var storage = new Storage(authority, clock.CreateScope("storage"));
        storage.Add(2, KVFileType.PureValues);
        storage.Add(4, KVFileType.KeyIndex);
        Assert.Throws<ArgumentOutOfRangeException>(() => new RemoteGarbageCollector(storage, authority, TimeSpan.Zero));
        var gc = new RemoteGarbageCollector(storage, authority, TimeSpan.FromTicks(1));
        storage.DelayDelete = true;
        await gc.CollectAsync(new(4, 3, new HashSet<uint>()), default);
        Assert.Null(storage.DelayedDelete); // Marked, not yet due.
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        await gc.CollectAsync(new(4, 3, new HashSet<uint>()), default);
        Assert.NotNull(storage.DelayedDelete);
        using var local = new InMemoryReplicationFileStorage();
        var source = local.ImportFile(2, "pvl");
        Assert.True(await storage.ProtectPureValuesAsync(2, new(2, KVFileType.PureValues, 0, 0, source), default));
        storage.DelayedDelete!();
        Assert.NotNull(storage.Inner.Files.GetFile(2));
        storage.DelayDelete = false;
        storage.Add(6, KVFileType.KeyIndex);
        await gc.CollectAsync(new(4, 3, new HashSet<uint>()), default);
        Assert.Empty(storage.Deleted);
        authority.Fence();
        await gc.CollectAsync(new(6, 3, new HashSet<uint>()), default);
        Assert.Empty(storage.Deleted);
    }

    [Fact]
    public async Task ActualCompactedCheckpointRestoresAfterCleanupAndFailedKviCannotAuthorizeDeletes()
    {
        var clock = new DeterministicScheduler(903);
        var authority = Lease(clock);
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await CheckpointPublisherTest.OpenForPublication(local, capture);
        await CheckpointPublisherTest.Populate(db);
        using var storage = new Storage(authority, clock.CreateScope("storage"));
        await using var files = new ReplicationFileSet(local, storage);
        using var canonical = CheckpointPublisherTest.CreateCanonical(db, capture, storage.Inner, authority);
        using var maintenance = new ReplicationMaintenance(db, files, canonical, storage, authority,
            clock.CreateScope("maintenance"), TimeSpan.FromTicks(10), TimeSpan.FromTicks(1));
        storage.Inner.FailKvi = true;
        await Assert.ThrowsAsync<IOException>(() => maintenance.RunDueAsync(default).AsTask());
        Assert.Empty(storage.Deleted);
        var intendedId = storage.Inner.KviAttempts.Single();
        storage.Inner.FailKvi = false;
        await maintenance.RunDueAsync(default);
        Assert.All(storage.Inner.KviAttempts, id => Assert.Equal(intendedId, id));
        Assert.DoesNotContain(local.Enumerate(), f => local.GetFileType(f.Index) == KVFileType.KeyIndex);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        var attempts = storage.Inner.KviAttempts.Count;
        await maintenance.RunDueAsync(default);
        Assert.Equal(attempts, storage.Inner.KviAttempts.Count); // Unchanged: no second identical KVI.
        Assert.DoesNotContain(intendedId, storage.Deleted);
        await Change(db, 100);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        await maintenance.RunDueAsync(default);
        Assert.DoesNotContain(intendedId, storage.Deleted); // Superseded and marked, but not yet due.
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        attempts = storage.Inner.KviAttempts.Count;
        await maintenance.RunDueAsync(default);
        Assert.Equal(attempts, storage.Inner.KviAttempts.Count);
        Assert.Contains(intendedId, storage.Deleted); // An unchanged database still collects due deletions.
        using var cache = new InMemoryReplicationFileStorage();
        await using var restoredFiles = new ReplicationFileSet(cache, storage);
        await restoredFiles.InitializeAsync();
        using var restored = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        { FileCollection = restoredFiles, Compression = new NoCompressionStrategy(), CompactorScheduler = null });
        using var expected = db.StartReadOnlyTransaction();
        using var actual = restored.StartReadOnlyTransaction();
        Assert.Equal(expected.GetCommitUlong(), actual.GetCommitUlong());
        using var expectedCursor = expected.CreateCursor();
        using var actualCursor = actual.CreateCursor();
        Assert.Equal(expectedCursor.GetKeyValueCount([]), actualCursor.GetKeyValueCount([]));
        Assert.True(actualCursor.FindExactKey(new byte[] { 200 }));
        Span<byte> buffer = stackalloc byte[2048];
        Assert.Equal(new byte[2000], actualCursor.GetValueSpan(ref buffer).ToArray());
    }
}
