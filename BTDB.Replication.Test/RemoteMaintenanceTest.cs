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
    internal sealed class Storage(LeaseAuthority authority) : IRemoteMaintenanceStorage, IDisposable
    {
        internal readonly CheckpointPublisherTest.Storage Inner = new();
        readonly Dictionary<uint, int> _versions = new();
        public readonly List<uint> Deleted = new();
        public Action? DelayedDelete;
        public bool DelayDelete;
        string Version(uint id) => Inner.Describe(id).Version + ":" + _versions.GetValueOrDefault(id);
        public async IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync([EnumeratorCancellation] CancellationToken cancellation)
        {
            await foreach (var file in Inner.EnumerateAsync(cancellation))
                yield return new(file.FileId.ToString(), file.FileId, file.FileType, Version(file.FileId));
        }
        public ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            if (!authority.IsValid) throw new InvalidOperationException();
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
            _versions[id] = _versions.GetValueOrDefault(id) + 1;
            return ValueTask.FromResult(true);
        }
        public IAsyncEnumerable<RemoteFile> EnumerateAsync(CancellationToken cancellation) => Inner.EnumerateAsync(cancellation);
        public ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation) => Inner.ReadAsync(file, offset, buffer, cancellation);
        public ValueTask EnsurePureValuesAsync(uint id, KeyIndexFileSource source, CancellationToken cancellation) => Inner.EnsurePureValuesAsync(id, source, cancellation);
        public ValueTask PublishKeyIndexAsync(uint id, KeyIndexSnapshot snapshot, IReadOnlyDictionary<uint, uint> map, CancellationToken cancellation) => Inner.PublishKeyIndexAsync(id, snapshot, map, cancellation);
        public void Dispose() => Inner.Dispose();
        public void Add(uint id, KVFileType type)
        {
            Inner.Files.ImportFile(id, type == KVFileType.PureValues ? "pvl" : "kvi");
            Inner.Types.Add(id, type);
        }
    }

    static LeaseAuthority Lease(DeterministicScheduler clock)
    {
        var authority = new LeaseAuthority(clock.CreateScope("leader"), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        return authority;
    }

    [Fact]
    public async Task ConfirmedCheckpointCleanupDelaysDeletionPreservesClosureAndNeverReusesHighestId()
    {
        var clock = new DeterministicScheduler(901);
        var authority = Lease(clock);
        using var storage = new Storage(authority);
        storage.Add(2, KVFileType.PureValues);
        storage.Add(4, KVFileType.PureValues);
        storage.Add(6, KVFileType.KeyIndex);
        storage.Add(8, KVFileType.KeyIndex);
        storage.Add(10, KVFileType.PureValues); // Highest allocated orphan anchors allocation.
        var gc = new RemoteGarbageCollector(storage, authority, clock.CreateScope("gc"), TimeSpan.FromTicks(10));
        var checkpoint = new PublishedCheckpoint(8, 7, new HashSet<uint> { 4 });
        await gc.CollectAsync(checkpoint, default);
        Assert.Empty(storage.Deleted);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        await gc.CollectAsync(checkpoint, default);
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
        using var storage = new Storage(authority);
        storage.Add(2, KVFileType.PureValues);
        storage.Add(4, KVFileType.KeyIndex);
        var gc = new RemoteGarbageCollector(storage, authority, clock.CreateScope("gc"), TimeSpan.Zero);
        storage.DelayDelete = true;
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
        using var storage = new Storage(authority);
        await using var files = new ReplicationFileSet(local, storage);
        using var canonical = CheckpointPublisherTest.CreateCanonical(db, capture, storage.Inner, authority);
        using var maintenance = new ReplicationMaintenance(db, files, canonical, storage, authority,
            clock.CreateScope("maintenance"), TimeSpan.FromTicks(10), TimeSpan.Zero);
        storage.Inner.FailKvi = true;
        await Assert.ThrowsAsync<IOException>(() => maintenance.RunDueAsync(default).AsTask());
        Assert.Empty(storage.Deleted);
        var intendedId = storage.Inner.KviAttempts.Single();
        storage.Inner.FailKvi = false;
        await maintenance.RunDueAsync(default);
        Assert.All(storage.Inner.KviAttempts, id => Assert.Equal(intendedId, id));
        Assert.DoesNotContain(local.Enumerate(), f => local.GetFileType(f.Index) == KVFileType.KeyIndex);
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        await maintenance.RunDueAsync(default);
        Assert.Contains(intendedId, storage.Deleted);
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
