using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDB.Replication.Test;

public class ReplicationFileSetTest
{
    [Fact]
    public async Task RefreshReplacesInventoryAndVersionsWithoutDeletingLocalFiles()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        AddRemote(remote, 2, [1]);
        AddRemote(remote, 4, [2]);
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        await files.PrefetchAsync(2);
        await files.PrefetchAsync(4);
        var removedLocal = local.GetFile(files.GetLocalFileId(4));
        var unpublished = Add(local, 6, [9]);
        remote.Files.GetFile(2).Remove();
        remote.Types.Remove(2);
        AddRemote(remote, 2, [3, 4]);
        remote.Files.GetFile(4).Remove();
        AddRemote(remote, 6, [5]);
        await files.RefreshRemoteInventoryAsync();
        Assert.Equal(2u, files.GetRemoteCount());
        Assert.Null(files.GetRemoteFile(4));
        Assert.Equal(2ul, files.GetRemoteFile(2)!.GetSize());
        Assert.Same(removedLocal, local.GetFile(4));
        Assert.Same(unpublished, local.GetFile(6));
        Assert.NotEqual(6u, files.GetLocalFileId(6));
        await files.PrefetchAsync(2);
        await files.PrefetchAsync(6);
        var bytes = new byte[2];
        local.GetFile(2).RandomRead(bytes, 0, false);
        Assert.Equal(new byte[] { 3, 4 }, bytes);
        Assert.Same(unpublished, local.GetFile(6));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedRefreshKeepsPreviousInventory(bool canceled)
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        AddRemote(remote, 2, [1]);
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        var previous = files.GetRemoteFile(2);
        remote.BeforeEnumerate = _ => throw (canceled ? new OperationCanceledException() : new IOException());
        await Assert.ThrowsAnyAsync<Exception>(() => files.RefreshRemoteInventoryAsync().AsTask());
        Assert.Same(previous, files.GetRemoteFile(2));
        Assert.Equal(1u, files.GetRemoteCount());
        remote.BeforeEnumerate = null;
        AddRemote(remote, 4, [2]);
        await files.RefreshRemoteInventoryAsync();
        Assert.Equal(2u, files.GetRemoteCount());
    }

    static IFileCollectionFile Add(InMemoryReplicationFileStorage files, uint id, byte[] bytes, string hint = "pvl")
    {
        var file = files.ImportFile(id, hint);
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(bytes);
        writer.Flush();
        return file;
    }

    static KeyIndexFileSource Source(IFileCollectionFile file) =>
        new(file.Index, KVFileType.PureValues, file.GetSize(), 0, file);

    static RemoteFile AddRemote(CheckpointPublisherTest.Storage remote, uint id, byte[] bytes)
    {
        Add(remote.Files, id, bytes);
        remote.Types.Add(id, KVFileType.PureValues);
        return remote.Describe(id);
    }

    sealed class CacheLogger : IKeyValueDBLogger
    {
        public readonly List<string> Messages = new();
        public void LogInfo(string message) => Messages.Add(message);
        public void ReportTransactionLeak(IKeyValueDBTransaction transaction) { }
        public void CompactionStart(ulong totalWaste) { }
        public void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory) { }
        public void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration, ulong beforeCompressionSize) { }
        public void TransactionLogCreated(uint fileId) { }
        public void FileMarkedForDelete(uint fileId) { }
    }

    sealed class ObservedStorage(InMemoryReplicationFileStorage inner) : InMemoryReplicationFileStorage
    {
        readonly Dictionary<uint, ObservedFile> _files = new();
        public int Reads;
        public bool ForbidReads;
        public bool ForbidSize;
        IFileCollectionFile Wrap(IFileCollectionFile file)
        {
            if (!_files.TryGetValue(file.Index, out var wrapped) || !ReferenceEquals(wrapped.Inner, file))
                _files[file.Index] = wrapped = new(this, file);
            return wrapped;
        }
        public override IFileCollectionFile AddFile(string hint) => Wrap(inner.AddFile(hint));
        public override IFileCollectionFile AddFile(string hint, FileIdParity parity) => Wrap(inner.AddFile(hint, parity));
        public override IFileCollectionFile ImportFile(uint id, string hint) => Wrap(inner.ImportFile(id, hint));
        public override IFileCollectionFile GetFile(uint id) => inner.GetFile(id) is { } file ? Wrap(file) : null!;
        public override KVFileType? GetFileType(uint id) => inner.GetFileType(id);
        public override uint GetCount() => inner.GetCount();
        public override IEnumerable<IFileCollectionFile> Enumerate() => inner.Enumerate().Select(Wrap);
        public override void ConcurrentTemporaryTruncate(uint id, uint offset) => inner.ConcurrentTemporaryTruncate(id, offset);
        public override void Dispose() { }

        sealed class ObservedFile(ObservedStorage owner, IFileCollectionFile inner) : IFileCollectionFile
        {
            internal readonly IFileCollectionFile Inner = inner;
            public uint Index => Inner.Index;
            public ulong GetSize()
            {
                Assert.False(owner.ForbidSize);
                return Inner.GetSize();
            }
            public IMemReader GetExclusiveReader() => Inner.GetExclusiveReader();
            public void AdvisePrefetch() => Inner.AdvisePrefetch();
            public void RandomRead(Span<byte> bytes, ulong offset, bool doNotCache)
            {
                Assert.False(owner.ForbidReads);
                owner.Reads++;
                Inner.RandomRead(bytes, offset, doNotCache);
            }
            public IMemWriter GetAppenderWriter() => Inner.GetAppenderWriter();
            public IMemWriter GetExclusiveAppenderWriter() => Inner.GetExclusiveAppenderWriter();
            public void HardFlush() => Inner.HardFlush();
            public void HardFlushTruncateSwitchToReadOnlyMode() => Inner.HardFlushTruncateSwitchToReadOnlyMode();
            public void HardFlushTruncateSwitchToDisposedMode() => Inner.HardFlushTruncateSwitchToDisposedMode();
            public void Remove() => Inner.Remove();
        }
    }

    [Theory]
    [InlineData("extension", 0)]
    [InlineData("unknown-extension", 0)]
    [InlineData("missing-sha", 0)]
    [InlineData("active", 0)]
    [InlineData("length", 0)]
    [InlineData("sha", 1)]
    [InlineData("matching", 1)]
    public async Task InitializationChecksExtensionThenLengthThenShaAndLogsRemovalReasons(string kind, int expectedReads)
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var extensionMismatch = kind is "extension" or "unknown-extension";
        var observed = new ObservedStorage(local)
        {
            ForbidReads = expectedReads == 0, ForbidSize = extensionMismatch
        };
        var logger = new CacheLogger();
        Add(local, 2, kind == "length" ? [1] : kind == "sha" ? [9, 9, 9] : [1, 2, 3],
            kind == "extension" ? "trl" : kind == "unknown-extension" ? "other" : "pvl");
        remote.OmitChecksum = kind == "missing-sha";
        remote.IsSealed = kind != "active";
        AddRemote(remote, 2, [1, 2, 3]);
        var remoteReads = 0;
        remote.BeforeRead = (_, _, _) => { remoteReads++; return ValueTask.CompletedTask; };
        await using var files = new ReplicationFileSet(observed, remote, logger: logger);
        await files.InitializeAsync();
        Assert.Equal(expectedReads, observed.Reads);
        Assert.Equal(0, remoteReads);
        Assert.Equal(kind == "matching", local.GetFile(2) != null);
        if (kind == "matching") Assert.Empty(logger.Messages);
        else
        {
            var message = Assert.Single(logger.Messages);
            Assert.Contains("local cache file 2 mapped to remote file 2", message);
            Assert.Contains(kind switch
            {
                "extension" or "unknown-extension" => "extension mismatch",
                "length" => "length mismatch (local 1, remote 3)",
                "sha" => "SHA-256 validation failed",
                "missing-sha" => "SHA-256 metadata is missing",
                _ => "remote file is active"
            }, message);
        }
        observed.ForbidSize = false;
        await files.PrefetchAsync(2);
        Assert.Equal(expectedReads, observed.Reads);
        Assert.Equal(kind == "matching" ? 0 : 1, remoteReads);
        var bytes = new byte[3];
        local.GetFile(2)!.RandomRead(bytes, 0, false);
        Assert.Equal(new byte[] { 1, 2, 3 }, bytes);
    }

    [Fact]
    public async Task RememberedPvlDoesNotAliasAnotherRemoteFileWithTheSameNumericId()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var cached = Add(local, 100, [1]);
        var first = AddRemote(remote, 2, [1]);
        AddRemote(remote, 100, [9]);
        await using var files = new ReplicationFileSet(local, remote);
        files.RememberVerifiedPureValues(Source(cached), first);
        await files.InitializeAsync();
        Assert.Equal(100u, files.GetLocalFileId(2));
        var secondLocalId = files.GetLocalFileId(100);
        Assert.NotEqual(100u, secondLocalId);
        await files.PrefetchAsync(2);
        await files.PrefetchAsync(100);
        var bytes = new byte[1];
        files.GetFile(100).RandomRead(bytes, 0, false);
        Assert.Equal(1, bytes[0]);
        files.GetFile(secondLocalId).RandomRead(bytes, 0, false);
        Assert.Equal(9, bytes[0]);
    }

    [Fact]
    public async Task LeaderPublicationThroughCollectionRetriesSameRemoteIdAndRemembersMapping()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage { FailPvl = true };
        AddRemote(remote, 2, [9]);
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        var source = Source(files.AddFile("pvl", FileIdParity.Even));
        IFileReplicatedCollection collection = files;
        await Assert.ThrowsAsync<IOException>(() => collection.PublishPureValuesAsync(source).AsTask());
        Assert.Throws<FileNotFoundException>(() => collection.GetLocalFileId(4));
        remote.FailPvl = false;
        Assert.Equal(4u, await collection.PublishPureValuesAsync(source));
        Assert.Equal(source.FileId, collection.GetLocalFileId(4));
        Assert.Equal(4u, await collection.PublishPureValuesAsync(source));
        Assert.Equal(new uint[] { 4, 4 }, remote.PvlAttempts); // Retry, but no third upload for the confirmed mapping.
    }

    [Fact]
    public async Task SessionMappingRetainsMappedCacheButIsLostOnRestart()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var cached = Add(local, 100, [1, 2, 3]);
        var selected = AddRemote(remote, 2, [1, 2, 3]);
        var reads = 0;
        remote.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        await using (var files = new ReplicationFileSet(local, remote))
        {
            files.RememberVerifiedPureValues(Source(cached), selected);
            await files.InitializeAsync();
            Assert.Equal(100u, files.GetLocalFileId(2));
            Assert.Same(cached, files.GetFile(100));
            await files.PrefetchAsync(2);
            Assert.Equal(0, reads);
            Assert.Null(files.GetFile(2));
            Assert.Equal(2u, await files.PublishPureValuesAsync(Source(cached), default));
            cached.Remove();
            Assert.Equal(100u, files.GetLocalFileId(2)); // The ID assignment is stable even while its cache is absent.
            await files.PrefetchAsync(2);
            Assert.NotNull(files.GetFile(100));
            Assert.Equal(1, reads);
        }
        await using var restarted = new ReplicationFileSet(local, remote);
        await restarted.InitializeAsync();
        Assert.Equal(2u, restarted.GetLocalFileId(2));
        Assert.Null(restarted.GetFile(100));
        await restarted.PrefetchAsync(2);
        Assert.NotNull(restarted.GetFile(2));
        Assert.Equal(2, reads); // No persisted map or cross-ID content search.
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UninitializedInventoryAndOpenFailWithoutImplicitDiscovery(bool emptyRemote)
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var localFile = Add(local, 100, [9]);
        var logger = new CacheLogger();
        if (!emptyRemote) AddRemote(remote, 2, [1, 2, 3]);
        var discoveries = 0;
        remote.BeforeEnumerate = _ => { discoveries++; return ValueTask.CompletedTask; };
        await using var files = new ReplicationFileSet(local, remote, logger: logger);
        Assert.Same(localFile, files.GetFile(100));
        Assert.Equal(1u, files.GetCount());
        Assert.Single(files.Enumerate());
        Assert.Throws<InvalidOperationException>(() => files.GetRemoteCount());
        Assert.Throws<InvalidOperationException>(() => files.GetRemoteFile(2));
        Assert.Throws<InvalidOperationException>(() => files.RemoteEnumerate());
        Assert.Throws<InvalidOperationException>(() => files.GetFileType(2));
        await Assert.ThrowsAsync<InvalidOperationException>(() => files.ReadFileInfoAsync(2).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() => files.PrefetchAsync(2).AsTask());
        var options = new KeyValueDBOptions
        {
            FileCollection = files, Logger = logger, ReadOnly = true, CompactorScheduler = null
        };
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => BTreeKeyValueDB.OpenAsync(options).AsTask());
        Assert.Contains(nameof(files.InitializeAsync), error.Message);
        Assert.Equal(0, discoveries);
        Assert.Same(localFile, files.GetFile(100));

        await files.InitializeAsync();
        Assert.Contains("local cache file 100: no corresponding file in the remote inventory", Assert.Single(logger.Messages));
        Assert.Equal(emptyRemote ? 0u : 1u, files.GetRemoteCount());
        using var db = await BTreeKeyValueDB.OpenAsync(options);
        Assert.Same(logger, db.Logger);
        Assert.Equal(1, discoveries);
        Assert.Equal(0u, local.GetCount()); // Initialization removes files absent from the remote inventory.
    }

    [Fact]
    public async Task FailedDiscoveryPreservesLocalFilesUntilSuccessfulRetry()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var selected = Add(local, 2, [9]);
        var unselected = Add(local, 100, [8]);
        AddRemote(remote, 2, [1]);
        remote.BeforeEnumerate = _ => throw new IOException("Listing failed.");
        await using var files = new ReplicationFileSet(local, remote);
        await Assert.ThrowsAsync<IOException>(() => files.InitializeAsync().AsTask());
        Assert.Same(selected, local.GetFile(2));
        Assert.Same(unselected, local.GetFile(100));
        remote.BeforeEnumerate = null;
        await files.InitializeAsync();
        Assert.Null(local.GetFile(2)); // Matching IDs with different bytes are removed during initialization.
        Assert.Null(local.GetFile(100));
        Assert.Equal(0u, local.GetCount());
    }

    [Fact]
    public async Task PendingOrCancelledInitializationDoesNotExposeAnEmptyInventory()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        AddRemote(remote, 2, [1]);
        var unselected = Add(local, 100, [9]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        remote.BeforeEnumerate = async ct =>
        {
            entered.SetResult();
            await release.Task.WaitAsync(ct);
        };
        await using var files = new ReplicationFileSet(local, remote);
        using var cancellation = new CancellationTokenSource();
        var initialization = files.InitializeAsync(cancellation.Token).AsTask();
        await entered.Task;
        Assert.Throws<InvalidOperationException>(() => files.GetRemoteCount());
        await Assert.ThrowsAsync<InvalidOperationException>(() => files.PrefetchAsync(2).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() => BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, CompactorScheduler = null
        }).AsTask());
        Assert.Same(unselected, files.GetFile(100));
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => initialization);
        Assert.Same(unselected, files.GetFile(100));
        Assert.Throws<InvalidOperationException>(() => files.RemoteEnumerate());
        remote.BeforeEnumerate = null;
        await files.InitializeAsync();
        Assert.Equal(1u, files.GetRemoteCount());
        Assert.Null(files.GetFile(100));
        await files.PrefetchAsync(2);
        Assert.Equal(1ul, files.GetFile(2).GetSize());
    }

    [Fact]
    public async Task LocalStorageAndRemoteInventoryRemainSeparate()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        Add(local, 2, [9, 9]);
        Add(local, 10000, [8]);
        AddRemote(remote, 2, [1, 2, 3]);
        AddRemote(remote, 4, [4]);
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        var reads = 0;
        remote.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };

        Assert.Equal(0u, files.GetCount());
        Assert.Equal(2u, files.GetRemoteCount());
        Assert.Empty(files.Enumerate());
        Assert.Equal(new uint[] { 2, 4 }, files.RemoteEnumerate().Select(f => f.Index).Order());
        Assert.Null(files.GetFile(2));
        Assert.Null(files.GetFile(10000));
        Assert.Null(files.GetFile(4));
        Assert.Null(files.GetRemoteFile(10000));
        await Assert.ThrowsAsync<FileNotFoundException>(() => files.PrefetchAsync(10000).AsTask());
        var selected = files.GetRemoteFile(2)!;
        Assert.Equal(3ul, selected.GetSize());
        Assert.Equal(0, reads); // Neither inventory access nor local lookup downloads anything.

        var bytes = new byte[3];
        selected.RandomRead(bytes, 0, false);
        Assert.Equal(new byte[] { 1, 2, 3 }, bytes);
        Assert.Null(files.GetFile(2)); // Remote reads do not replace the cache.
        Assert.Throws<NotSupportedException>(() => selected.GetAppenderWriter());
        Assert.Throws<NotSupportedException>(() => selected.Remove());

        await files.PrefetchAsync(2);
        Assert.NotNull(files.GetFile(2));
        Assert.Same(local.GetFile(2), files.GetFile(2));
        files.GetFile(2).RandomRead(bytes, 0, false);
        Assert.Equal(new byte[] { 1, 2, 3 }, bytes);
        files.GetFile(2).Remove();
        Assert.Null(files.GetFile(2));
        Assert.Same(selected, files.GetRemoteFile(2));
        Assert.Equal(2u, files.GetRemoteCount());
        await files.PrefetchAsync(2); // Eviction must not invalidate the remote handle or its download state.
        Assert.Equal(3ul, files.GetFile(2).GetSize());

        var created = files.AddFile("pvl", FileIdParity.Even);
        Assert.Same(created, files.GetFile(created.Index));
        Assert.Null(files.GetRemoteFile(created.Index));
        await files.InitializeAsync(); // Repeated initialization must not delete files created in this session.
        Assert.Same(created, files.GetFile(created.Index));
        Assert.Equal(2u, files.GetCount());
        Assert.Equal(2u, files.GetRemoteCount());
        Assert.Equal(new uint[] { 2, 4 }, files.RemoteEnumerate().Select(f => f.Index).Order());
    }

    [Fact]
    public async Task EqualLocalAndRemoteIdsDoNotImplyEqualFilesOrSharedAllocation()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var source = Source(Add(local, 2, [1, 2, 3]));
        Add(local, 10000, [9]);
        var other = AddRemote(remote, 2, [4, 5, 6]);
        var files = new ReplicationFileSet(local, remote);
        Assert.Throws<IOException>(() => files.RememberVerifiedPureValues(source, other));
        Assert.Equal(4u, await files.PublishPureValuesAsync(source, CancellationToken.None));
        Assert.Equal(2u, files.GetLocalFileId(4));
        Assert.Equal(new uint[] { 4 }, remote.PvlAttempts);
        Assert.Equal(10002u, local.AddFile("pvl", FileIdParity.Even).Index);
        Assert.Equal(6u, await files.AllocateRemoteFileIdAsync(CancellationToken.None));
        Assert.Equal(4u, await files.PublishPureValuesAsync(source, CancellationToken.None));
        Assert.Single(remote.PvlAttempts);
    }

    [Fact]
    public async Task ParallelDownloadsKeepExactIdsAndVerifiedPvlNeedsNoUpload()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage { ReadChunkSize = 997 };
        var bytes = Enumerable.Range(0, 150000).Select(i => (byte)i).ToArray();
        var low = AddRemote(remote, 2, bytes);
        var high = AddRemote(remote, 100, [7, 8, 9]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        remote.BeforeRead = async (file, offset, ct) =>
        {
            if (file.FileId == 2 && offset == 0)
            {
                entered.SetResult();
                await release.Task.WaitAsync(ct);
            }
        };
        var files = new ReplicationFileSet(local, remote);
        var pending = files.DownloadAsync(low).AsTask();
        await entered.Task;
        var completed = await files.DownloadAsync(high);
        Assert.Equal(100u, completed.Index);
        Assert.False(pending.IsCompleted);
        release.SetResult();
        var downloaded = await pending;
        var actual = new byte[bytes.Length];
        downloaded.RandomRead(actual, 0, false);
        Assert.Equal(bytes, actual);
        Assert.Equal(2u, await files.PublishPureValuesAsync(Source(downloaded), CancellationToken.None));
        Assert.Equal(100u, await files.PublishPureValuesAsync(Source(completed), CancellationToken.None));
        Assert.Empty(remote.PvlAttempts);
        Assert.Equal(102u, local.AddFile("pvl", FileIdParity.Even).Index);
    }

    [Theory]
    [InlineData("checksum")]
    [InlineData("version")]
    [InlineData("missing")]
    [InlineData("truncated")]
    [InlineData("cancelled")]
    public async Task FailedDownloadRemovesPartialFileAndDoesNotRememberPlacement(string fault)
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage { ReadChunkSize = 2 };
        var file = AddRemote(remote, 2, [1, 2, 3, 4]);
        using var cancellation = new CancellationTokenSource();
        remote.CorruptRead = fault == "checksum";
        remote.BeforeRead = (selected, offset, ct) =>
        {
            if (offset != 0)
            {
                if (fault is "version" or "missing") remote.Files.GetFile(2)!.Remove();
                if (fault == "version") Add(remote.Files, 2, [5, 6, 7, 8]);
                if (fault == "truncated") remote.TruncateRead = true;
                if (fault == "cancelled") cancellation.Cancel();
                ct.ThrowIfCancellationRequested();
            }
            return ValueTask.CompletedTask;
        };
        var files = new ReplicationFileSet(local, remote);
        if (fault == "cancelled")
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => files.DownloadAsync(file, cancellation.Token).AsTask());
        else
            await Assert.ThrowsAsync<IOException>(() => files.DownloadAsync(file, cancellation.Token).AsTask());
        Assert.Null(local.GetFile(2));
        Assert.Equal(0u, local.GetCount());
        // Retrying the same selected ID after a fresh listing is allowed and establishes the first receipt.
        remote.BeforeRead = null;
        remote.CorruptRead = remote.TruncateRead = false;
        if (remote.Files.GetFile(2) == null) Add(remote.Files, 2, [1, 2, 3, 4]);
        var restored = await files.DownloadAsync(remote.Describe(2));
        Assert.Equal(2u, await files.PublishPureValuesAsync(Source(restored), CancellationToken.None));
        Assert.Empty(remote.PvlAttempts);
    }

    [Fact]
    public async Task DownloadCollisionPreservesExistingLocalBytes()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var existing = Add(local, 2, [9, 9]);
        var file = AddRemote(remote, 2, [1, 2]);
        var files = new ReplicationFileSet(local, remote);
        await Assert.ThrowsAsync<InvalidOperationException>(() => files.DownloadAsync(file).AsTask());
        var bytes = new byte[2];
        existing.RandomRead(bytes, 0, false);
        Assert.Equal(new byte[] { 9, 9 }, bytes);
        Assert.Same(existing, local.GetFile(2));
    }

    [Fact]
    public async Task PrefetchQueuesAllRequestsButBoundsActiveTransfers()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        AddRemote(remote, 2, [1]);
        AddRemote(remote, 4, [2]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        remote.BeforeRead = async (_, _, ct) =>
        {
            Interlocked.Increment(ref reads);
            entered.TrySetResult();
            await release.Task.WaitAsync(ct);
        };
        await using var files = new ReplicationFileSet(local, remote, maxConcurrentDownloads: 1);
        await files.InitializeAsync();
        var first = files.PrefetchAsync(2).AsTask();
        await entered.Task;
        var second = files.PrefetchAsync(4).AsTask();
        Assert.Equal(1, reads);
        Assert.False(second.IsCompleted);
        release.SetResult();
        await Task.WhenAll(first, second);
        Assert.Equal(2, reads);
    }

    [Fact]
    public async Task PrefetchIsSharedAndCancellingOneWaiterDoesNotCancelAnother()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        AddRemote(remote, 2, [1, 2, 3]);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        remote.BeforeRead = async (_, _, ct) =>
        {
            Interlocked.Increment(ref reads);
            entered.TrySetResult();
            await release.Task.WaitAsync(ct);
        };
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        using var cancellation = new CancellationTokenSource();
        var first = files.PrefetchAsync(2, cancellation.Token).AsTask();
        await entered.Task;
        var second = files.PrefetchAsync(2).AsTask();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        Assert.False(second.IsCompleted);
        release.SetResult();
        await second;
        await files.PrefetchAsync(2);
        Assert.Equal(1, reads);
        Assert.Equal(3ul, files.GetFile(2).GetSize());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InitializationValidatesCacheBeforePrefetch(bool matching)
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        AddRemote(remote, 2, [1, 2, 3]);
        Add(local, 2, matching ? [1, 2, 3] : [4, 5, 6]);
        var reads = 0;
        remote.BeforeRead = (_, _, _) => { reads++; return ValueTask.CompletedTask; };
        await using var files = new ReplicationFileSet(local, remote);
        await files.InitializeAsync();
        Assert.Equal(matching, files.GetFile(2) != null);
        Assert.Equal(0, reads);
        await files.PrefetchAsync(2);
        var bytes = new byte[3];
        files.GetFile(2).RandomRead(bytes, 0, false);
        Assert.Equal(new byte[] { 1, 2, 3 }, bytes);
        Assert.Equal(matching ? 0 : 1, reads);
        Assert.Equal(2u, await files.PublishPureValuesAsync(Source(files.GetFile(2)), CancellationToken.None));
        Assert.Empty(remote.PvlAttempts);
    }

    [Fact]
    public async Task MissingChecksumDoesNotCreateVerifiedReuseReceipt()
    {
        using var local = new InMemoryReplicationFileStorage();
        using var remote = new CheckpointPublisherTest.Storage();
        var selected = AddRemote(remote, 2, [1, 2]) with { Sha256 = null };
        var files = new ReplicationFileSet(local, remote);
        var restored = await files.DownloadAsync(selected);
        Assert.Equal(4u, await files.PublishPureValuesAsync(Source(restored), CancellationToken.None));
        Assert.Equal(new uint[] { 4 }, remote.PvlAttempts);
    }
}
