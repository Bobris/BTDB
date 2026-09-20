using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Allocators;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

public class AsyncOpenFailureTest
{
    sealed class FailingFinalPrefetch(IFileCollection inner, Action failure) : IFileReplicatedCollection
    {
        public ValueTask RefreshRemoteInventoryAsync(CancellationToken cancellation = default) => throw new InvalidOperationException("Unexpected refresh.");
        public uint GetLocalFileId(uint remoteId) => remoteId;
        public ValueTask<uint> PublishPureValuesAsync(KeyIndexFileSource source, CancellationToken cancellation = default) =>
            throw new NotSupportedException("This test collection does not publish remote files.");
        public ValueTask InitializeAsync(CancellationToken cancellation = default)
        {
            cancellation.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }
        public ValueTask<IFileInfo> ReadFileInfoAsync(uint id, CancellationToken cancellation = default)
        {
            cancellation.ThrowIfCancellationRequested();
            return new(FileCollectionWithFileInfos.ReadFileInfo(inner.GetFile(id)));
        }
        public KVFileType? GetFileType(uint id) => null;
        public IFileCollectionFile AddFile(string hint, FileIdParity parity) => throw new InvalidOperationException("Unexpected allocation.");
        readonly HashSet<uint> _requested = new();
        public ValueTask PrefetchAsync(uint id, CancellationToken cancellation = default)
        {
            if (!_requested.Add(id)) failure(); // TRL replay fetched it; its next request is the final prefetch.
            return ValueTask.CompletedTask;
        }
        public uint GetRemoteCount() => inner.GetCount();
        public IFileCollectionFile? GetRemoteFile(uint id) => inner.GetFile(id);
        public IEnumerable<IFileCollectionFile> RemoteEnumerate() => inner.Enumerate();
        public IFileCollectionFile AddFile(string hint) => inner.AddFile(hint);
        public IFileCollectionFile GetFile(uint id) => inner.GetFile(id);
        public uint GetCount() => inner.GetCount();
        public IEnumerable<IFileCollectionFile> Enumerate() => inner.Enumerate();
        public void ConcurrentTemporaryTruncate(uint id, uint offset) => inner.ConcurrentTemporaryTruncate(id, offset);
        public void Dispose() { }
    }

    sealed class UnopenedCollection : IFileReplicatedCollection
    {
        public ValueTask RefreshRemoteInventoryAsync(CancellationToken cancellation = default) => throw new InvalidOperationException("Unexpected refresh.");
        public uint GetLocalFileId(uint remoteId) => throw new InvalidOperationException("Unexpected mapping access.");
        public ValueTask<uint> PublishPureValuesAsync(KeyIndexFileSource source, CancellationToken cancellation = default) => throw new InvalidOperationException("Unexpected publication.");
        public ValueTask PrefetchAsync(uint id, CancellationToken cancellation = default) => throw new InvalidOperationException("Unexpected prefetch.");
        public ValueTask<IFileInfo> ReadFileInfoAsync(uint id, CancellationToken cancellation = default) => throw new InvalidOperationException("Unexpected metadata access.");
        public KVFileType? GetFileType(uint id) => throw new InvalidOperationException("Unexpected type access.");
        public IFileCollectionFile AddFile(string hint, FileIdParity parity) => throw new InvalidOperationException("Unexpected allocation.");

        public uint GetRemoteCount() => throw new InvalidOperationException("Unexpected remote inventory access.");
        public IFileCollectionFile? GetRemoteFile(uint id) => throw new InvalidOperationException("Unexpected remote file access.");
        public IEnumerable<IFileCollectionFile> RemoteEnumerate() => throw new InvalidOperationException("Unexpected remote enumeration.");
        public ValueTask InitializeAsync(CancellationToken cancellation = default) => throw new InvalidOperationException("Unexpected initialization.");
        public IEnumerable<IFileCollectionFile> Enumerate() => throw new InvalidOperationException("Unexpected enumeration.");
        public IFileCollectionFile AddFile(string hint) => throw new InvalidOperationException("Unexpected allocation.");
        public IFileCollectionFile GetFile(uint id) => throw new InvalidOperationException("Unexpected file access.");
        public uint GetCount() => throw new InvalidOperationException("Unexpected inventory access.");
        public void ConcurrentTemporaryTruncate(uint id, uint offset) => throw new InvalidOperationException("Unexpected truncation.");
        public void Dispose() { }
    }

    [Theory]
    [InlineData(false, 0ul)]
    [InlineData(false, 42ul)]
    [InlineData(false, ulong.MaxValue)]
    [InlineData(true, 0ul)]
    [InlineData(true, 42ul)]
    [InlineData(true, ulong.MaxValue)]
    public async Task HistoryOptionsAreRejectedBeforeAccessingFiles(bool preserve, ulong value)
    {
        using var allocator = new LeakDetectorWrapperAllocator(new MallocAllocator());
        var options = new KeyValueDBOptions
        {
            FileCollection = new UnopenedCollection(), Allocator = allocator, CompactorScheduler = null,
            OpenUpToCommitUlong = preserve ? null : value,
            PreserveHistoryUpToCommitUlong = preserve ? value : null
        };
        var error = await Assert.ThrowsAsync<ArgumentException>(() => BTreeKeyValueDB.OpenAsync(options).AsTask());
        Assert.Contains(preserve ? nameof(options.PreserveHistoryUpToCommitUlong) : nameof(options.OpenUpToCommitUlong), error.Message);
        Assert.Equal(0ul, allocator.QueryAllocations().Count);
    }

    [Fact]
    public async Task HistoryRetentionCannotBeEnabledAfterAsyncOpen()
    {
        using var files = new InMemoryReplicationFileStorage();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(files), CompactorScheduler = null
        });
        Assert.Null(db.PreserveHistoryUpToCommitUlong);
        Assert.Throws<InvalidOperationException>(() => db.PreserveHistoryUpToCommitUlong = 42);
        Assert.Null(db.PreserveHistoryUpToCommitUlong);
        db.PreserveHistoryUpToCommitUlong = null;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FinalPrefetchFailureFreesNativeRootsWithoutAppendingToTrl(bool cancel)
    {
        using var files = new InMemoryReplicationFileStorage();
        using (var original = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            FileCollection = files, CompactorScheduler = null, Compression = new NoCompressionStrategy()
        }))
        {
            using (var tr = await original.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue([1], new byte[100]);
                tr.Commit();
            }
            original.CreateKvi(default);
        }
        var trl = files.GetFile(1)!;
        var before = new byte[checked((int)trl.GetSize())];
        trl.RandomRead(before, 0, false);
        using var allocator = new LeakDetectorWrapperAllocator(new MallocAllocator());
        using var cancellation = new CancellationTokenSource();
        var failing = new FailingFinalPrefetch(files, () =>
        {
            if (cancel)
            {
                cancellation.Cancel();
                cancellation.Token.ThrowIfCancellationRequested();
            }
            throw new IOException("Prefetch failed.");
        });
        var options = new KeyValueDBOptions
        {
            FileCollection = failing, Allocator = allocator, CompactorScheduler = null,
            Compression = new NoCompressionStrategy()
        };
        if (cancel)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => BTreeKeyValueDB.OpenAsync(options, cancellation.Token).AsTask());
        else
            await Assert.ThrowsAsync<IOException>(() => BTreeKeyValueDB.OpenAsync(options).AsTask());
        Assert.Equal(0ul, allocator.QueryAllocations().Count);
        var after = new byte[checked((int)trl.GetSize())];
        trl.RandomRead(after, 0, false);
        Assert.Equal(before, after);
    }
}
