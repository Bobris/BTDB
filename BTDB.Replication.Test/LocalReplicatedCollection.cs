using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication.Test;

// Local backing for tests which explicitly exercise replication semantics.
sealed class LocalReplicatedCollection(InMemoryReplicationFileStorage inner) : IFileReplicatedCollection
{
    public ValueTask RefreshRemoteInventoryAsync(CancellationToken cancellation = default) => InitializeAsync(cancellation);
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
    public ValueTask PrefetchAsync(uint id, CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        var file = inner.GetFile(id) ?? throw new FileNotFoundException($"File {id} is missing.");
        file.AdvisePrefetch();
        return ValueTask.CompletedTask;
    }
    public uint GetRemoteCount() => inner.GetCount();
    public IFileCollectionFile? GetRemoteFile(uint id) => inner.GetFile(id);
    public IEnumerable<IFileCollectionFile> RemoteEnumerate() => inner.Enumerate();
    public IFileCollectionFile AddFile(string hint) => inner.AddFile(hint);
    public IFileCollectionFile AddFile(string hint, FileIdParity parity) =>
        inner.AddFile(hint, parity);
    public KVFileType? GetFileType(uint id) => inner.GetFileType(id);
    public IFileCollectionFile GetFile(uint id) => inner.GetFile(id);
    public uint GetCount() => inner.GetCount();
    public IEnumerable<IFileCollectionFile> Enumerate() => inner.Enumerate();
    public void ConcurrentTemporaryTruncate(uint id, uint offset) => inner.ConcurrentTemporaryTruncate(id, offset);
    public void Dispose() { }
}
