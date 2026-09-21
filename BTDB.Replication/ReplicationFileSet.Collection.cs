using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication;

// Local storage operations never implicitly fetch remote files. Remote discovery and prefetch are explicit.
internal sealed partial class ReplicationFileSet : IFileReplicatedCollection, IAsyncDisposable
{
    volatile ConcurrentDictionary<uint, RemoteInventoryFile> _remoteFiles = new();
    readonly SemaphoreSlim _initialization = new(1);
    readonly CancellationTokenSource _lifetime = new();
    readonly object _creationLock = new();
    uint _lastOddId, _lastEvenId;
    volatile bool _initialized, _disposed;

    public async ValueTask InitializeAsync(CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_initialized) return;
        await _initialization.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_initialized) return;
            // Finish remote discovery before touching local cache contents.
            var inventory = new Dictionary<uint, RemoteInventoryFile>();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, _lifetime.Token);
            await foreach (var file in Remote.EnumerateAsync(linked.Token).ConfigureAwait(false))
            {
                if (file.FileId == 0 || !inventory.TryAdd(file.FileId, new(this, file)))
                    throw new IOException("The remote inventory contains an invalid or duplicate file ID.");
            }
            // Freeze mappings before pruning local storage. Remembered PVL placements may have different IDs.
            linked.Token.ThrowIfCancellationRequested();
            var retainedLocalIds = new HashSet<uint>();
            lock (_placementLock)
            {
                foreach (var id in inventory.Keys) ObserveId(id);
                foreach (var localId in _remoteToLocal.Values) ObserveId(localId);
                foreach (var (remoteId, file) in inventory)
                {
                    var localId = GetOrAssignLocalFileId(remoteId, file.Selected.FileType);
                    if (localId != remoteId && file.Selected.FileType != KVFileType.PureValues)
                        throw new InvalidOperationException("Only PVL files can have different local and remote IDs.");
                    retainedLocalIds.Add(localId);
                }
            }
            // Only a complete remote listing can prove that a local file is unselected.
            foreach (var localFile in Local.Enumerate().ToArray())
            {
                linked.Token.ThrowIfCancellationRequested();
                if (!retainedLocalIds.Contains(localFile.Index))
                    DiscardCachedFile(localFile, "no corresponding file in the remote inventory");
            }
            foreach (var (id, file) in inventory)
            {
                var localId = GetLocalFileId(id);
                if (Local.GetFile(localId) is { } candidate)
                {
                    if (ValidateCachedFile(candidate, file.Selected, linked.Token, out var reason))
                        file.UseValidatedLocal(candidate);
                    else
                        DiscardCachedFile(candidate, reason!, id);
                }
                _remoteFiles[id] = file;
                ObserveId(id);
                ObserveId(GetLocalFileId(id));
            }
            _initialized = true;
        }
        finally { _initialization.Release(); }
    }

    public async ValueTask RefreshRemoteInventoryAsync(CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        EnsureInitialized();
        await _initialization.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, _lifetime.Token);
            var inventory = new ConcurrentDictionary<uint, RemoteInventoryFile>();
            foreach (var file in _remoteFiles.Values)
                if (file.Pending is { IsCompleted: false })
                    throw new InvalidOperationException("Wait for prefetch operations before refreshing remote inventory.");
            await foreach (var file in Remote.EnumerateAsync(linked.Token).ConfigureAwait(false))
            {
                var handle = _remoteFiles.TryGetValue(file.FileId, out var previous) && previous.Selected == file
                    ? previous : new RemoteInventoryFile(this, file);
                if (file.FileId == 0 || !inventory.TryAdd(file.FileId, handle))
                    throw new IOException("The remote inventory contains an invalid or duplicate file ID.");
            }
            linked.Token.ThrowIfCancellationRequested();
            lock (_placementLock)
            {
                // Reserve above every observed local/remote ID before assigning new mappings.
                foreach (var file in Local.Enumerate()) ObserveId(file.Index);
                foreach (var id in inventory.Keys) ObserveId(id);
                foreach (var (id, file) in inventory)
                    ObserveId(GetOrAssignLocalFileId(id, file.Selected.FileType, preserveUnmappedLocal: true));
            }
            // Reconfirm publications with the current authority before reusing an old session receipt.
            foreach (var placement in _placements.Values) placement.Confirmed = false;
            _remoteFiles = inventory;
        }
        finally { _initialization.Release(); }
    }

    void EnsureInitialized()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("Call and await InitializeAsync before accessing the remote inventory or opening the database.");
    }

    void ObserveId(uint id)
    {
        if ((id & 1) != 0) _lastOddId = Math.Max(_lastOddId, id);
        else _lastEvenId = Math.Max(_lastEvenId, id);
    }

    bool OwnsSource(KeyIndexFileSource source) => ReferenceEquals(Local.GetFile(source.FileId), source.File);

    public async ValueTask PrefetchAsync(uint fileId, CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        EnsureInitialized();
        if (!_remoteFiles.TryGetValue(fileId, out var file))
            throw new FileNotFoundException($"File {fileId} is not in the remote inventory.");
        await file.GetLocalAsync(cancellation).ConfigureAwait(false);
    }

    public KVFileType? GetFileType(uint fileId)
    {
        EnsureInitialized();
        return _remoteFiles.TryGetValue(fileId, out var file) ? file.Selected.FileType : Local.GetFileType(fileId);
    }

    public async ValueTask<IFileInfo> ReadFileInfoAsync(uint fileId, CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        EnsureInitialized();
        if (!_remoteFiles.TryGetValue(fileId, out var file))
            return FileCollectionWithFileInfos.ReadFileInfo(Local.GetFile(fileId) ??
                throw new FileNotFoundException($"File {fileId} is not in either inventory."));
        var selected = file.Selected;

        // Headers are variable length (KVI Ulong metadata in particular). Grow only when parsing needs more bytes.
        var header = new byte[(int)Math.Min(128ul, selected.Length)];
        var filled = 0;
        while (true)
        {
            while (filled < header.Length)
            {
                var read = await Remote.ReadAsync(selected, (ulong)filled, header.AsMemory(filled), cancellation)
                    .ConfigureAwait(false);
                if (read <= 0 || read > header.Length - filled) throw new IOException("Truncated remote file header.");
                filled += read;
            }
            try { return ParseHeader(header, (ulong)header.Length == selected.Length); }
            catch (EndOfStreamException) when ((ulong)header.Length < selected.Length)
            {
                Array.Resize(ref header, (int)Math.Min((ulong)checked(Math.Max(1, header.Length) * 2), selected.Length));
            }
        }
    }

    static IFileInfo ParseHeader(byte[] bytes, bool complete)
    {
        using var controller = new ReadOnlyMemoryMemReader(bytes);
        var reader = new MemReader(controller);
        return FileCollectionWithFileInfos.ReadFileInfo(ref reader, throwOnEndOfStream: !complete);
    }

    public IFileCollectionFile? GetRemoteFile(uint index)
    {
        EnsureInitialized();
        return _remoteFiles.GetValueOrDefault(index);
    }

    public uint GetRemoteCount()
    {
        EnsureInitialized();
        return (uint)_remoteFiles.Count;
    }

    public IEnumerable<IFileCollectionFile> RemoteEnumerate()
    {
        EnsureInitialized();
        return _remoteFiles.Values;
    }

    public IFileCollectionFile GetFile(uint index) => Local.GetFile(index);
    public uint GetCount() => Local.GetCount();
    public IEnumerable<IFileCollectionFile> Enumerate() => Local.Enumerate();
    public void ConcurrentTemporaryTruncate(uint index, uint offset) => Local.ConcurrentTemporaryTruncate(index, offset);

    public IFileCollectionFile AddFile(string humanHint) => AddFile(humanHint, FileIdParity.Any);

    public IFileCollectionFile AddFile(string humanHint, FileIdParity parity)
    {
        EnsureInitialized();
        lock (_creationLock)
        {
            var previous = parity switch
            {
                FileIdParity.Odd => _lastOddId,
                FileIdParity.Even => _lastEvenId,
                FileIdParity.Any => Math.Max(_lastOddId, _lastEvenId),
                _ => throw new ArgumentOutOfRangeException(nameof(parity))
            };
            var next = (ulong)previous + 1;
            if (parity != FileIdParity.Any && (next & 1) != (parity == FileIdParity.Odd ? 1ul : 0ul)) next++;
            if (next > uint.MaxValue) throw new InvalidOperationException("File IDs exhausted.");
            var id = (uint)next;
            ObserveId(id);
            return Local.ImportFile(id, humanHint);
        }
    }

    async Task<IFileCollectionFile> PopulateCacheAsync(RemoteFile selected)
    {
        await _downloads.WaitAsync(_lifetime.Token).ConfigureAwait(false);
        try
        {
            var localId = GetLocalFileId(selected.FileId);
            if (Local.GetFile(localId) is { } candidate)
            {
                if (ValidateCachedFile(candidate, selected, _lifetime.Token, out var reason)) return candidate;
                DiscardCachedFile(candidate, reason!, selected.FileId);
            }
            return await DownloadAsync(selected, _lifetime.Token).ConfigureAwait(false);
        }
        finally { _downloads.Release(); }
    }

    void DiscardCachedFile(IFileCollectionFile candidate, string reason, uint? remoteId = null)
    {
        Logger?.LogInfo($"Removing local cache file {candidate.Index}" +
                        (remoteId.HasValue ? $" mapped to remote file {remoteId.Value}" : "") + $": {reason}.");
        candidate.Remove();
        lock (_placementLock)
        {
            if (_placements.Remove(candidate.Index, out var placement)) _placedRemoteIds.Remove(placement.RemoteId);
        }
    }

    static string FileExtension(KVFileType? type) => type switch
    {
        KVFileType.TransactionLog => "trl",
        KVFileType.KeyIndex or KVFileType.KeyIndexWithCommitUlong or KVFileType.ModernKeyIndex or
            KVFileType.ModernKeyIndexWithUlongs => "kvi",
        KVFileType.PureValues => "pvl",
        KVFileType.PureValuesWithId => "hpv",
        KVFileType.HashKeyIndex => "hid",
        _ => "<unknown>"
    };

    bool ValidateCachedFile(IFileCollectionFile candidate, RemoteFile selected, CancellationToken cancellation,
        out string? reason)
    {
        var localExtension = FileExtension(Local.GetFileType(candidate.Index));
        var remoteExtension = FileExtension(selected.FileType);
        if (localExtension != remoteExtension || localExtension == "<unknown>")
        {
            reason = $"extension mismatch (local .{localExtension}, remote .{remoteExtension})";
            return false;
        }
        // Check cheap metadata before reading any local bytes.
        var length = candidate.GetSize();
        if (length != selected.Length)
        {
            reason = $"length mismatch (local {length}, remote {selected.Length})";
            return false;
        }
        if (!selected.IsSealed)
        {
            reason = "remote file is active and cannot reuse cached bytes";
            return false;
        }
        if (selected.Sha256 is null)
        {
            reason = "remote SHA-256 metadata is missing";
            return false;
        }
        try { VerifyRemoteInventoryFile(candidate, selected, cancellation); }
        catch (IOException error)
        {
            reason = $"SHA-256 validation failed: {error.Message}";
            return false;
        }
        reason = null;
        if (selected.FileType == KVFileType.PureValues)
        {
            lock (_placementLock)
            {
                // Another confirmed remote copy may already represent the same local PVL.
                if (!_placements.ContainsKey(candidate.Index))
                    AddPlacement(candidate.Index, new(selected.Length, selected.FileId, true));
            }
        }
        return true;
    }

    static void VerifyRemoteInventoryFile(IFileCollectionFile file, RemoteFile selected, CancellationToken cancellation)
    {
        using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256);
        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            for (ulong offset = 0; offset < selected.Length;)
            {
                cancellation.ThrowIfCancellationRequested();
                var count = (int)Math.Min((ulong)buffer.Length, selected.Length - offset);
                file.RandomRead(buffer.AsSpan(0, count), offset, false);
                hash.AppendData(buffer, 0, count);
                offset += (uint)count;
            }
            VerifyChecksum(hash, selected);
        }
        finally { System.Buffers.ArrayPool<byte>.Shared.Return(buffer); }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        await _lifetime.CancelAsync().ConfigureAwait(false);
        await _initialization.WaitAsync().ConfigureAwait(false);
        _initialization.Release();
        var pending = _remoteFiles.Values.Select(f => f.Pending).Where(t => t != null).ToArray();
        try { await Task.WhenAll(pending!).ConfigureAwait(false); }
        catch (Exception) { /* Prefetch callers observe transfer failures; disposal only drains cleanup. */ }
        // The caller owns the physical cache and remote adapter.
    }

    sealed class RemoteInventoryFile : IFileCollectionFile
    {
        readonly ReplicationFileSet _owner;
        readonly object _lock = new();
        Task<IFileCollectionFile>? _pending;
        IFileCollectionFile? _local;
        internal readonly RemoteFile Selected;
        public uint Index { get; }

        internal RemoteInventoryFile(ReplicationFileSet owner, RemoteFile selected)
        {
            _owner = owner;
            Selected = selected;
            Index = selected.FileId;
        }

        internal void UseValidatedLocal(IFileCollectionFile local) => _local = local;

        internal Task<IFileCollectionFile>? Pending { get { lock (_lock) return _pending; } }

        internal async ValueTask<IFileCollectionFile> GetLocalAsync(CancellationToken cancellation)
        {
            Task<IFileCollectionFile> pending;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_owner._disposed, this);
                cancellation.ThrowIfCancellationRequested();
                if (_local != null)
                {
                    if (ReferenceEquals(_owner.Local.GetFile(_local.Index), _local)) return _local;
                    _local = null;
                    _pending = null;
                }
                if (_pending is null || _pending.IsFaulted || _pending.IsCanceled)
                    _pending = _owner.PopulateCacheAsync(Selected);
                pending = _pending;
            }
            var local = await pending.WaitAsync(cancellation).ConfigureAwait(false);
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_owner._disposed, this);
                return _local = local;
            }
        }

        public ulong GetSize() => Selected.Length;
        public IMemReader GetExclusiveReader() => new FileCollectionFileReader(this);
        public void AdvisePrefetch() { }

        public void RandomRead(Span<byte> data, ulong position, bool doNotCache)
        {
            if (position > Selected.Length || (ulong)data.Length > Selected.Length - position)
                throw new EndOfStreamException();
            var buffer = ArrayPool<byte>.Shared.Rent(Math.Min(data.Length, 64 * 1024));
            try
            {
                while (!data.IsEmpty)
                {
                    var count = Math.Min(data.Length, buffer.Length);
                    var read = _owner.Remote.ReadAsync(Selected, position, buffer.AsMemory(0, count),
                        _owner._lifetime.Token).AsTask().GetAwaiter().GetResult();
                    if (read <= 0 || read > count) throw new IOException("Truncated remote file.");
                    buffer.AsSpan(0, read).CopyTo(data);
                    data = data[read..];
                    position += (uint)read;
                }
            }
            finally { ArrayPool<byte>.Shared.Return(buffer); }
        }

        public IMemWriter GetAppenderWriter() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public IMemWriter GetExclusiveAppenderWriter() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void HardFlush() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void HardFlushTruncateSwitchToReadOnlyMode() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void HardFlushTruncateSwitchToDisposedMode() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void Remove() => throw new NotSupportedException("Remote deletion requires the publication protocol.");
    }
}
