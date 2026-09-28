using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication;

// Local storage operations never implicitly fetch remote files. Remote discovery and prefetch are explicit.
public sealed partial class ReplicationFileSet : IFileReplicatedCollection, IAsyncDisposable
{
    // Replaced once, complete, by initialization. Only database open changes it afterwards (discarding an unfinished
    // TRL or publishing the legacy transition header), before any concurrent reader exists.
    volatile Dictionary<uint, RemoteInventoryFile> _remoteFiles = new();
    readonly SemaphoreSlim _initialization = new(1);
    readonly CancellationTokenSource _lifetime = new();
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
            linked.Token.ThrowIfCancellationRequested();
            // The session starts here: every selected file keeps its remote ID locally. A cached copy under another
            // ID is not reused; restore downloads it again under the remote ID.
            lock (_placementLock)
            {
                _remoteToLocal.Clear();
                _placements.Clear();
                _placedRemoteIds.Clear();
                foreach (var id in inventory.Keys)
                {
                    ObserveId(id);
                    RememberMapping(id, id);
                }
            }
            // Only a complete remote listing can prove that a local file is unselected.
            foreach (var localFile in Local.Enumerate().ToArray())
            {
                linked.Token.ThrowIfCancellationRequested();
                if (!inventory.ContainsKey(localFile.Index))
                    DiscardCachedFile(localFile, "no corresponding file in the remote inventory");
            }
            // Cheap metadata discards what can never be reused (another type or length, an active remote file, no
            // checksum). The whole-file checksum runs on the first prefetch instead: a warm open then validates the
            // files a KVI needs while the KVI loads, and never hashes cached files it does not use.
            foreach (var file in inventory.Values)
            {
                linked.Token.ThrowIfCancellationRequested();
                if (Local.GetFile(file.Index) is { } candidate && CachedFileMismatch(candidate, file.Selected) is { } reason)
                    DiscardCachedFile(candidate, reason, file.Index);
            }
            // Publish the inventory only once complete: a failed attempt must not leave a partial or stale listing.
            _remoteFiles = inventory;
            _initialized = true;
        }
        finally { _initialization.Release(); }
    }

    void EnsureInitialized()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("Call and await InitializeAsync before accessing the remote inventory or opening the database.");
    }

    // Caller holds _placementLock: native file creation and session PVL remapping share one ID sequence.
    void ObserveId(uint id)
    {
        if ((id & 1) != 0) _lastOddId = Math.Max(_lastOddId, id);
        else _lastEvenId = Math.Max(_lastEvenId, id);
    }

    bool OwnsSource(KeyIndexFileSource source) => ReferenceEquals(Local.GetFile(source.FileId), source.File);

    public IStreamingFileRead? StartStreamingRead(uint fileId, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        EnsureInitialized();
        return _remoteFiles.TryGetValue(fileId, out var file) ? file.StartStreamingRead(cancellation) : null;
    }

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
        // Prefetch verified the cached copy or downloaded this exact selected version: reuse its header rather than
        // issuing another range request. An unverified cached copy is verified first; the database reads only the
        // headers of files it needs (the chosen KVI and the TRLs it replays), so no checksum is wasted.
        if (file.GetCachedLocal() is { } local) return CheckCheckpointCut(FileCollectionWithFileInfos.ReadFileInfo(local));
        // A cached KVI is verified while it loads (StartStreamingRead), so read its header remotely instead.
        if (Local.GetFile(fileId) != null && FileExtension(file.Selected.FileType) != "kvi")
            return FileCollectionWithFileInfos.ReadFileInfo(await file.GetLocalAsync(cancellation).ConfigureAwait(false));
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
            try { return CheckCheckpointCut(ParseHeader(header, (ulong)header.Length == selected.Length)); }
            catch (EndOfStreamException) when ((ulong)header.Length < selected.Length)
            {
                Array.Resize(ref header, (int)Math.Min((ulong)checked(Math.Max(1, header.Length) * 2), selected.Length));
            }
        }
    }

    // The PVL/KVI listing follows TRL discovery, so a checkpoint published in between can reference history this
    // inventory lacks: its cut TRL absent or listed shorter, and opening it would silently miss values. Such a KVI is
    // hidden (open then skips it), exactly as if the listing had preceded discovery: the newest KVI that existed at
    // discovery is covered and outlives this restore by the deletion delay, and without one discovery kept the whole
    // TRL history.
    IFileInfo CheckCheckpointCut(IFileInfo info) =>
        info is IKeyIndex { TrLogFileId: not 0 } checkpoint &&
        (!_remoteFiles.TryGetValue(checkpoint.TrLogFileId, out var trl) ||
         trl.Selected.FileType != KVFileType.TransactionLog || trl.Selected.Length < checkpoint.TrLogOffset)
            ? NewerCheckpoint.Instance : info;

    sealed class NewerCheckpoint : IFileInfo
    {
        public static readonly NewerCheckpoint Instance = new();
        public KVFileType FileType => KVFileType.Unknown;
        public Guid? Guid => null;
        public long Generation => -1;
        public long SubDBId => -1;
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

    public IEnumerable<IFileCollectionFile> RemoteEnumerate()
    {
        EnsureInitialized();
        return _remoteFiles.Values;
    }

    // A selected remote file is visible only once prefetch verified or downloaded it: an unverified cached copy or a
    // download in progress looks missing, so no reader can see bytes that are not the selected version.
    public IFileCollectionFile GetFile(uint index) =>
        _remoteFiles.TryGetValue(index, out var remote) ? remote.GetCachedLocal()! : Local.GetFile(index);
    public uint GetCount() => Local.GetCount();
    public IEnumerable<IFileCollectionFile> Enumerate() => Local.Enumerate();
    public void ConcurrentTemporaryTruncate(uint index, uint offset) => Local.ConcurrentTemporaryTruncate(index, offset);

    public IFileCollectionFile AddFile(string humanHint) => AddFile(humanHint, FileIdParity.Any);

    public void DiscardUncommittedTransactionLog(uint fileId)
    {
        EnsureInitialized();
        if (GetFileType(fileId) != KVFileType.TransactionLog) throw new InvalidOperationException("Only unfinished TRLs may be discarded.");
        Local.GetFile(fileId)?.Remove();
        _remoteFiles.Remove(fileId);
    }

    public void DiscardLocalFile(uint fileId) => Local.GetFile(fileId)?.Remove();

    public IFileCollectionFile CreateTransactionLogFile(uint fileId)
    {
        EnsureInitialized();
        if ((fileId & 1) == 0) throw new ArgumentOutOfRangeException(nameof(fileId), "New TRL IDs must be odd.");
        lock (_placementLock)
        {
            if (_remoteFiles.ContainsKey(fileId))
                throw new IOException($"TRL file ID {fileId} is already selected in the remote inventory.");
            var file = Local.ImportFile(fileId, "trl");
            ObserveId(fileId);
            return file;
        }
    }

    public async ValueTask PublishTransactionLogHeaderAsync(IFileCollectionFile file, CancellationToken cancellation)
    {
        EnsureInitialized();
        if (Remote is not IReplicationStorage storage)
            throw new NotSupportedException("Legacy transition requires writable replication storage.");
        var predecessor = ((IFileTransactionLog)FileCollectionWithFileInfos.ReadFileInfo(file)).PreviousFileId;
        // A node may have listed the old tail before another startup published its transition and cleanup changed
        // the retained odd IDs. Do not create a different successor from that stale inventory.
        await foreach (var head in storage.EnumerateTrlsAsync(cancellation).ConfigureAwait(false))
            if (head.FileId > predecessor && head.FileId != file.Index)
                throw new IOException("Canonical history already has another successor; restore again.");
        var length = checked((uint)file.GetSize());
        var key = TrlFileName.Key(file.Index);
        var result = await storage.WriteAsync(new(file.Index, key, null, 0, length, file), cancellation)
            .ConfigureAwait(false);
        var state = result.Outcome == TrlWriteOutcome.Applied ? result.State :
            await storage.ReadAsync(key, cancellation).ConfigureAwait(false);
        if (state == null) throw new IOException("Transition publication is unresolved; restore again.");
        if (state.Length < length) throw new InvalidDataException("Canonical transition header is truncated.");
        var actual = new byte[length];
        await storage.ReadRangeAsync(key, state.Token, 0, actual, cancellation).ConfigureAwait(false);
        var expected = new byte[length];
        file.RandomRead(expected, 0, false);
        if (!actual.AsSpan().SequenceEqual(expected))
            throw new InvalidDataException("Canonical transition header differs; restart from remote history.");
        if (state.Length != length)
            throw new IOException("Canonical transition already has subsequent writes; restore again.");
        var published = new RemoteInventoryFile(this,
            new(file.Index, KVFileType.TransactionLog, length, state.Token, false, null));
        published.UseValidatedLocal(file);
        _remoteFiles.Add(file.Index, published);
        lock (_placementLock) RememberMapping(file.Index, file.Index);
    }

    public IFileCollectionFile AddFile(string humanHint, FileIdParity parity)
    {
        EnsureInitialized();
        lock (_placementLock)
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

    async Task<IFileCollectionFile?> PopulateCacheAsync(RemoteFile selected, DownloadProgress? progress = null)
    {
        try
        {
            await _downloads.WaitAsync(_lifetime.Token).ConfigureAwait(false);
            try
            {
                // Cached bytes live only under the remote ID; another local file is never searched for a copy.
                var id = selected.FileId;
                if (GetLocalFileId(id) is var localId && localId != id)
                    throw new InvalidOperationException($"Remote file {id} is mapped to local file {localId} this session and is not downloaded again.");
                if (Local.GetFile(id) is { } candidate)
                {
                    if (ValidateCachedFile(candidate, selected, _lifetime.Token, out var reason))
                    {
                        progress?.Finish(candidate, (long)selected.Length);
                        return candidate;
                    }
                    DiscardCachedFile(candidate, reason!, selected.FileId);
                }
                var downloaded = await DownloadAsync(selected, _lifetime.Token, progress).ConfigureAwait(false);
                progress?.Finish(downloaded, (long)selected.Length);
                return downloaded;
            }
            finally { _downloads.Release(); }
        }
        catch (Exception error)
        {
            progress?.Fail(error);
            throw;
        }
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

    // Metadata that rules a cached copy out without reading its bytes; null when only the checksum can decide.
    string? CachedFileMismatch(IFileCollectionFile candidate, RemoteFile selected)
    {
        var localExtension = FileExtension(Local.GetFileType(candidate.Index));
        var remoteExtension = FileExtension(selected.FileType);
        if (localExtension != remoteExtension || localExtension == "<unknown>")
            return $"extension mismatch (local .{localExtension}, remote .{remoteExtension})";
        var length = candidate.GetSize();
        if (length != selected.Length) return $"length mismatch (local {length}, remote {selected.Length})";
        if (!selected.IsSealed) return "remote file is active and cannot reuse cached bytes";
        if (selected.Sha256 is null) return "remote SHA-256 metadata is missing";
        return null;
    }

    bool ValidateCachedFile(IFileCollectionFile candidate, RemoteFile selected, CancellationToken cancellation,
        out string? reason)
    {
        reason = CachedFileMismatch(candidate, selected);
        if (reason != null) return false;
        try { VerifyLocalChecksum(candidate, selected, cancellation); }
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

    static void VerifyLocalChecksum(IFileCollectionFile file, RemoteFile selected, CancellationToken cancellation)
    {
        var checksum = FileChecksum.Compute(file, selected.Length, 64 * 1024, cancellation);
        if (!checksum.Equals(selected.Sha256, StringComparison.OrdinalIgnoreCase))
            throw new IOException("The local file does not match the remote whole-file checksum.");
    }

    async Task<IFileCollectionFile?> VerifyCachedAsync(IFileCollectionFile candidate, RemoteFile selected)
    {
        await _downloads.WaitAsync(_lifetime.Token).ConfigureAwait(false);
        try
        {
            // A streaming reader may still be reading this candidate. Leave an invalid copy in place until
            // completion or a later prefetch; neither may accept it without a successful checksum.
            return ValidateCachedFile(candidate, selected, _lifetime.Token, out _) ? candidate : null;
        }
        finally { _downloads.Release(); }
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
        // The single shared download/verification. Null result means a streamed cache candidate failed
        // validation; a later prefetch downloads it. A completed file must still be selected locally.
        volatile Task<IFileCollectionFile?>? _pending;
        internal readonly RemoteFile Selected;
        public uint Index { get; }

        internal RemoteInventoryFile(ReplicationFileSet owner, RemoteFile selected)
        {
            _owner = owner;
            Selected = selected;
            Index = selected.FileId;
        }

        internal void UseValidatedLocal(IFileCollectionFile local)
        {
            lock (_lock) _pending = Task.FromResult<IFileCollectionFile?>(local);
        }

        internal IFileCollectionFile? GetCachedLocal()
        {
            // File lookups are on the ordinary read path; one task snapshot avoids taking the population lock.
            var pending = _pending;
            return pending is { IsCompletedSuccessfully: true, Result: { } local } &&
                   ReferenceEquals(_owner.Local.GetFile(Index), local) ? local : null;
        }

        internal Task<IFileCollectionFile?>? Pending { get { lock (_lock) return _pending; } }

        // Null when a verified copy or a prefetch already exists, or a cached copy is ruled out by metadata.
        internal IStreamingFileRead? StartStreamingRead(CancellationToken cancellation)
        {
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_owner._disposed, this);
                if (GetCachedLocal() != null || _pending is { IsCompleted: false }) return null;
                if (_owner.Local.GetFile(Index) is { } candidate)
                {
                    if (_owner.CachedFileMismatch(candidate, Selected) != null) return null;
                    // Use the collection lifetime, like downloads: abandoning one reader does not cancel
                    // a concurrent prefetch, and collection disposal cancels and drains both kinds of work.
                    _pending = Task.Run(() => _owner.VerifyCachedAsync(candidate, Selected), _owner._lifetime.Token);
                    return new CachedStreamingRead(_owner, this, candidate, _pending);
                }
                var progress = new DownloadProgress();
                _pending = _owner.PopulateCacheAsync(Selected, progress);
                return new DownloadingStreamingRead(this, progress, cancellation);
            }
        }

        internal async ValueTask<IFileCollectionFile> GetLocalAsync(CancellationToken cancellation)
        {
            while (true)
            {
                Task<IFileCollectionFile?> pending;
                lock (_lock)
                {
                    ObjectDisposedException.ThrowIf(_owner._disposed, this);
                    cancellation.ThrowIfCancellationRequested();
                    if (GetCachedLocal() is { } local) return local;
                    // Completed tasks can hold an evicted file, including an abandoned streaming download.
                    if (_pending is null || _pending.IsCompleted)
                        _pending = _owner.PopulateCacheAsync(Selected);
                    pending = _pending;
                }
                await pending.WaitAsync(cancellation).ConfigureAwait(false);
                // Recheck the actual local instance before exposing the result. A rejected warm candidate
                // or an evicted result starts an ordinary cache population on the next iteration.
            }
        }

        public ulong GetSize() => Selected.Length;
        // Metadata-only handle: the database reads bytes from the local cache after explicit prefetch.
        // Never block a caller on a synchronous network read here.
        public IMemReader GetExclusiveReader() => throw new NotSupportedException("Prefetch remote files before reading them.");
        public void AdvisePrefetch() { }
        public void RandomRead(Span<byte> data, ulong position, bool doNotCache) =>
            throw new NotSupportedException("Prefetch remote files before reading them.");

        public IMemWriter GetAppenderWriter() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public IMemWriter GetExclusiveAppenderWriter() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void HardFlush() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void HardFlushTruncateSwitchToReadOnlyMode() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void HardFlushTruncateSwitchToDisposedMode() => throw new NotSupportedException("Remote inventory handles are read-only.");
        public void Remove() => throw new NotSupportedException("Remote deletion requires the publication protocol.");
    }
}
