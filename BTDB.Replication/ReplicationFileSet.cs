using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication;

/// <summary>One local database session and its separate remote inventory. Does not own either collection.
/// Local IDs must not be reused during this session. Receipts retain no bytes, file handles or roots.
/// Serialize publication/receipt changes externally; restore finishes before publication starts.
/// Remote cleanup must protect receipt destinations while this session may reuse them.</summary>
public sealed partial class ReplicationFileSet(IReplicationFileStorage local, IRemoteFileCollection remote, int maxConcurrentDownloads = 4,
    IKeyValueDBLogger? logger = null)
{
    sealed class Placement(ulong length, uint remoteId, bool confirmed)
    {
        internal readonly ulong Length = length;
        internal readonly uint RemoteId = remoteId;
        internal bool Confirmed = confirmed;
    }

    readonly SemaphoreSlim _downloads = new(maxConcurrentDownloads > 0 ? maxConcurrentDownloads :
        throw new ArgumentOutOfRangeException(nameof(maxConcurrentDownloads)), maxConcurrentDownloads);
    readonly object _placementLock = new();
    readonly Dictionary<uint, Placement> _placements = new();
    readonly HashSet<uint> _placedRemoteIds = new();
    readonly Dictionary<uint, uint> _remoteToLocal = new();

    // Session-only identity assignments. They survive eviction, but carry no claim that bytes are cached or verified.
    void RememberMapping(uint remoteId, uint localId)
    {
        lock (_placementLock)
        {
            if (_remoteToLocal.TryGetValue(remoteId, out var previousLocal) && previousLocal != localId)
                throw new InvalidOperationException("The file already has a different session mapping.");
            _remoteToLocal[remoteId] = localId;
        }
    }

    public uint GetLocalFileId(uint remoteFileId)
    {
        lock (_placementLock)
        {
            if (_remoteToLocal.TryGetValue(remoteFileId, out var localId)) return localId;
        }
        EnsureInitialized();
        throw new FileNotFoundException($"Remote file {remoteFileId} has no session mapping.");
    }

    /// Use the same logger instance as KeyValueDBOptions.Logger; initialization runs before database opening.
    public IKeyValueDBLogger? Logger { get; set; } = logger;
    public IReplicationFileStorage Local { get; } = local;
    public IRemoteFileCollection Remote { get; } = remote;

    void AddPlacement(uint localId, Placement placement)
    {
        lock (_placementLock)
        {
            if (_placements.TryGetValue(localId, out var existing))
            {
                if (existing.Length == placement.Length && existing.RemoteId == placement.RemoteId &&
                    existing.Confirmed && placement.Confirmed) return;
                throw new InvalidOperationException("The local file already has a placement.");
            }
            if (_placedRemoteIds.Contains(placement.RemoteId))
                throw new InvalidOperationException("Remote file IDs collide.");
            if (placement.Confirmed) RememberMapping(placement.RemoteId, localId);
            _placedRemoteIds.Add(placement.RemoteId);
            _placements.Add(localId, placement);
        }
    }

    // Matches the Azure block size: per-request latency dominates smaller ranges (Azurite, 4 x 128 MB: 1.40 s at
    // 256 KiB, 1.00 s at 4 MiB; far fewer round trips on real Blob latency). Peak memory is 16 MiB per download.
    internal const int DownloadBlockSize = 4 * 1024 * 1024;

    /// <summary>Restore under the exact remote file ID before publication starts.
    /// A collision fails without touching the existing local file. Sealed files with a checksum are verified while
    /// they are written. Partial/invalid downloads are removed and never establish a placement.</summary>
    internal async ValueTask<IFileCollectionFile> DownloadAsync(RemoteFile file, CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        var target = Local.ImportFile(file.FileId, FileExtension(file.FileType));
        const int blockSize = DownloadBlockSize;
        const int parallelBlocks = 4;
        var buffers = new byte[parallelBlocks][];
        var reads = new Task<int>?[parallelBlocks];
        using var hash = file.IsSealed && file.Sha256 != null ? IncrementalHash.CreateHash(HashAlgorithmName.SHA256) : null;
        using var transfer = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        ulong requested = 0;
        var head = 0;
        var active = 0;
        try
        {
            if (file.Length == 0)
                await Remote.ReadAsync(file, 0, Memory<byte>.Empty, transfer.Token).ConfigureAwait(false);
            // A sliding window keeps parallelBlocks reads in flight while completed blocks are written in order.
            while (active < parallelBlocks && requested < file.Length) StartRead();
            while (active != 0)
            {
                int count;
                try { count = await reads[head]!.ConfigureAwait(false); }
                catch (OperationCanceledException) when (!cancellation.IsCancellationRequested)
                {
                    // A failed sibling read cancelled the transfer; surface its failure rather than the cancellation.
                    await Task.WhenAll(reads.OfType<Task<int>>()).ConfigureAwait(false);
                    throw;
                }
                cancellation.ThrowIfCancellationRequested();
                WriteBlock(target, buffers[head].AsSpan(0, count));
                hash?.AppendData(buffers[head], 0, count);
                head = (head + 1) % parallelBlocks;
                active--;
                if (requested < file.Length) StartRead();
            }
            cancellation.ThrowIfCancellationRequested();
            if (hash != null) VerifyChecksum(hash, file);
            target.HardFlush();
            RememberMapping(file.FileId, file.FileId);
            if (file.FileType == KVFileType.PureValues && file.IsSealed && file.Sha256 != null)
                AddPlacement(file.FileId, new(file.Length, file.FileId, true));
            return target;
        }
        catch (Exception error)
        {
            DiscardCachedFile(target, $"download failed: {error.Message}", file.FileId);
            throw;
        }
        finally
        {
            // Drain every read before returning pooled buffers, including on failure.
            foreach (var read in reads)
                if (read != null) await ((Task)read).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            foreach (var buffer in buffers)
                if (buffer != null) ArrayPool<byte>.Shared.Return(buffer);
        }

        void StartRead()
        {
            var slot = (head + active) % parallelBlocks;
            var length = (int)Math.Min((ulong)blockSize, file.Length - requested);
            buffers[slot] ??= ArrayPool<byte>.Shared.Rent(blockSize);
            reads[slot] = ReadBlockAsync(requested, buffers[slot].AsMemory(0, length));
            requested += (uint)length;
            active++;
        }

        async Task<int> ReadBlockAsync(ulong offset, Memory<byte> destination)
        {
            try
            {
                var filled = 0;
                while (filled < destination.Length)
                {
                    transfer.Token.ThrowIfCancellationRequested();
                    var read = await Remote.ReadAsync(file, offset + (uint)filled, destination[filled..], transfer.Token)
                        .ConfigureAwait(false);
                    if (read <= 0 || read > destination.Length - filled)
                        throw new IOException("Remote file is truncated or returned an invalid read length.");
                    filled += read;
                }
                return filled;
            }
            catch
            {
                transfer.Cancel();
                throw;
            }
        }
    }

    static void WriteBlock(IFileCollectionFile file, ReadOnlySpan<byte> bytes)
    {
        var writer = new MemWriter(file.GetAppenderWriter());
        writer.WriteBlock(bytes);
        writer.Flush();
    }

    static void VerifyChecksum(IncrementalHash hash, RemoteFile file)
    {
        if (!Convert.ToHexString(hash.GetHashAndReset()).Equals(file.Sha256, StringComparison.OrdinalIgnoreCase))
            throw new IOException("The local file does not match the remote whole-file checksum.");
    }

    internal IReplicationStorage PublicationStorage => Remote as IReplicationStorage ??
        throw new InvalidOperationException("This inventory is read-only; supply session-bound replication storage for publication.");

    uint _lastRemoteEvenId;

    // Called under the publication lane. Refresh only remote discovery: refreshing local mappings here would
    // invalidate confirmed PVL receipts during the same checkpoint. No remote reservation object is created.
    internal async ValueTask ScanRemoteIdsAsync(IRemoteFileCollection storage, CancellationToken cancellation)
    {
        await foreach (var file in storage.EnumerateAsync(cancellation).ConfigureAwait(false))
            if ((file.FileId & 1) == 0) _lastRemoteEvenId = Math.Max(_lastRemoteEvenId, file.FileId);
        cancellation.ThrowIfCancellationRequested();
    }

    /// <summary>Rescan unless the caller already scanned under the same publication lane, e.g. once per checkpoint.</summary>
    internal async ValueTask<uint> AllocateRemoteFileIdAsync(CancellationToken cancellation = default,
        IReplicationStorage? storage = null, bool rescan = true)
    {
        if (rescan) await ScanRemoteIdsAsync(storage ?? Remote, cancellation).ConfigureAwait(false);
        var next = (ulong)_lastRemoteEvenId + 2;
        if (next > uint.MaxValue) throw new InvalidOperationException("Remote PVL/KVI IDs exhausted.");
        return _lastRemoteEvenId = (uint)next;
    }

    internal ValueTask<uint> PublishPureValuesAsync(KeyIndexFileSource source, CancellationToken cancellation = default) =>
        PublishPureValuesAsync(source, PublicationStorage, cancellation);

    internal async ValueTask<uint> PublishPureValuesAsync(KeyIndexFileSource source, IReplicationStorage storage,
        CancellationToken cancellation, bool rescan = true)
    {
        if (source.FileType != KVFileType.PureValues ||
            !OwnsSource(source) || source.Length != source.File.GetSize())
            throw new InvalidOperationException("The snapshot does not refer to a complete file in this local inventory.");
        while (true)
        {
            cancellation.ThrowIfCancellationRequested();
            Placement? placement;
            lock (_placementLock) _placements.TryGetValue(source.FileId, out placement);
            if (placement == null)
            {
                var remoteId = await AllocateRemoteFileIdAsync(cancellation, storage, rescan).ConfigureAwait(false);
                placement = new(source.Length, remoteId, false);
                AddPlacement(source.FileId, placement);
            }
            if (placement.Length != source.Length)
                throw new InvalidOperationException("A sealed local PVL identity changed; start a new restore session.");
            if (!placement.Confirmed)
            {
                await storage.EnsurePureValuesAsync(placement.RemoteId, source, cancellation).ConfigureAwait(false);
                RememberMapping(placement.RemoteId, source.FileId);
                lock (_placementLock) placement.Confirmed = true;
            }
            if (!await storage.ProtectPureValuesAsync(placement.RemoteId, source, cancellation).ConfigureAwait(false))
            {
                _lastRemoteEvenId = Math.Max(_lastRemoteEvenId, placement.RemoteId);
                lock (_placementLock)
                {
                    _placements.Remove(source.FileId);
                    _placedRemoteIds.Remove(placement.RemoteId);
                }
                continue;
            }
            return placement.RemoteId;
        }
    }
}
