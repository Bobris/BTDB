using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
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
internal sealed partial class ReplicationFileSet(InMemoryReplicationFileStorage local, ICheckpointStorage remote, int maxConcurrentDownloads = 4,
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
    readonly HashSet<uint> _mappedLocalIds = new();

    // Session-only identity assignments. They survive eviction, but carry no claim that bytes are cached or verified.
    void RememberMapping(uint remoteId, uint localId)
    {
        lock (_placementLock)
        {
            if (_remoteToLocal.TryGetValue(remoteId, out var previousLocal) && previousLocal != localId)
                throw new InvalidOperationException("The file already has a different session mapping.");
            _remoteToLocal[remoteId] = localId;
            _mappedLocalIds.Add(localId);
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

    uint GetOrAssignLocalFileId(uint remoteFileId, KVFileType fileType, bool preserveUnmappedLocal = false)
    {
        lock (_placementLock)
        {
            if (_remoteToLocal.TryGetValue(remoteFileId, out var localId)) return localId;
            localId = remoteFileId;
            if (_mappedLocalIds.Contains(localId) || (preserveUnmappedLocal && Local.GetFile(localId) != null))
            {
                if (fileType != KVFileType.PureValues)
                    throw new InvalidOperationException("A remembered PVL mapping conflicts with a canonical TRL or KVI ID.");
                // A remembered mapping already occupies this numeric ID. Allocate a separate local PVL identity.
                var next = (ulong)_lastEvenId + 2;
                if (next > uint.MaxValue) throw new InvalidOperationException("Local PVL file IDs exhausted.");
                localId = (uint)next;
                ObserveId(localId);
            }
            RememberMapping(remoteFileId, localId);
            return localId;
        }
    }

    /// Use the same logger instance as KeyValueDBOptions.Logger; initialization runs before database opening.
    public IKeyValueDBLogger? Logger { get; set; } = logger;
    public InMemoryReplicationFileStorage Local { get; } = local;
    public ICheckpointStorage Remote { get; } = remote;

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

    /// <summary>Verify a complete sealed local PVL against selected remote metadata before reusing it.
    /// A matching ID or length alone does not establish a local-to-remote placement.</summary>
    public void RememberVerifiedPureValues(KeyIndexFileSource source, RemoteFile file)
    {
        if (source.FileType != KVFileType.PureValues || file.FileType != KVFileType.PureValues ||
            !file.IsSealed || file.Sha256 is null || file.FileId == 0 || source.Length != file.Length ||
            source.Length != source.File.GetSize() || !OwnsSource(source))
            throw new ArgumentException("A complete local PVL and a sealed remote PVL with a checksum are required.");
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            for (ulong offset = 0; offset < source.Length;)
            {
                var count = (int)Math.Min((ulong)buffer.Length, source.Length - offset);
                source.File.RandomRead(buffer.AsSpan(0, count), offset, false);
                hash.AppendData(buffer, 0, count);
                offset += (uint)count;
            }
            VerifyChecksum(hash, file);
            AddPlacement(source.FileId, new(source.Length, file.FileId, true));
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }

    /// <summary>Restore under the assigned local ID, using the remote ID for a new identity mapping.
    /// A collision fails without touching
    /// the existing local file. Partial/invalid downloads are removed and never establish a placement.</summary>
    internal async ValueTask<IFileCollectionFile> DownloadAsync(RemoteFile file, CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        var localId = GetOrAssignLocalFileId(file.FileId, file.FileType);
        var target = Local.ImportFile(localId, FileExtension(file.FileType));
        byte[]? buffer = null;
        try
        {
            buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
            using var hash = file.IsSealed && file.Sha256 != null
                ? IncrementalHash.CreateHash(HashAlgorithmName.SHA256) : null;
            if (file.Length == 0)
                await Remote.ReadAsync(file, 0, Memory<byte>.Empty, cancellation).ConfigureAwait(false);
            for (ulong offset = 0; offset < file.Length;)
            {
                var count = (int)Math.Min((ulong)buffer.Length, file.Length - offset);
                var read = await Remote.ReadAsync(file, offset, buffer.AsMemory(0, count), cancellation).ConfigureAwait(false);
                if (read <= 0 || read > count) throw new IOException("Remote file is truncated or returned an invalid read length.");
                hash?.AppendData(buffer, 0, read);
                WriteBlock(target, buffer.AsSpan(0, read));
                offset += (uint)read;
            }
            cancellation.ThrowIfCancellationRequested();
            if (hash != null) VerifyChecksum(hash, file);
            target.HardFlush();
            if (file.FileType == KVFileType.PureValues && hash != null)
                AddPlacement(localId, new(file.Length, file.FileId, true));
            return target;
        }
        catch (Exception error)
        {
            DiscardCachedFile(target, $"download failed: {error.Message}", file.FileId);
            throw;
        }
        finally
        {
            if (buffer != null) ArrayPool<byte>.Shared.Return(buffer);
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

    uint _lastRemoteEvenId;

    // Called under the publication lane. Refresh only remote discovery: refreshing local mappings here would
    // invalidate confirmed PVL receipts during the same checkpoint. No remote reservation object is created.
    internal async ValueTask<uint> AllocateRemoteFileIdAsync(CancellationToken cancellation = default)
    {
        await foreach (var file in Remote.EnumerateAsync(cancellation).ConfigureAwait(false))
            if ((file.FileId & 1) == 0) _lastRemoteEvenId = Math.Max(_lastRemoteEvenId, file.FileId);
        cancellation.ThrowIfCancellationRequested();
        var next = (ulong)_lastRemoteEvenId + 2;
        if (next > uint.MaxValue) throw new InvalidOperationException("Remote PVL/KVI IDs exhausted.");
        return _lastRemoteEvenId = (uint)next;
    }

    public async ValueTask<uint> PublishPureValuesAsync(KeyIndexFileSource source, CancellationToken cancellation = default)
    {
        if (source.FileType != KVFileType.PureValues ||
            !OwnsSource(source) || source.Length != source.File.GetSize())
            throw new InvalidOperationException("The snapshot does not refer to a complete file in this local inventory.");
        if (!_placements.TryGetValue(source.FileId, out var placement))
        {
            var remoteId = await AllocateRemoteFileIdAsync(cancellation).ConfigureAwait(false);
            placement = new(source.Length, remoteId, false);
            AddPlacement(source.FileId, placement);
        }
        if (placement.Length != source.Length)
            throw new InvalidOperationException("A sealed local PVL identity changed; start a new restore session.");
        if (!placement.Confirmed)
        {
            await Remote.EnsurePureValuesAsync(placement.RemoteId, source, cancellation).ConfigureAwait(false);
            RememberMapping(placement.RemoteId, source.FileId);
            placement.Confirmed = true;
        }
        return placement.RemoteId;
    }
}
