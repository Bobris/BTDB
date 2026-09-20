using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.KVDBLayer;

internal sealed class LazyFileCollectionWithFileInfos : IFileCollectionWithFileInfos
{
    readonly IFileReplicatedCollection _fileCollection;
    readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new();
    readonly ConcurrentDictionary<uint, byte> _knownFiles = new();
    readonly Dictionary<uint, uint> _remoteIdsByLocal = new();
    volatile bool _inventoryDiscovered;
    Guid? _guid;
    readonly object _metadataLock = new();
    internal LazyFileCollectionWithFileInfos(IFileReplicatedCollection fileCollection)
    {
        _fileCollection = fileCollection;
    }

    static KVFileType Normalize(KVFileType type) => type is KVFileType.KeyIndexWithCommitUlong or
        KVFileType.ModernKeyIndex or KVFileType.ModernKeyIndexWithUlongs ? KVFileType.KeyIndex : type;

    uint RemoteId(uint localId) => _remoteIdsByLocal.GetValueOrDefault(localId, localId);

    internal uint GetLocalFileId(uint remoteId) => _fileCollection.GetLocalFileId(remoteId);

    bool CouldHaveType(uint id, KVFileType type) =>
        _fileCollection.GetFileType(RemoteId(id)) is not { } known || Normalize(known) == type;

    internal async ValueTask<IFileInfo?> FileInfoByIdxAsync(uint id, CancellationToken cancellation)
    {
        if (_fileInfos.TryGetValue(id, out var cached)) return cached;
        DiscoverInventory();
        if (!_knownFiles.ContainsKey(id)) return null;
        var info = await _fileCollection.ReadFileInfoAsync(RemoteId(id), cancellation).ConfigureAwait(false);
        RejectSubDatabase(info.FileType);
        lock (_metadataLock)
        {
            if (info.Guid.HasValue)
            {
                if (_guid.HasValue && _guid != info.Guid) info = UnknownFile.Instance;
                else _guid = info.Guid;
            }
            return _fileInfos.GetOrAdd(id, info);
        }
    }

    // File IDs order TRLs and KVIs independently. PVLs are retained by references, never by age.
    internal IEnumerable<uint> FileIdsOfType(KVFileType type) =>
        FileTypes.Where(f => f.Value == type).Select(f => f.Key);

    internal IEnumerable<KeyValuePair<uint, IFileInfo>> FileInfosOfType(KVFileType type)
    {
        DiscoverInventory();
        foreach (var id in _knownFiles.Keys)
        {
            if (!CouldHaveType(id, type)) continue;
            var info = FileInfoByIdx(id);
            if (info?.FileType == type) yield return new(id, info);
        }
    }

    internal void LoadInventory(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        DiscoverInventory();
        foreach (var (_, type) in FileTypes) RejectSubDatabase(type);
    }

    static void RejectSubDatabase(KVFileType type)
    {
        if (type is KVFileType.HashKeyIndex or KVFileType.PureValuesWithId)
            throw new NotSupportedException("HID and HPV files are not supported by replication opening.");
    }

    void DiscoverInventory()
    {
        if (_inventoryDiscovered) return;
        lock (_metadataLock)
        {
            if (_inventoryDiscovered) return;
            foreach (var file in _fileCollection.RemoteEnumerate())
            {
                var localId = _fileCollection.GetLocalFileId(file.Index);
                _remoteIdsByLocal.TryAdd(localId, file.Index);
                _knownFiles.TryAdd(localId, 0);
            }
            _inventoryDiscovered = true;
        }
    }

    internal ValueTask PrefetchAsync(uint fileId, CancellationToken cancellation) =>
        _fileCollection.PrefetchAsync(RemoteId(fileId), cancellation);

    public IEnumerable<KeyValuePair<uint, IFileInfo>> FileInfos
    {
        get
        {
            DiscoverInventory();
            foreach (var id in _knownFiles.Keys)
            {
                var info = FileInfoByIdx(id);
                if (info != null) yield return new(id, info);
            }
        }
    }

    internal KVFileType? FileTypeByIdx(uint id) => _fileInfos.TryGetValue(id, out var info)
        ? info.FileType
        : _fileCollection.GetFileType(RemoteId(id)) is { } known ? Normalize(known) : FileInfoByIdx(id)?.FileType;

    public IEnumerable<KeyValuePair<uint, KVFileType>> FileTypes
    {
        get
        {
            DiscoverInventory();
            foreach (var id in _knownFiles.Keys)
            {
                var type = FileTypeByIdx(id);
                if (type.HasValue) yield return new(id, type.Value);
            }
        }
    }

    public long LastFileGeneration => throw new NotSupportedException("Replication does not use file generations.");

    public Guid? Guid
    {
        get { lock (_metadataLock) return _guid ??= System.Guid.NewGuid(); }
    }

    public IFileInfo? FileInfoByIdx(uint idx) => FileInfoByIdxAsync(idx, default).GetAwaiter().GetResult();

    public void MakeIdxUnknown(uint key)
    {
        _fileInfos[key] = UnknownFile.Instance;
    }

    public void DeleteAllUnknownFiles()
    {
        if (_fileInfos.All(fi => fi.Value.FileType != KVFileType.Unknown)) return;
        foreach (var fileId in _fileInfos.Where(fi => fi.Value.FileType == KVFileType.Unknown).Select(fi => fi.Key)
                     .ToArray())
        {
            _fileCollection.GetFile(fileId)?.Remove();
            _fileInfos.TryRemove(fileId);
            _knownFiles.TryRemove(fileId, out _);
        }
    }

    public IFileCollectionFile GetFile(uint fileId)
    {
        return _fileCollection.GetFile(fileId);
    }

    public uint GetCount()
    {
        return _fileCollection.GetCount();
    }

    public ulong GetSize(uint key)
    {
        return (_fileCollection.GetFile(key) ?? _fileCollection.GetRemoteFile(RemoteId(key)))!.GetSize();
    }

    public IFileCollectionFile AddFile(string humanHint)
    {
        if (humanHint is not ("trl" or "kvi" or "pvl"))
            throw new NotSupportedException("Replication supports only TRL, KVI and PVL files.");
        return _fileCollection.AddFile(humanHint, humanHint == "trl" ? FileIdParity.Odd : FileIdParity.Even);
    }

    public long NextGeneration() => throw new NotSupportedException("Replication does not use file generations.");

    public void SetInfo(uint idx, IFileInfo fileInfo)
    {
        _fileInfos.TryAdd(idx, fileInfo);
        _knownFiles.TryAdd(idx, 0);
    }

    public void ConcurentTemporaryTruncate(uint idx, uint offset)
    {
        _fileCollection.ConcurrentTemporaryTruncate(idx, offset);
    }
}
