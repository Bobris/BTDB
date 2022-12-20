using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class FileCollectionWithFileInfos : IFileCollectionWithFileInfos
{
    readonly IFileCollection _fileCollection;
    readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
    long _fileGeneration;
    internal static readonly byte[] MagicStartOfFile = { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };

    internal static readonly byte[] MagicStartOfFileWithGuid =
        { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'3' };

    internal static void SkipHeader(ref SpanReader reader)
    {
        var magic = reader.ReadByteArrayRaw(MagicStartOfFile.Length);
        var withGuid = magic.AsSpan().SequenceEqual(MagicStartOfFileWithGuid);
        if (withGuid) reader.SkipGuid();
    }

    internal static void WriteHeader(ref SpanWriter writer, Guid? guid)
    {
        if (guid.HasValue)
        {
            writer.WriteByteArrayRaw(MagicStartOfFileWithGuid);
            writer.WriteGuid(guid.Value);
        }
        else
        {
            writer.WriteByteArrayRaw(MagicStartOfFile);
        }
    }

    public FileCollectionWithFileInfos(IFileCollection fileCollection)
    {
        _fileCollection = fileCollection;
        Guid = null;
        LoadInfoAboutFiles();
    }

    void LoadInfoAboutFiles()
    {
        foreach (var file in _fileCollection.Enumerate())
        {
            try
            {
                var readerController = file.GetExclusiveReader();
                var reader = new SpanReader(readerController);
                var magic = reader.ReadByteArrayRaw(MagicStartOfFile.Length);
                Guid? guid = null;
                if (magic.AsSpan().SequenceEqual(MagicStartOfFileWithGuid))
                {
                    guid = reader.ReadGuid();
                    if (Guid.HasValue && Guid.Value != guid)
                    {
                        _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
                        continue;
                    }

                    Guid = guid;
                }
                else if (!magic.AsSpan().SequenceEqual(MagicStartOfFile))
                {
                    _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
                    continue;
                }

                var fileType = (KVFileType)reader.ReadUInt8();
                IFileInfo fileInfo;
                switch (fileType)
                {
                    case KVFileType.TransactionLog:
                        fileInfo = new FileTransactionLog(ref reader, guid);
                        break;
                    case KVFileType.KeyIndex:
                        fileInfo = new FileKeyIndex(ref reader, guid, false, false, false);
                        break;
                    case KVFileType.KeyIndexWithCommitUlong:
                        fileInfo = new FileKeyIndex(ref reader, guid, true, false, false);
                        break;
                    case KVFileType.ModernKeyIndex:
                        fileInfo = new FileKeyIndex(ref reader, guid, true, true, false);
                        break;
                    case KVFileType.ModernKeyIndexWithUlongs:
                        fileInfo = new FileKeyIndex(ref reader, guid, true, true, true);
                        break;
                    case KVFileType.PureValues:
                        fileInfo = new FilePureValues(ref reader, guid);
                        break;
                    case KVFileType.PureValuesWithId:
                        fileInfo = new FilePureValuesWithId(ref reader, guid);
                        break;
                    case KVFileType.HashKeyIndex:
                        fileInfo = new HashKeyIndex(ref reader, guid);
                        break;
                    default:
                        fileInfo = UnknownFile.Instance;
                        break;
                }

                if (_fileGeneration < fileInfo.Generation) _fileGeneration = fileInfo.Generation;
                _fileInfos.TryAdd(file.Index, fileInfo);
            }
            catch (Exception)
            {
                _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
            }
        }

        if (!Guid.HasValue)
        {
            Guid = System.Guid.NewGuid();
        }
    }

    public IEnumerable<KeyValuePair<uint, IFileInfo>> FileInfos => _fileInfos;

    public long LastFileGeneration => _fileGeneration;

    public Guid? Guid { get; private set; }

    public IFileInfo FileInfoByIdx(uint idx)
    {
        IFileInfo res;
        return _fileInfos.TryGetValue(idx, out res) ? res : null;
    }

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
            _fileCollection.GetFile(fileId).Remove();
            _fileInfos.TryRemove(fileId);
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
        return _fileCollection.GetFile(key).GetSize();
    }

    public IFileCollectionFile AddFile(string humanHint)
    {
        return _fileCollection.AddFile(humanHint);
    }

    public long NextGeneration()
    {
        return Interlocked.Increment(ref _fileGeneration);
    }

    public void SetInfo(uint idx, IFileInfo fileInfo)
    {
        _fileInfos.TryAdd(idx, fileInfo);
    }

    public void ConcurentTemporaryTruncate(uint idx, uint offset)
    {
        _fileCollection.ConcurrentTemporaryTruncate(idx, offset);
    }
}
