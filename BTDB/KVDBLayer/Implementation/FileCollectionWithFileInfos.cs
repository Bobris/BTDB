using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class FileCollectionWithFileInfos : IFileCollectionWithFileInfos
{
    readonly IFileCollection _fileCollection;
    readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new();
    long _fileGeneration;
    internal static readonly byte[] MagicStartOfFile = "BTDB2"u8.ToArray();

    internal static readonly byte[] MagicStartOfFileWithGuid = "BTDB3"u8.ToArray();

    internal static void SkipHeader(ref MemReader reader)
    {
        var magic = reader.ReadByteArrayRaw(MagicStartOfFile.Length);
        var withGuid = magic.AsSpan().SequenceEqual(MagicStartOfFileWithGuid);
        if (withGuid) reader.SkipGuid();
    }

    internal static void WriteHeader(ref MemWriter writer, Guid? guid)
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

    // The files of one database share its Guid, and a file with another one is foreign and deleted. A foreign file,
    // or one whose Guid is damaged, must not decide it however files are listed: take the Guid most files carry and,
    // among equally common ones, the Guid of the earliest file.
    static Guid? ChooseGuid(List<(uint Index, Guid? Guid, IFileInfo Info)> headers)
    {
        var guids = new Dictionary<Guid, (int Count, uint First)>();
        foreach (var (index, guid, _) in headers)
        {
            if (guid is not { } value) continue;
            guids[value] = guids.TryGetValue(value, out var seen)
                ? (seen.Count + 1, Math.Min(seen.First, index))
                : (1, index);
        }

        return guids.Count == 0 ? null : guids.MaxBy(p => (p.Value.Count, -(long)p.Value.First)).Key;
    }

    void LoadInfoAboutFiles()
    {
        var headers = new List<(uint Index, Guid? Guid, IFileInfo Info)>();
        foreach (var file in _fileCollection.Enumerate())
        {
            Guid? guid = null;
            try
            {
                var readerController = file.GetExclusiveReader();
                var reader = new MemReader(readerController);
                var magic = reader.ReadByteArrayRaw(MagicStartOfFile.Length);
                if (magic.AsSpan().SequenceEqual(MagicStartOfFileWithGuid))
                {
                    guid = reader.ReadGuid();
                }
                else if (!magic.AsSpan().SequenceEqual(MagicStartOfFile))
                {
                    headers.Add((file.Index, null, UnknownFile.Instance));
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

                headers.Add((file.Index, guid, fileInfo));
            }
            catch (Exception)
            {
                headers.Add((file.Index, guid, UnknownFile.Instance));
            }
        }

        Guid = ChooseGuid(headers);
        foreach (var (index, guid, fileInfo) in headers)
        {
            if (guid.HasValue && guid != Guid)
            {
                _fileInfos.TryAdd(index, UnknownFile.Instance);
                continue;
            }

            if (_fileGeneration < fileInfo.Generation) _fileGeneration = fileInfo.Generation;
            _fileInfos.TryAdd(index, fileInfo);
        }

        if (!Guid.HasValue)
        {
            Guid = System.Guid.NewGuid();
        }
    }

    internal static KVFileType? FileTypeFromHint(string? hint) => hint switch
    {
        "trl" => KVFileType.TransactionLog,
        "kvi" => KVFileType.KeyIndex,
        "pvl" => KVFileType.PureValues,
        "hpv" => KVFileType.PureValuesWithId,
        "hid" => KVFileType.HashKeyIndex,
        _ => null
    };

    /// Parse native metadata only. Remote collections can provide a bounded header reader.
    public static IFileInfo ReadFileInfo(ref MemReader reader, bool throwOnEndOfStream = false)
    {
        try { return ParseFileInfo(ref reader); }
        catch (EndOfStreamException) when (!throwOnEndOfStream) { return UnknownFile.Instance; }
        catch (InvalidDataException) { return UnknownFile.Instance; }
        catch (OverflowException) { return UnknownFile.Instance; }
    }

    static IFileInfo ParseFileInfo(ref MemReader reader)
    {
        var magic = reader.ReadByteArrayRaw(MagicStartOfFile.Length);
        Guid? guid = null;
        if (magic.AsSpan().SequenceEqual(MagicStartOfFileWithGuid)) guid = reader.ReadGuid();
        else if (!magic.AsSpan().SequenceEqual(MagicStartOfFile)) return UnknownFile.Instance;
        return (KVFileType)reader.ReadUInt8() switch
        {
            KVFileType.TransactionLog => new FileTransactionLog(ref reader, guid),
            KVFileType.KeyIndex => new FileKeyIndex(ref reader, guid, false, false, false),
            KVFileType.KeyIndexWithCommitUlong => new FileKeyIndex(ref reader, guid, true, false, false),
            KVFileType.ModernKeyIndex => new FileKeyIndex(ref reader, guid, true, true, false),
            KVFileType.ModernKeyIndexWithUlongs => new FileKeyIndex(ref reader, guid, true, true, true),
            KVFileType.PureValues => new FilePureValues(ref reader, guid),
            KVFileType.PureValuesWithId => new FilePureValuesWithId(ref reader, guid),
            KVFileType.HashKeyIndex => new HashKeyIndex(ref reader, guid),
            _ => UnknownFile.Instance
        };
    }

    public static IFileInfo ReadFileInfo(IFileCollectionFile file)
    {
        try
        {
            var reader = new MemReader(file.GetExclusiveReader());
            return ReadFileInfo(ref reader);
        }
        catch (Exception) { return UnknownFile.Instance; }
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
