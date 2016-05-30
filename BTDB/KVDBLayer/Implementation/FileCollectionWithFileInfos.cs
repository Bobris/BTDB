using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class FileCollectionWithFileInfos : IFileCollectionWithFileInfos
    {
        readonly IFileCollection _fileCollection;
        readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
        long _fileGeneration;
        internal static readonly byte[] MagicStartOfFile = { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };
        internal static readonly byte[] MagicStartOfFileWithGuid = { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'3' };

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            var magic = reader.ReadByteArrayRaw(MagicStartOfFile.Length);
            var withGuid = BitArrayManipulation.CompareByteArray(magic, magic.Length,
                MagicStartOfFileWithGuid, MagicStartOfFileWithGuid.Length) == 0;
            if (withGuid) reader.SkipGuid();
        }

        internal static void WriteHeader(AbstractBufferedWriter writer, Guid? guid)
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
                    var reader = file.GetExclusiveReader();
                    var magic = reader.ReadByteArrayRaw(MagicStartOfFile.Length);
                    Guid? guid = null;
                    if (
                        BitArrayManipulation.CompareByteArray(magic, magic.Length, MagicStartOfFileWithGuid,
                            MagicStartOfFileWithGuid.Length) == 0)
                    {
                        guid = reader.ReadGuid();
                        if (Guid.HasValue && Guid.Value != guid)
                        {
                            _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
                            continue;
                        }
                        Guid = guid;
                    }
                    else if (
                        BitArrayManipulation.CompareByteArray(magic, magic.Length, MagicStartOfFile,
                            MagicStartOfFile.Length) != 0)
                    {
                        _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
                        continue;
                    }
                    var fileType = (KVFileType)reader.ReadUInt8();
                    IFileInfo fileInfo;
                    switch (fileType)
                    {
                        case KVFileType.TransactionLog:
                            fileInfo = new FileTransactionLog(reader, guid);
                            break;
                        case KVFileType.KeyIndex:
                            fileInfo = new FileKeyIndex(reader, guid, false);
                            break;
                        case KVFileType.KeyIndexWithCommitUlong:
                            fileInfo = new FileKeyIndex(reader, guid, true);
                            break;
                        case KVFileType.PureValues:
                            fileInfo = new FilePureValues(reader, guid);
                            break;
                        case KVFileType.PureValuesWithId:
                            fileInfo = new FilePureValuesWithId(reader, guid);
                            break;
                        case KVFileType.HashKeyIndex:
                            fileInfo = new HashKeyIndex(reader, guid);
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
            foreach (var fileId in _fileInfos.Where(fi => fi.Value.FileType == KVFileType.Unknown).Select(fi => fi.Key).ToArray())
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
            _fileCollection.ConcurentTemporaryTruncate(idx, offset);
        }
    }
}