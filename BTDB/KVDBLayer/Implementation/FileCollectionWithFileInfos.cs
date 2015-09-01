using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace BTDB.KVDBLayer
{
    class FileCollectionWithFileInfos : IFileCollectionWithFileInfos
    {
        readonly IFileCollection _fileCollection;
        readonly ConcurrentDictionary<uint, IFileInfo> _fileInfos = new ConcurrentDictionary<uint, IFileInfo>();
        long _fileGeneration;
        internal static readonly byte[] MagicStartOfFile = { (byte)'B', (byte)'T', (byte)'D', (byte)'B', (byte)'2' };

        public FileCollectionWithFileInfos(IFileCollection fileCollection)
        {
            _fileCollection = fileCollection;
            LoadInfoAboutFiles();
        }

        void LoadInfoAboutFiles()
        {
            foreach (var file in _fileCollection.Enumerate())
            {
                try
                {
                    var reader = file.GetExclusiveReader();
                    if (reader.CheckMagic(MagicStartOfFile))
                    {
                        var fileType = (KVFileType)reader.ReadUInt8();
                        IFileInfo fileInfo;
                        switch (fileType)
                        {
                            case KVFileType.TransactionLog:
                                fileInfo = new FileTransactionLog(reader);
                                break;
                            case KVFileType.KeyIndex:
                                fileInfo = new FileKeyIndex(reader);
                                break;
                            case KVFileType.PureValues:
                                fileInfo = new FilePureValues(reader);
                                break;
                            case KVFileType.PureValuesWithId:
                                fileInfo = new FilePureValuesWithId(reader);
                                break;
                            case KVFileType.HashKeyIndex:
                                fileInfo = new HashKeyIndex(reader);
                                break;
                            default:
                                fileInfo = UnknownFile.Instance;
                                break;
                        }
                        if (_fileGeneration < fileInfo.Generation) _fileGeneration = fileInfo.Generation;
                        _fileInfos.TryAdd(file.Index, fileInfo);
                    }
                    else
                    {
                        _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
                    }
                }
                catch (Exception)
                {
                    _fileInfos.TryAdd(file.Index, UnknownFile.Instance);
                }
            }
        }

        public IEnumerable<KeyValuePair<uint, IFileInfo>> FileInfos
        {
            get { return _fileInfos; }
        }

        public long LastFileGeneration
        {
            get { return _fileGeneration; }
        }

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
    }
}