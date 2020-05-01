using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer.BTree;
using BTDB.KVDBLayer.Implementation;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDB : IHaveSubDB, IKeyValueDBInternal
    {
        const int MaxValueSizeInlineInMemory = 7;
        const int EndOfIndexFileMarker = 0x1234DEAD;
        IBTreeRootNode _lastCommited;

        long _preserveHistoryUpToCommitUlong; // it is long only because Interlock.Read is just long capable, MaxValue means no preserving history

        IBTreeRootNode? _nextRoot;
        KeyValueDBTransaction? _writingTransaction;

        readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue =
            new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();

        readonly object _writeLock = new object();
        uint _fileIdWithTransactionLog;
        uint _fileIdWithPreviousTransactionLog;
        IFileCollectionFile? _fileWithTransactionLog;
        AbstractBufferedWriter? _writerWithTransactionLog;
        static readonly byte[] MagicStartOfTransaction = { (byte)'t', (byte)'R' };
        readonly ICompressionStrategy _compression;
        readonly ICompactorScheduler? _compactorScheduler;

        readonly HashSet<IBTreeRootNode> _usedBTreeRootNodes =
            new HashSet<IBTreeRootNode>(ReferenceEqualityComparer<IBTreeRootNode>.Instance);

        readonly object _usedBTreeRootNodesLock = new object();
        readonly IFileCollectionWithFileInfos _fileCollection;
        readonly Dictionary<long, object> _subDBs = new Dictionary<long, object>();
        readonly Func<CancellationToken, bool>? _compactFunc;
        readonly bool _readOnly;
        readonly uint _maxTrLogFileSize;
        public uint CompactorRamLimitInMb { get; set; }
        public ulong CompactorReadBytesPerSecondLimit { get; }
        public ulong CompactorWriteBytesPerSecondLimit { get; }

        public KeyValueDB(IFileCollection fileCollection)
            : this(fileCollection, new SnappyCompressionStrategy())
        {
        }

        public KeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression,
            uint fileSplitSize = int.MaxValue)
            : this(fileCollection, compression, fileSplitSize, CompactorScheduler.Instance)
        {
        }

        public KeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize,
            ICompactorScheduler compactorScheduler)
            : this(new KeyValueDBOptions
            {
                FileCollection = fileCollection,
                Compression = compression,
                FileSplitSize = fileSplitSize,
                CompactorScheduler = compactorScheduler
            })
        {
        }

        public KeyValueDB(KeyValueDBOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options.FileCollection == null) throw new ArgumentNullException(nameof(options.FileCollection));
            if (options.FileSplitSize < 1024 || options.FileSplitSize > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(options.FileSplitSize), "Allowed range 1024 - 2G");
            _compactorScheduler = options.CompactorScheduler;
            _maxTrLogFileSize = options.FileSplitSize;
            _compression = options.Compression ?? throw new ArgumentNullException(nameof(options.Compression));
            DurableTransactions = false;
            _fileCollection = new FileCollectionWithFileInfos(options.FileCollection);
            _readOnly = options.ReadOnly;
            CompactorReadBytesPerSecondLimit = options.CompactorReadBytesPerSecondLimit ?? 0;
            CompactorWriteBytesPerSecondLimit = options.CompactorWriteBytesPerSecondLimit ?? 0;
            _lastCommited = new BTreeRoot(0);
            _preserveHistoryUpToCommitUlong = (long)(options.PreserveHistoryUpToCommitUlong ?? ulong.MaxValue);
            CompactorRamLimitInMb = 200;
            LoadInfoAboutFiles(options.OpenUpToCommitUlong);
            if (!_readOnly)
            {
                _compactFunc = _compactorScheduler?.AddCompactAction(Compact);
                _compactorScheduler?.AdviceRunning(true);
            }
        }

        ulong IKeyValueDBInternal.DistanceFromLastKeyIndex(IRootNodeInternal root)
        {
            return DistanceFromLastKeyIndex((IBTreeRootNode)root);
        }

        List<KeyIndexInfo> IKeyValueDBInternal.BuildKeyIndexInfos()
        {
            var keyIndexes = new List<KeyIndexInfo>();
            foreach (var fileInfo in _fileCollection.FileInfos)
            {
                var keyIndex = fileInfo.Value as IKeyIndex;
                if (keyIndex == null) continue;
                keyIndexes.Add(new KeyIndexInfo
                { Key = fileInfo.Key, Generation = keyIndex.Generation, CommitUlong = keyIndex.CommitUlong });
            }

            if (keyIndexes.Count > 1)
                keyIndexes.Sort((l, r) => Comparer<long>.Default.Compare(l.Generation, r.Generation));
            return keyIndexes;
        }

        void LoadInfoAboutFiles(ulong? openUpToCommitUlong)
        {
            long latestGeneration = -1;
            uint lastTrLogFileId = 0;
            foreach (var fileInfo in _fileCollection.FileInfos)
            {
                var trLog = fileInfo.Value as IFileTransactionLog;
                if (trLog == null) continue;
                if (trLog.Generation > latestGeneration)
                {
                    latestGeneration = trLog.Generation;
                    lastTrLogFileId = fileInfo.Key;
                }
            }

            var keyIndexes = ((IKeyValueDBInternal)this).BuildKeyIndexInfos();
            var preserveKeyIndexKey =
                ((IKeyValueDBInternal)this).CalculatePreserveKeyIndexKeyFromKeyIndexInfos(keyIndexes);
            var preserveKeyIndexGeneration = CalculatePreserveKeyIndexGeneration(preserveKeyIndexKey);
            var firstTrLogId = LinkTransactionLogFileIds(lastTrLogFileId);
            var firstTrLogOffset = 0u;
            var hasKeyIndex = false;
            while (keyIndexes.Count > 0)
            {
                var nearKeyIndex = keyIndexes.Count - 1;
                if (openUpToCommitUlong.HasValue)
                {
                    while (nearKeyIndex >= 0)
                    {
                        if (keyIndexes[nearKeyIndex].CommitUlong <= openUpToCommitUlong.Value)
                            break;
                        nearKeyIndex--;
                    }

                    if (nearKeyIndex < 0)
                    {
                        // If we have all trl files we can replay from start
                        if (GetGeneration(firstTrLogId) == 1)
                            break;
                        // Or we have to start with oldest kvi
                        nearKeyIndex = 0;
                    }
                }

                var keyIndex = keyIndexes[nearKeyIndex];
                keyIndexes.RemoveAt(nearKeyIndex);
                var info = (IKeyIndex)_fileCollection.FileInfoByIdx(keyIndex.Key);
                _nextRoot = _lastCommited.NewTransactionRoot();
                if (LoadKeyIndex(keyIndex.Key, info!) && firstTrLogId <= info.TrLogFileId)
                {
                    _lastCommited = _nextRoot!;
                    _nextRoot = null;
                    firstTrLogId = info.TrLogFileId;
                    firstTrLogOffset = info.TrLogOffset;
                    hasKeyIndex = true;
                    break;
                }

                // Corrupted kvi - could be removed
                _fileCollection.MakeIdxUnknown(keyIndex.Key);
            }

            while (keyIndexes.Count > 0)
            {
                var keyIndex = keyIndexes[^1];
                keyIndexes.RemoveAt(keyIndexes.Count - 1);
                if (keyIndex.Key != preserveKeyIndexKey)
                    _fileCollection.MakeIdxUnknown(keyIndex.Key);
            }

            LoadTransactionLogs(firstTrLogId, firstTrLogOffset, openUpToCommitUlong);
            if (!_readOnly)
            {
                if (openUpToCommitUlong.HasValue || lastTrLogFileId != firstTrLogId && firstTrLogId != 0 || !hasKeyIndex && _fileCollection.FileInfos.Any(p => p.Value.SubDBId == 0))
                {
                    // Need to create new trl if cannot append to last one so it is then written to kvi
                    if (openUpToCommitUlong.HasValue && _fileIdWithTransactionLog == 0)
                    {
                        WriteStartOfNewTransactionLogFile();
                        _fileWithTransactionLog!.HardFlush();
                        _fileWithTransactionLog.Truncate();
                        UpdateTransactionLogInBTreeRoot(_lastCommited);
                    }
                    // When not opening history commit KVI file will be created by compaction
                    if (openUpToCommitUlong.HasValue)
                    {
                        CreateIndexFile(CancellationToken.None, preserveKeyIndexGeneration, true);
                    }
                }
                if (_fileIdWithTransactionLog != 0)
                {
                    if (_writerWithTransactionLog == null)
                    {
                        _fileWithTransactionLog = FileCollection.GetFile(_fileIdWithTransactionLog);
                        _writerWithTransactionLog = _fileWithTransactionLog!.GetAppenderWriter();
                    }
                    if (_writerWithTransactionLog.GetCurrentPosition() > _maxTrLogFileSize)
                    {
                        WriteStartOfNewTransactionLogFile();
                    }
                }
                _fileCollection.DeleteAllUnknownFiles();
            }
            foreach (var fileInfo in _fileCollection.FileInfos)
            {
                var ft = fileInfo.Value.FileType;
                if (ft == KVFileType.TransactionLog || ft == KVFileType.PureValuesWithId || ft == KVFileType.PureValues)
                {
                    _fileCollection.GetFile(fileInfo.Key)?.AdvisePrefetch();
                }
            }
        }

        bool IKeyValueDBInternal.LoadUsedFilesFromKeyIndex(uint fileId, IKeyIndex info)
        {
            return LoadUsedFilesFromKeyIndex(fileId, info);
        }

        public long CalculatePreserveKeyIndexGeneration(uint preserveKeyIndexKey)
        {
            if (preserveKeyIndexKey <= 0) return -1;
            if (preserveKeyIndexKey < uint.MaxValue)
            {
                return GetGeneration(preserveKeyIndexKey);
            }
            else
            {
                return long.MaxValue;
            }
        }

        uint IKeyValueDBInternal.CalculatePreserveKeyIndexKeyFromKeyIndexInfos(List<KeyIndexInfo> keyIndexes)
        {
            var preserveKeyIndexKey = uint.MaxValue;
            var preserveHistoryUpToCommitUlong = (ulong)Interlocked.Read(ref _preserveHistoryUpToCommitUlong);
            if (preserveHistoryUpToCommitUlong != ulong.MaxValue &&
                _lastCommited.CommitUlong != preserveHistoryUpToCommitUlong)
            {
                var nearKeyIndex = keyIndexes.Count - 1;
                while (nearKeyIndex >= 0)
                {
                    if (keyIndexes[nearKeyIndex].CommitUlong <= preserveHistoryUpToCommitUlong)
                    {
                        preserveKeyIndexKey = keyIndexes[nearKeyIndex].Key;
                        break;
                    }

                    nearKeyIndex--;
                }

                if (nearKeyIndex < 0)
                    preserveKeyIndexKey = 0;
            }

            return preserveKeyIndexKey;
        }

        uint IKeyValueDBInternal.GetTrLogFileId(IRootNodeInternal root)
        {
            return ((IBTreeRootNode)root).TrLogFileId;
        }

        void IKeyValueDBInternal.IterateRoot(IRootNodeInternal root, ValuesIterateAction visit)
        {
            ((IBTreeRootNode)root).Iterate(visit);
        }

        internal void CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration, bool fullSpeed = false)
        {
            var idxFileId = CreateKeyIndexFile(_lastCommited, cancellation, fullSpeed);
            MarkAsUnknown(_fileCollection.FileInfos.Where(p =>
                p.Value.FileType == KVFileType.KeyIndex && p.Key != idxFileId &&
                p.Value.Generation != preserveKeyIndexGeneration).Select(p => p.Key));
        }

        internal bool LoadUsedFilesFromKeyIndex(uint fileId, IKeyIndex info)
        {
            try
            {
                var reader = FileCollection.GetFile(fileId)!.GetExclusiveReader();
                FileKeyIndex.SkipHeader(reader);
                var keyCount = info.KeyValueCount;
                var usedFileIds = new HashSet<uint>();
                if (info.Compression == KeyIndexCompression.Old)
                {
                    for (var i = 0; i < keyCount; i++)
                    {
                        var keyLength = reader.ReadVInt32();
                        reader.SkipBlock(keyLength);
                        var vFileId = reader.ReadVUInt32();
                        if (vFileId > 0) usedFileIds.Add(vFileId);
                        reader.SkipVUInt32();
                        reader.SkipVInt32();
                    }
                }
                else
                {
                    if (info.Compression != KeyIndexCompression.None)
                        return false;
                    for (var i = 0; i < keyCount; i++)
                    {
                        reader.SkipVUInt32();
                        var keyLengthWithoutPrefix = (int)reader.ReadVUInt32();
                        reader.SkipBlock(keyLengthWithoutPrefix);
                        var vFileId = reader.ReadVUInt32();
                        if (vFileId > 0) usedFileIds.Add(vFileId);
                        reader.SkipVUInt32();
                        reader.SkipVInt32();
                    }
                }

                var trlGeneration = GetGeneration(info.TrLogFileId);
                info.UsedFilesInOlderGenerations = usedFileIds.Select(GetGenerationIgnoreMissing).Where(gen => gen > 0 && gen < trlGeneration).OrderBy(a => a).ToArray();
                if (reader.Eof) return true;
                return reader.ReadInt32() == EndOfIndexFileMarker;
            }
            catch (Exception)
            {
                return false;
            }
        }

        bool LoadKeyIndex(uint fileId, IKeyIndex info)
        {
            try
            {
                var reader = FileCollection.GetFile(fileId)!.GetExclusiveReader();
                FileKeyIndex.SkipHeader(reader);
                var keyCount = info.KeyValueCount;
                _nextRoot!.TrLogFileId = info.TrLogFileId;
                _nextRoot.TrLogOffset = info.TrLogOffset;
                _nextRoot.CommitUlong = info.CommitUlong;
                _nextRoot.UlongsArray = info.Ulongs;
                var usedFileIds = new HashSet<uint>();
                if (info.Compression == KeyIndexCompression.Old)
                {
                    _nextRoot.BuildTree(keyCount, () =>
                    {
                        var keyLength = reader.ReadVInt32();
                        var key = ByteBuffer.NewAsync(new byte[Math.Abs(keyLength)]);
                        reader.ReadBlock(key);
                        if (keyLength < 0)
                        {
                            _compression.DecompressKey(ref key);
                        }

                        var vFileId = reader.ReadVUInt32();
                        if (vFileId > 0) usedFileIds.Add(vFileId);
                        return new BTreeLeafMember
                        {
                            Key = key.ToByteArray(),
                            ValueFileId = vFileId,
                            ValueOfs = reader.ReadVUInt32(),
                            ValueSize = reader.ReadVInt32()
                        };
                    });
                }
                else
                {
                    if (info.Compression != KeyIndexCompression.None)
                        return false;
                    var prevKey = ByteBuffer.NewEmpty();
                    _nextRoot.BuildTree(keyCount, () =>
                    {
                        var prefixLen = (int)reader.ReadVUInt32();
                        var keyLengthWithoutPrefix = (int)reader.ReadVUInt32();
                        var key = ByteBuffer.NewAsync(new byte[prefixLen + keyLengthWithoutPrefix]);
                        Array.Copy(prevKey.Buffer, prevKey.Offset, key.Buffer, key.Offset, prefixLen);
                        reader.ReadBlock(key.Slice(prefixLen));
                        prevKey = key;
                        var vFileId = reader.ReadVUInt32();
                        if (vFileId > 0) usedFileIds.Add(vFileId);
                        return new BTreeLeafMember
                        {
                            Key = key.ToByteArray(),
                            ValueFileId = vFileId,
                            ValueOfs = reader.ReadVUInt32(),
                            ValueSize = reader.ReadVInt32()
                        };
                    });
                }

                var trlGeneration = GetGeneration(info.TrLogFileId);
                info.UsedFilesInOlderGenerations = usedFileIds.Select(fi => GetGenerationIgnoreMissing(fi)).Where(gen => gen > 0 && gen < trlGeneration).OrderBy(a => a).ToArray();

                if (reader.Eof) return true;
                if (reader.ReadInt32() == EndOfIndexFileMarker) return true;
                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        void LoadTransactionLogs(uint firstTrLogId, uint firstTrLogOffset, ulong? openUpToCommitUlong)
        {
            while (firstTrLogId != 0 && firstTrLogId != uint.MaxValue)
            {
                _fileIdWithTransactionLog = 0;
                if (LoadTransactionLog(firstTrLogId, firstTrLogOffset, openUpToCommitUlong))
                {
                    _fileIdWithTransactionLog = firstTrLogId;
                }

                firstTrLogOffset = 0;
                _fileIdWithPreviousTransactionLog = firstTrLogId;
                var fileInfo = _fileCollection.FileInfoByIdx(firstTrLogId);
                if (fileInfo == null)
                    return;
                firstTrLogId = ((IFileTransactionLog)fileInfo).NextFileId;
            }
        }

        // Return true if it is suitable for continuing writing new transactions
        bool LoadTransactionLog(uint fileId, uint logOffset, ulong? openUpToCommitUlong)
        {
            if (openUpToCommitUlong.HasValue && _lastCommited.CommitUlong >= openUpToCommitUlong)
            {
                return false;
            }

            var inlineValueBuf = new byte[MaxValueSizeInlineInMemory];
            var stack = new List<NodeIdxPair>();
            var collectionFile = FileCollection.GetFile(fileId);
            var reader = collectionFile!.GetExclusiveReader();
            try
            {
                if (logOffset == 0)
                {
                    FileTransactionLog.SkipHeader(reader);
                }
                else
                {
                    reader.SkipBlock(logOffset);
                }

                if (reader.Eof) return true;
                var afterTemporaryEnd = false;
                var finishReading = false;
                while (!reader.Eof)
                {
                    var command = (KVCommandType)reader.ReadUInt8();
                    if (command == 0 && afterTemporaryEnd)
                    {
                        collectionFile.SetSize(reader.GetCurrentPosition() - 1);
                        return true;
                    }

                    if (finishReading)
                    {
                        return false;
                    }

                    afterTemporaryEnd = false;
                    switch (command & KVCommandType.CommandMask)
                    {
                        case KVCommandType.CreateOrUpdateDeprecated:
                        case KVCommandType.CreateOrUpdate:
                            {
                                if (_nextRoot == null) return false;
                                var keyLen = reader.ReadVInt32();
                                var valueLen = reader.ReadVInt32();
                                var key = new byte[keyLen];
                                reader.ReadBlock(key);
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KVCommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }

                                var ctx = new CreateOrUpdateCtx
                                {
                                    KeyPrefix = Array.Empty<byte>(),
                                    Key = keyBuf,
                                    ValueFileId = fileId,
                                    ValueOfs = (uint)reader.GetCurrentPosition(),
                                    ValueSize = (command & KVCommandType.SecondParamCompressed) != 0 ? -valueLen : valueLen
                                };
                                if (valueLen <= MaxValueSizeInlineInMemory &&
                                    (command & KVCommandType.SecondParamCompressed) == 0)
                                {
                                    reader.ReadBlock(inlineValueBuf, 0, valueLen);
                                    StoreValueInlineInMemory(ByteBuffer.NewSync(inlineValueBuf, 0, valueLen),
                                        out ctx.ValueOfs, out ctx.ValueSize);
                                    ctx.ValueFileId = 0;
                                }
                                else
                                {
                                    reader.SkipBlock(valueLen);
                                }

                                _nextRoot.CreateOrUpdate(ctx);
                            }
                            break;
                        case KVCommandType.EraseOne:
                            {
                                if (_nextRoot == null) return false;
                                var keyLen = reader.ReadVInt32();
                                var key = new byte[keyLen];
                                reader.ReadBlock(key);
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KVCommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }

                                var findResult = _nextRoot.FindKey(stack, out var keyIndex, Array.Empty<byte>(), keyBuf);
                                if (findResult == FindResult.Exact)
                                    _nextRoot.EraseRange(keyIndex, keyIndex);
                            }
                            break;
                        case KVCommandType.EraseRange:
                            {
                                if (_nextRoot == null) return false;
                                var keyLen1 = reader.ReadVInt32();
                                var keyLen2 = reader.ReadVInt32();
                                var key = new byte[keyLen1];
                                reader.ReadBlock(key);
                                var keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KVCommandType.FirstParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }

                                var findResult = _nextRoot.FindKey(stack, out var keyIndex1, Array.Empty<byte>(), keyBuf);
                                if (findResult == FindResult.Previous) keyIndex1++;
                                key = new byte[keyLen2];
                                reader.ReadBlock(key);
                                keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KVCommandType.SecondParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }

                                findResult = _nextRoot.FindKey(stack, out var keyIndex2, Array.Empty<byte>(), keyBuf);
                                if (findResult == FindResult.Next) keyIndex2--;
                                _nextRoot.EraseRange(keyIndex1, keyIndex2);
                            }
                            break;
                        case KVCommandType.DeltaUlongs:
                            {
                                if (_nextRoot == null) return false;
                                var idx = reader.ReadVUInt32();
                                var delta = reader.ReadVUInt64();
                                // overflow is expected in case Ulong is decreasing but that should be rare
                                _nextRoot.SetUlong(idx, unchecked(_nextRoot.GetUlong(idx) + delta));
                            }
                            break;
                        case KVCommandType.TransactionStart:
                            if (!reader.CheckMagic(MagicStartOfTransaction))
                                return false;
                            _nextRoot = _lastCommited.NewTransactionRoot();
                            break;
                        case KVCommandType.CommitWithDeltaUlong:
                            unchecked // overflow is expected in case commitUlong is decreasing but that should be rare
                            {
                                _nextRoot.CommitUlong += reader.ReadVUInt64();
                            }

                            goto case KVCommandType.Commit;
                        case KVCommandType.Commit:
                            _nextRoot.TrLogFileId = fileId;
                            _nextRoot.TrLogOffset = (uint)reader.GetCurrentPosition();
                            _lastCommited = _nextRoot;
                            _nextRoot = null;
                            if (openUpToCommitUlong.HasValue && _lastCommited.CommitUlong >= openUpToCommitUlong)
                            {
                                finishReading = true;
                            }

                            break;
                        case KVCommandType.Rollback:
                            _nextRoot = null;
                            break;
                        case KVCommandType.EndOfFile:
                            return false;
                        case KVCommandType.TemporaryEndOfFile:
                            _lastCommited.TrLogFileId = fileId;
                            _lastCommited.TrLogOffset = (uint)reader.GetCurrentPosition();
                            afterTemporaryEnd = true;
                            break;
                        default:
                            _nextRoot = null;
                            return false;
                    }
                }

                return afterTemporaryEnd;
            }
            catch (EndOfStreamException)
            {
                _nextRoot = null;
                return false;
            }
        }

        void StoreValueInlineInMemory(ByteBuffer value, out uint valueOfs, out int valueSize)
        {
            var inlineValueBuf = value.Buffer;
            var valueLen = value.Length;
            var ofs = value.Offset;
            switch (valueLen)
            {
                case 0:
                    valueOfs = 0;
                    valueSize = 0;
                    break;
                case 1:
                    valueOfs = 0;
                    valueSize = 0x1000000 | (inlineValueBuf[ofs] << 16);
                    break;
                case 2:
                    valueOfs = 0;
                    valueSize = 0x2000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8);
                    break;
                case 3:
                    valueOfs = 0;
                    valueSize = 0x3000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) |
                                inlineValueBuf[ofs + 2];
                    break;
                case 4:
                    valueOfs = inlineValueBuf[ofs + 3];
                    valueSize = 0x4000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) |
                                inlineValueBuf[ofs + 2];
                    break;
                case 5:
                    valueOfs = inlineValueBuf[ofs + 3] | ((uint)inlineValueBuf[ofs + 4] << 8);
                    valueSize = 0x5000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) |
                                inlineValueBuf[ofs + 2];
                    break;
                case 6:
                    valueOfs = inlineValueBuf[ofs + 3] | ((uint)inlineValueBuf[ofs + 4] << 8) |
                               ((uint)inlineValueBuf[ofs + 5] << 16);
                    valueSize = 0x6000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) |
                                inlineValueBuf[ofs + 2];
                    break;
                case 7:
                    valueOfs = inlineValueBuf[ofs + 3] | ((uint)inlineValueBuf[ofs + 4] << 8) |
                               ((uint)inlineValueBuf[ofs + 5] << 16) | (((uint)inlineValueBuf[ofs + 6]) << 24);
                    valueSize = 0x7000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) |
                                inlineValueBuf[ofs + 2];
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        uint LinkTransactionLogFileIds(uint lastestTrLogFileId)
        {
            var nextId = 0u;
            var currentId = lastestTrLogFileId;
            while (currentId != 0)
            {
                var fileInfo = _fileCollection.FileInfoByIdx(currentId);

                var fileTransactionLog = fileInfo as IFileTransactionLog;
                if (fileTransactionLog == null) break;
                fileTransactionLog.NextFileId = nextId;
                nextId = currentId;
                currentId = fileTransactionLog.PreviousFileId;
            }

            return nextId;
        }

        public void Dispose()
        {
            _compactorScheduler?.RemoveCompactAction(_compactFunc!);
            lock (_writeLock)
            {
                if (_writingTransaction != null)
                    throw new BTDBException("Cannot dispose KeyValueDB when writing transaction still running");
                while (_writeWaitingQueue.Count > 0)
                {
                    _writeWaitingQueue.Dequeue().TrySetCanceled();
                }
            }

            if (_writerWithTransactionLog != null)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.TemporaryEndOfFile);
                _fileWithTransactionLog!.HardFlushTruncateSwitchToDisposedMode();
            }
        }

        public bool DurableTransactions { get; set; }

        public IRootNodeInternal ReferenceAndGetLastCommitted()
        {
            return _lastCommited;
        }

        public IFileCollectionWithFileInfos FileCollection => _fileCollection;

        bool IKeyValueDBInternal.ContainsValuesAndDoesNotTouchGeneration(uint fileKey, long dontTouchGeneration)
        {
            return ContainsValuesAndDoesNotTouchGeneration(fileKey, dontTouchGeneration);
        }

        long IKeyValueDBInternal.MaxTrLogFileSize => _maxTrLogFileSize;

        public IRootNodeInternal ReferenceAndGetOldestRoot()
        {
            lock (_usedBTreeRootNodesLock)
            {
                var oldestRoot = _lastCommited;
                foreach (var usedTransaction in _usedBTreeRootNodes)
                {
                    if (unchecked(usedTransaction.TransactionId - oldestRoot.TransactionId) < 0)
                    {
                        oldestRoot = usedTransaction;
                    }
                }

                return oldestRoot;
            }
        }

        public IKeyValueDBTransaction StartTransaction()
        {
            return new KeyValueDBTransaction(this, _lastCommited, false, false);
        }

        public IKeyValueDBTransaction StartReadOnlyTransaction()
        {
            return new KeyValueDBTransaction(this, _lastCommited, false, true);
        }

        public ValueTask<IKeyValueDBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {

                if (_writingTransaction == null)
                {
                    return new ValueTask<IKeyValueDBTransaction>(NewWritingTransactionUnsafe());
                }

                var tcs = new TaskCompletionSource<IKeyValueDBTransaction>();
                _writeWaitingQueue.Enqueue(tcs);

                return new ValueTask<IKeyValueDBTransaction>(tcs.Task);
            }
        }

        public string CalcStats()
        {
            var oldestRoot = (IBTreeRootNode)ReferenceAndGetOldestRoot();
            var lastCommitted = (IBTreeRootNode)ReferenceAndGetLastCommitted();
            try
            {
                var sb = new StringBuilder(
                    $"KeyValueCount:{lastCommitted.CalcKeyCount()}\nFileCount:{FileCollection.GetCount()}\nFileGeneration:{FileCollection.LastFileGeneration}\n");
                sb.Append($"LastTrId:{lastCommitted.TransactionId},TRL:{lastCommitted.TrLogFileId},ComUlong:{lastCommitted.CommitUlong}\n");
                sb.Append($"OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong}\n");
                foreach (var file in _fileCollection.FileInfos)
                {
                    sb.AppendFormat("{0} Size:{1} Type:{2} Gen:{3}\n", file.Key, FileCollection.GetSize(file.Key),
                        file.Value.FileType, file.Value.Generation);
                }

                return sb.ToString();
            }
            finally
            {
                DereferenceRootNodeInternal(oldestRoot);
                DereferenceRootNodeInternal(lastCommitted);
            }
        }

        public bool Compact(CancellationToken cancellation)
        {
            return new Compactor(this, cancellation).Run();
        }

        public void CreateKvi(CancellationToken cancellation)
        {
            CreateIndexFile(cancellation, 0);
        }

        public IKeyValueDBLogger? Logger { get; set; }

        public ulong? PreserveHistoryUpToCommitUlong
        {
            get
            {
                var preserveHistoryUpToCommitUlong = (ulong)Interlocked.Read(ref _preserveHistoryUpToCommitUlong);
                return preserveHistoryUpToCommitUlong == ulong.MaxValue
                    ? null
                    : (ulong?)preserveHistoryUpToCommitUlong;
            }
            set => Interlocked.Exchange(ref _preserveHistoryUpToCommitUlong, (long)(value ?? ulong.MaxValue));
        }

        internal IBTreeRootNode MakeWritableTransaction(KeyValueDBTransaction keyValueDBTransaction,
            IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null)
                    throw new BTDBTransactionRetryException("Another writing transaction already running");
                if (_lastCommited != btreeRoot)
                    throw new BTDBTransactionRetryException("Another writing transaction already finished");
                _writingTransaction = keyValueDBTransaction;
                return btreeRoot.NewTransactionRoot();
            }
        }

        internal void CommitWritingTransaction(IBTreeRootNode btreeRoot, bool temporaryCloseTransactionLog)
        {
            WriteUlongsDiff(btreeRoot.UlongsArray, _lastCommited.UlongsArray);
            var deltaUlong = unchecked(btreeRoot.CommitUlong - _lastCommited.CommitUlong);
            if (deltaUlong != 0)
            {
                _writerWithTransactionLog!.WriteUInt8((byte)KVCommandType.CommitWithDeltaUlong);
                _writerWithTransactionLog.WriteVUInt64(deltaUlong);
            }
            else
            {
                _writerWithTransactionLog!.WriteUInt8((byte)KVCommandType.Commit);
            }
            if (DurableTransactions)
            {
                _fileWithTransactionLog!.HardFlush();
            }
            else
            {
                _fileWithTransactionLog!.Flush();
            }
            UpdateTransactionLogInBTreeRoot(btreeRoot);
            if (temporaryCloseTransactionLog)
            {
                _writerWithTransactionLog!.WriteUInt8((byte)KVCommandType.TemporaryEndOfFile);
                _fileWithTransactionLog!.Flush();
                _fileWithTransactionLog.Truncate();
            }

            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = btreeRoot;
                TryDequeWaiterForWritingTransaction();
            }
        }

        void WriteUlongsDiff(ulong[]? newArray, ulong[]? oldArray)
        {
            var newCount = newArray?.Length ?? 0;
            var oldCount = oldArray?.Length ?? 0;
            var maxCount = Math.Max(newCount, oldCount);
            for (var i = 0; i < maxCount; i++)
            {
                var oldValue = i < oldCount ? oldArray![i] : 0;
                var newValue = i < newCount ? newArray![i] : 0;
                var deltaUlong = unchecked(newValue - oldValue);
                if (deltaUlong != 0)
                {
                    _writerWithTransactionLog!.WriteUInt8((byte)KVCommandType.DeltaUlongs);
                    _writerWithTransactionLog.WriteVUInt32((uint)i);
                    _writerWithTransactionLog.WriteVUInt64(deltaUlong);
                }
            }
        }

        void UpdateTransactionLogInBTreeRoot(IBTreeRootNode btreeRoot)
        {
            // Create new KVI file if new trl file was created, if preserve history is used it this is co
            if (btreeRoot.TrLogFileId != _fileIdWithTransactionLog && btreeRoot.TrLogFileId != 0 && !PreserveHistoryUpToCommitUlong.HasValue)
            {
                _compactorScheduler?.AdviceRunning(false);
            }

            btreeRoot.TrLogFileId = _fileIdWithTransactionLog;
            if (_writerWithTransactionLog != null)
            {
                btreeRoot.TrLogOffset = (uint)_writerWithTransactionLog.GetCurrentPosition();
            }
            else
            {
                btreeRoot.TrLogOffset = 0;
            }
        }

        void TryDequeWaiterForWritingTransaction()
        {
            if (_writeWaitingQueue.Count == 0) return;
            var tcs = _writeWaitingQueue.Dequeue();
            tcs.SetResult(NewWritingTransactionUnsafe());
        }

        KeyValueDBTransaction NewWritingTransactionUnsafe()
        {
            if (_readOnly) throw new BTDBException("Database opened in readonly mode");
            var newTransactionRoot = _lastCommited.NewTransactionRoot();
            var tr = new KeyValueDBTransaction(this, newTransactionRoot, true, false);
            _writingTransaction = tr;
            return tr;
        }

        internal void RevertWritingTransaction(bool nothingWrittenToTransactionLog)
        {
            if (!nothingWrittenToTransactionLog)
            {
                _writerWithTransactionLog!.WriteUInt8((byte)KVCommandType.Rollback);
                _writerWithTransactionLog.FlushBuffer();
                _fileWithTransactionLog!.Flush();
                var newRoot = _lastCommited.CloneRoot();
                UpdateTransactionLogInBTreeRoot(newRoot);
                lock (_writeLock)
                {
                    _writingTransaction = null;
                    _lastCommited = newRoot;
                    TryDequeWaiterForWritingTransaction();
                }
            }
            else
            {
                lock (_writeLock)
                {
                    _writingTransaction = null;
                    TryDequeWaiterForWritingTransaction();
                }
            }
        }

        internal void WriteStartTransaction()
        {
            if (_fileIdWithTransactionLog == 0)
            {
                WriteStartOfNewTransactionLogFile();
            }
            else
            {
                if (_writerWithTransactionLog == null)
                {
                    _fileWithTransactionLog = FileCollection.GetFile(_fileIdWithTransactionLog);
                    _writerWithTransactionLog = _fileWithTransactionLog!.GetAppenderWriter();
                }

                if (_writerWithTransactionLog.GetCurrentPosition() > _maxTrLogFileSize)
                {
                    WriteStartOfNewTransactionLogFile();
                }
            }

            _writerWithTransactionLog!.WriteUInt8((byte)KVCommandType.TransactionStart);
            _writerWithTransactionLog.WriteByteArrayRaw(MagicStartOfTransaction);
        }

        void WriteStartOfNewTransactionLogFile()
        {
            if (_writerWithTransactionLog != null)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.EndOfFile);
                _fileWithTransactionLog!.HardFlushTruncateSwitchToReadOnlyMode();
                _fileIdWithPreviousTransactionLog = _fileIdWithTransactionLog;
            }

            _fileWithTransactionLog = FileCollection.AddFile("trl");
            _fileIdWithTransactionLog = _fileWithTransactionLog.Index;
            var transactionLog = new FileTransactionLog(FileCollection.NextGeneration(), FileCollection.Guid,
                _fileIdWithPreviousTransactionLog);
            _writerWithTransactionLog = _fileWithTransactionLog.GetAppenderWriter();
            transactionLog.WriteHeader(_writerWithTransactionLog);
            FileCollection.SetInfo(_fileIdWithTransactionLog, transactionLog);
        }

        public void WriteCreateOrUpdateCommand(byte[] prefix, ByteBuffer key, ByteBuffer value, out uint valueFileId,
            out uint valueOfs, out int valueSize)
        {
            var command = KVCommandType.CreateOrUpdate;
            if (_compression.ShouldTryToCompressKey(prefix.Length + key.Length))
            {
                if (prefix.Length != 0)
                {
                    var fullKey = new byte[prefix.Length + key.Length];
                    Array.Copy(prefix, 0, fullKey, 0, prefix.Length);
                    Array.Copy(key.Buffer, key.Offset, fullKey, prefix.Length, key.Length);
                    prefix = Array.Empty<byte>();
                    key = ByteBuffer.NewAsync(fullKey);
                }

                if (_compression.CompressKey(ref key))
                {
                    command |= KVCommandType.FirstParamCompressed;
                }
            }

            valueSize = value.Length;
            if (_compression.CompressValue(ref value))
            {
                command |= KVCommandType.SecondParamCompressed;
                valueSize = -value.Length;
            }

            var trlPos = _writerWithTransactionLog!.GetCurrentPosition();
            if (trlPos > 256 && trlPos + prefix.Length + key.Length + 16 + value.Length > _maxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }

            _writerWithTransactionLog!.WriteUInt8((byte)command);
            _writerWithTransactionLog.WriteVInt32(prefix.Length + key.Length);
            _writerWithTransactionLog.WriteVInt32(value.Length);
            _writerWithTransactionLog.WriteBlock(prefix);
            _writerWithTransactionLog.WriteBlock(key);
            if (valueSize != 0)
            {
                if (valueSize > 0 && valueSize < MaxValueSizeInlineInMemory)
                {
                    StoreValueInlineInMemory(value, out valueOfs, out valueSize);
                    valueFileId = 0;
                }
                else
                {
                    valueFileId = _fileIdWithTransactionLog;
                    valueOfs = (uint)_writerWithTransactionLog.GetCurrentPosition();
                }

                _writerWithTransactionLog!.WriteBlock(value);
            }
            else
            {
                valueFileId = 0;
                valueOfs = 0;
            }
        }

        public static uint CalcValueSize(uint valueFileId, uint valueOfs, int valueSize)
        {
            if (valueSize == 0) return 0;
            if (valueFileId == 0)
            {
                return (uint)(valueSize >> 24);
            }

            return (uint)Math.Abs(valueSize);
        }

        public ByteBuffer ReadValue(uint valueFileId, uint valueOfs, int valueSize)
        {
            if (valueSize == 0) return ByteBuffer.NewEmpty();
            if (valueFileId == 0)
            {
                var len = valueSize >> 24;
                var buf = new byte[len];
                switch (len)
                {
                    case 7:
                        buf[6] = (byte)(valueOfs >> 24);
                        goto case 6;
                    case 6:
                        buf[5] = (byte)(valueOfs >> 16);
                        goto case 5;
                    case 5:
                        buf[4] = (byte)(valueOfs >> 8);
                        goto case 4;
                    case 4:
                        buf[3] = (byte)valueOfs;
                        goto case 3;
                    case 3:
                        buf[2] = (byte)valueSize;
                        goto case 2;
                    case 2:
                        buf[1] = (byte)(valueSize >> 8);
                        goto case 1;
                    case 1:
                        buf[0] = (byte)(valueSize >> 16);
                        break;
                    default:
                        throw new BTDBException("Corrupted DB");
                }

                return ByteBuffer.NewAsync(buf);
            }

            var compressed = false;
            if (valueSize < 0)
            {
                compressed = true;
                valueSize = -valueSize;
            }

            var result = ByteBuffer.NewAsync(new byte[valueSize]);
            var file = FileCollection.GetFile(valueFileId);
            if (file == null)
                throw new BTDBException(
                    $"ReadValue({valueFileId},{valueOfs},{valueSize}) compressed: {compressed} file does not exist in {CalcStats()}");
            file.RandomRead(result.Buffer.AsSpan(0, valueSize), valueOfs, false);
            if (compressed)
                _compression.DecompressValue(ref result);
            return result;
        }

        public void WriteEraseOneCommand(ByteBuffer key)
        {
            var command = KVCommandType.EraseOne;
            if (_compression.ShouldTryToCompressKey(key.Length))
            {
                if (_compression.CompressKey(ref key))
                {
                    command |= KVCommandType.FirstParamCompressed;
                }
            }

            if (_writerWithTransactionLog!.GetCurrentPosition() > _maxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }

            _writerWithTransactionLog!.WriteUInt8((byte)command);
            _writerWithTransactionLog.WriteVInt32(key.Length);
            _writerWithTransactionLog.WriteBlock(key);
        }

        public void WriteEraseRangeCommand(ByteBuffer firstKey, ByteBuffer secondKey)
        {
            var command = KVCommandType.EraseRange;
            if (_compression.ShouldTryToCompressKey(firstKey.Length))
            {
                if (_compression.CompressKey(ref firstKey))
                {
                    command |= KVCommandType.FirstParamCompressed;
                }
            }

            if (_compression.ShouldTryToCompressKey(secondKey.Length))
            {
                if (_compression.CompressKey(ref secondKey))
                {
                    command |= KVCommandType.SecondParamCompressed;
                }
            }

            if (_writerWithTransactionLog!.GetCurrentPosition() > _maxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }

            _writerWithTransactionLog!.WriteUInt8((byte)command);
            _writerWithTransactionLog.WriteVInt32(firstKey.Length);
            _writerWithTransactionLog.WriteVInt32(secondKey.Length);
            _writerWithTransactionLog.WriteBlock(firstKey);
            _writerWithTransactionLog.WriteBlock(secondKey);
        }

        uint CreateKeyIndexFile(IBTreeRootNode root, CancellationToken cancellation, bool fullSpeed)
        {
            var bytesPerSecondLimiter = new BytesPerSecondLimiter(fullSpeed ? 0 : CompactorWriteBytesPerSecondLimit);
            var file = FileCollection.AddFile("kvi");
            var writer = file.GetExclusiveAppenderWriter();
            var keyCount = root.CalcKeyCount();
            if (root.TrLogFileId != 0)
                FileCollection.ConcurentTemporaryTruncate(root.TrLogFileId, root.TrLogOffset);
            var keyIndex = new FileKeyIndex(FileCollection.NextGeneration(), FileCollection.Guid, root.TrLogFileId,
                root.TrLogOffset, keyCount, root.CommitUlong, KeyIndexCompression.None, root.UlongsArray);
            keyIndex.WriteHeader(writer);
            var usedFileIds = new HashSet<uint>();
            if (keyCount > 0)
            {
                var stack = new List<NodeIdxPair>();
                var prevKey = ByteBuffer.NewEmpty();
                root.FillStackByIndex(stack, 0);
                do
                {
                    cancellation.ThrowIfCancellationRequested();
                    var nodeIdxPair = stack[^1];
                    var memberValue = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
                    var key = ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx);
                    var prefixLen = 0;
                    var minLen = Math.Min(prevKey.Length, key.Length);
                    for (var i = 0; i < minLen; i++)
                    {
                        if (prevKey[i] == key[i]) continue;
                        prefixLen = i;
                        break;
                    }
                    writer.WriteVUInt32((uint)prefixLen);
                    writer.WriteVUInt32((uint)(key.Length - prefixLen));
                    writer.WriteBlock(key.Slice(prefixLen));
                    var vFileId = memberValue.ValueFileId;
                    if (vFileId > 0) usedFileIds.Add(vFileId);
                    writer.WriteVUInt32(vFileId);
                    writer.WriteVUInt32(memberValue.ValueOfs);
                    writer.WriteVInt32(memberValue.ValueSize);
                    prevKey = key;
                    bytesPerSecondLimiter.Limit((ulong)writer.GetCurrentPosition());
                } while (root.FindNextKey(stack));
            }
            file.HardFlush();
            writer.WriteInt32(EndOfIndexFileMarker);
            file.HardFlushTruncateSwitchToDisposedMode();
            var trlGeneration = GetGeneration(keyIndex.TrLogFileId);
            keyIndex.UsedFilesInOlderGenerations = usedFileIds.Select(GetGenerationIgnoreMissing)
                .Where(gen => gen < trlGeneration).OrderBy(a => a).ToArray();
            FileCollection.SetInfo(file.Index, keyIndex);
            Logger?.KeyValueIndexCreated(file.Index, keyIndex.KeyValueCount, file.GetSize(), TimeSpan.FromMilliseconds(bytesPerSecondLimiter.TotalTimeInMs));
            return file.Index;
        }

        internal bool ContainsValuesAndDoesNotTouchGeneration(uint fileId, long dontTouchGeneration)
        {
            var info = FileCollection.FileInfoByIdx(fileId);
            if (info == null) return false;
            if (info.Generation >= dontTouchGeneration) return false;
            return info.FileType == KVFileType.TransactionLog || info.FileType == KVFileType.PureValues;
        }

        void IKeyValueDBInternal.CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration)
        {
            CreateIndexFile(cancellation, preserveKeyIndexGeneration);
        }

        AbstractBufferedWriter IKeyValueDBInternal.StartPureValuesFile(out uint fileId)
        {
            var fId = FileCollection.AddFile("pvl");
            fileId = fId.Index;
            var pureValues = new FilePureValues(FileCollection.NextGeneration(), FileCollection.Guid);
            var writer = fId.GetAppenderWriter();
            FileCollection.SetInfo(fId.Index, pureValues);
            pureValues.WriteHeader(writer);
            return writer;
        }

        public long ReplaceBTreeValues(CancellationToken cancellation, Dictionary<ulong, ulong> newPositionMap)
        {
            var ctx = new ReplaceValuesCtx
            {
                _cancellation = cancellation,
                _newPositionMap = newPositionMap
            };
            while (true)
            {
                ctx._iterationTimeOut = DateTime.UtcNow + TimeSpan.FromMilliseconds(50);
                ctx._interrupt = false;
                using (var tr = StartWritingTransaction().Result)
                {
                    var newRoot = ((KeyValueDBTransaction)tr).BtreeRoot;
                    newRoot.ReplaceValues(ctx);
                    cancellation.ThrowIfCancellationRequested();
                    lock (_writeLock)
                    {
                        _lastCommited = newRoot;
                    }

                    if (!ctx._interrupt)
                    {
                        return newRoot.TransactionId;
                    }
                }

                Thread.Sleep(10);
            }
        }

        public void MarkAsUnknown(IEnumerable<uint> fileIds)
        {
            foreach (var fileId in fileIds)
            {
                _fileCollection.MakeIdxUnknown(fileId);
            }
        }

        public long GetGeneration(uint fileId)
        {
            if (fileId == 0) return -1;
            var fileInfo = FileCollection.FileInfoByIdx(fileId);
            if (fileInfo == null)
            {
                throw new ArgumentOutOfRangeException(nameof(fileId));
            }

            return fileInfo.Generation;
        }

        internal long GetGenerationIgnoreMissing(uint fileId)
        {
            if (fileId == 0) return -1;
            var fileInfo = FileCollection.FileInfoByIdx(fileId);
            if (fileInfo == null)
            {
                return -1;
            }
            return fileInfo.Generation;
        }

        internal void StartedUsingBTreeRoot(IBTreeRootNode btreeRoot)
        {
            lock (_usedBTreeRootNodesLock)
            {
                var uses = btreeRoot.UseCount;
                uses++;
                btreeRoot.UseCount = uses;
                if (uses == 1)
                {
                    _usedBTreeRootNodes.Add(btreeRoot);
                }
            }
        }

        internal void FinishedUsingBTreeRoot(IBTreeRootNode btreeRoot)
        {
            lock (_usedBTreeRootNodesLock)
            {
                var uses = btreeRoot.UseCount;
                uses--;
                btreeRoot.UseCount = uses;
                if (uses == 0)
                {
                    _usedBTreeRootNodes.Remove(btreeRoot);
                }
            }
        }

        bool IKeyValueDBInternal.AreAllTransactionsBeforeFinished(long transactionId)
        {
            lock (_usedBTreeRootNodesLock)
            {
                foreach (var usedTransaction in _usedBTreeRootNodes)
                {
                    if (usedTransaction.TransactionId - transactionId >= 0) continue;
                    return false;
                }

                return true;
            }
        }

        internal ulong DistanceFromLastKeyIndex(IBTreeRootNode root)
        {
            var keyIndex = FileCollection.FileInfos.Where(p => p.Value.FileType == KVFileType.KeyIndex)
                .Select(p => (IKeyIndex)p.Value).FirstOrDefault();
            if (keyIndex == null)
            {
                if (FileCollection.FileInfos.Count(p => p.Value.SubDBId == 0) > 1) return ulong.MaxValue;
                return root.TrLogOffset;
            }

            if (root.TrLogFileId != keyIndex.TrLogFileId) return ulong.MaxValue;
            return root.TrLogOffset - keyIndex.TrLogOffset;
        }

        public T? GetSubDB<T>(long id) where T : class
        {
            if (_subDBs.TryGetValue(id, out var subDB))
            {
                if (!(subDB is T)) throw new ArgumentException($"SubDB of id {id} is not type {typeof(T).FullName}");
                return (T)subDB;
            }

            if (typeof(T) == typeof(IChunkStorage))
            {
                subDB = new ChunkStorageInKV(id, _fileCollection, _maxTrLogFileSize);
            }

            _subDBs.Add(id, subDB);
            return (T)subDB;
        }

        public void DereferenceRootNodeInternal(IRootNodeInternal root)
        {
            // Managed implementation does not need reference counting => nothing to do
        }
    }
}
