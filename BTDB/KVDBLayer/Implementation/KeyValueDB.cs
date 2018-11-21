using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer.BTree;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class KeyValueDB : IKeyValueDB, IHaveSubDB
    {
        const int MaxValueSizeInlineInMemory = 7;
        const int EndOfIndexFileMarker = 0x1234DEAD;
        IBTreeRootNode _lastCommited;
        long _preserveHistoryUpToCommitUlong; // it is long only because Interlock.Read is just long cappable, MaxValue means no preserving history
        IBTreeRootNode _nextRoot;
        KeyValueDBTransaction _writingTransaction;
        readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new Queue<TaskCompletionSource<IKeyValueDBTransaction>>();
        readonly object _writeLock = new object();
        uint _fileIdWithTransactionLog;
        uint _fileIdWithPreviousTransactionLog;
        IFileCollectionFile _fileWithTransactionLog;
        AbstractBufferedWriter _writerWithTransactionLog;
        static readonly byte[] MagicStartOfTransaction = { (byte)'t', (byte)'R' };
        internal readonly long MaxTrLogFileSize;
        readonly ICompressionStrategy _compression;
        readonly ICompactorScheduler _compactorScheduler;
        readonly HashSet<IBTreeRootNode> _usedBTreeRootNodes = new HashSet<IBTreeRootNode>(ReferenceEqualityComparer<IBTreeRootNode>.Instance);
        readonly object _usedBTreeRootNodesLock = new object();
        readonly IFileCollectionWithFileInfos _fileCollection;
        readonly Dictionary<long, object> _subDBs = new Dictionary<long, object>();
        readonly Func<CancellationToken, bool> _compactFunc;

        public KeyValueDB(IFileCollection fileCollection)
            : this(fileCollection, new SnappyCompressionStrategy())
        {
        }

        public KeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression,
            uint fileSplitSize = int.MaxValue)
            : this(fileCollection, compression, fileSplitSize, CompactorScheduler.Instance)
        {
        }

        public KeyValueDB(IFileCollection fileCollection, ICompressionStrategy compression, uint fileSplitSize, ICompactorScheduler compactorScheduler)
            : this(new KeyValueDBOptions { FileCollection = fileCollection, Compression = compression, FileSplitSize = fileSplitSize, CompactorScheduler = compactorScheduler })
        {
        }

        public KeyValueDB(KeyValueDBOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options.FileCollection == null) throw new ArgumentNullException(nameof(options.FileCollection));
            if (options.Compression == null) throw new ArgumentNullException(nameof(options.Compression));
            if (options.FileSplitSize < 1024 || options.FileSplitSize > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(options.FileSplitSize), "Allowed range 1024 - 2G");
            _compactorScheduler = options.CompactorScheduler;
            MaxTrLogFileSize = options.FileSplitSize;
            _compression = options.Compression;
            DurableTransactions = false;
            _fileCollection = new FileCollectionWithFileInfos(options.FileCollection);
            _lastCommited = new BTreeRoot(0);
            _preserveHistoryUpToCommitUlong = (long)(options.PreserveHistoryUpToCommitUlong ?? ulong.MaxValue);
            LoadInfoAboutFiles(options.OpenUpToCommitUlong);
            _compactFunc = _compactorScheduler?.AddCompactAction(Compact);
            _compactorScheduler?.AdviceRunning(true);
        }

        internal List<KeyIndexInfo> BuildKeyIndexInfos()
        {
            var keyIndexes = new List<KeyIndexInfo>();
            foreach (var fileInfo in _fileCollection.FileInfos)
            {
                var keyIndex = fileInfo.Value as IKeyIndex;
                if (keyIndex == null) continue;
                keyIndexes.Add(new KeyIndexInfo { Key = fileInfo.Key, Generation = keyIndex.Generation, CommitUlong = keyIndex.CommitUlong });
            }
            if (keyIndexes.Count > 1)
                keyIndexes.Sort((l, r) => Comparer<long>.Default.Compare(l.Generation, r.Generation));
            return keyIndexes;
        }

        void LoadInfoAboutFiles(ulong? openUpToCommitUlong)
        {
            long latestGeneration = -1;
            uint lastestTrLogFileId = 0;
            foreach (var fileInfo in _fileCollection.FileInfos)
            {
                var trLog = fileInfo.Value as IFileTransactionLog;
                if (trLog == null) continue;
                if (trLog.Generation > latestGeneration)
                {
                    latestGeneration = trLog.Generation;
                    lastestTrLogFileId = fileInfo.Key;
                }
            }
            var keyIndexes = BuildKeyIndexInfos();
            var preserveKeyIndexKey = CalculatePreserveKeyIndexKeyFromKeyIndexInfos(keyIndexes);
            var preserveKeyIndexGeneration = CalculatePreserveKeyIndexGeneration(preserveKeyIndexKey);
            var firstTrLogId = LinkTransactionLogFileIds(lastestTrLogFileId);
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
                _nextRoot = LastCommited.NewTransactionRoot();
                if (LoadKeyIndex(keyIndex.Key, info) && firstTrLogId <= info.TrLogFileId)
                {
                    _lastCommited = _nextRoot;
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
                var keyIndex = keyIndexes[keyIndexes.Count - 1];
                keyIndexes.RemoveAt(keyIndexes.Count - 1);
                if (keyIndex.Key != preserveKeyIndexKey)
                    _fileCollection.MakeIdxUnknown(keyIndex.Key);
            }
            LoadTransactionLogs(firstTrLogId, firstTrLogOffset, openUpToCommitUlong);
            if (openUpToCommitUlong.HasValue || lastestTrLogFileId != firstTrLogId && firstTrLogId != 0 || !hasKeyIndex && _fileCollection.FileInfos.Any(p => p.Value.SubDBId == 0))
            {
                // Need to create new trl if cannot append to last one so it is then written to kvi
                if (openUpToCommitUlong.HasValue && _fileIdWithTransactionLog == 0)
                {
                    WriteStartOfNewTransactionLogFile();
                    FlushCurrentTrl();
                    UpdateTransactionLogInBTreeRoot(LastCommited);
                }
                CreateIndexFile(CancellationToken.None, preserveKeyIndexGeneration);
            }
            _fileCollection.DeleteAllUnknownFiles();
        }

        internal long CalculatePreserveKeyIndexGeneration(uint preserveKeyIndexKey)
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

        internal uint CalculatePreserveKeyIndexKeyFromKeyIndexInfos(List<KeyIndexInfo> keyIndexes)
        {
            var preserveKeyIndexKey = uint.MaxValue;
            var preserveHistoryUpToCommitUlong = (ulong)Interlocked.Read(ref _preserveHistoryUpToCommitUlong);
            if (preserveHistoryUpToCommitUlong != ulong.MaxValue)
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

        internal void CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration)
        {
            var idxFileId = CreateKeyIndexFile(LastCommited, cancellation);
            MarkAsUnknown(_fileCollection.FileInfos.Where(p => p.Value.FileType == KVFileType.KeyIndex && p.Key != idxFileId && p.Value.Generation != preserveKeyIndexGeneration).Select(p => p.Key));
        }

        internal bool LoadUsedFilesFromKeyIndex(uint fileId, IKeyIndex info)
        {
            try
            {
                var reader = FileCollection.GetFile(fileId).GetExclusiveReader();
                FileKeyIndex.SkipHeader(reader);
                var keyCount = info.KeyValueCount;
                HashSet<uint> usedFileIds = new HashSet<uint>();
                if (info.Compression == KeyIndexCompression.Old)
                {
                    for (int i = 0; i < keyCount; i++)
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
                    for (int i = 0; i < keyCount; i++)
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
                try
                {
                    info.UsedFilesInOlderGenerations = usedFileIds.Select(fi => GetGeneration(fi)).Where(gen => gen < trlGeneration).OrderBy(a => a).ToArray();
                }
                catch (ArgumentOutOfRangeException)
                {
                    // KVI uses missing pvl/trl file
                    info.UsedFilesInOlderGenerations = new long[0];
                    return false;
                }
                if (reader.Eof) return true;
                if (reader.ReadInt32() == EndOfIndexFileMarker) return true;
                return false;
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
                var reader = FileCollection.GetFile(fileId).GetExclusiveReader();
                FileKeyIndex.SkipHeader(reader);
                var keyCount = info.KeyValueCount;
                _nextRoot.TrLogFileId = info.TrLogFileId;
                _nextRoot.TrLogOffset = info.TrLogOffset;
                _nextRoot.CommitUlong = info.CommitUlong;
                _nextRoot.UlongsArray = info.Ulongs;
                HashSet<uint> usedFileIds = new HashSet<uint>();
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
                        reader.ReadBlock(key.SubBuffer(prefixLen));
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
                try
                {
                    info.UsedFilesInOlderGenerations = usedFileIds.Select(fi => GetGeneration(fi)).Where(gen => gen < trlGeneration).OrderBy(a => a).ToArray();
                }
                catch (ArgumentOutOfRangeException)
                {
                    // KVI uses missing pvl/trl file
                    return false;
                }
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
            if (openUpToCommitUlong.HasValue && LastCommited.CommitUlong >= openUpToCommitUlong)
            {
                return false;
            }
            var inlineValueBuf = new byte[MaxValueSizeInlineInMemory];
            var stack = new List<NodeIdxPair>();
            var collectionFile = FileCollection.GetFile(fileId);
            var reader = collectionFile.GetExclusiveReader();
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
                                    KeyPrefix = BitArrayManipulation.EmptyByteArray,
                                    Key = keyBuf,
                                    ValueFileId = fileId,
                                    ValueOfs = (uint)reader.GetCurrentPosition(),
                                    ValueSize = (command & KVCommandType.SecondParamCompressed) != 0 ? -valueLen : valueLen
                                };
                                if (valueLen <= MaxValueSizeInlineInMemory && (command & KVCommandType.SecondParamCompressed) == 0)
                                {
                                    reader.ReadBlock(inlineValueBuf, 0, valueLen);
                                    StoreValueInlineInMemory(ByteBuffer.NewSync(inlineValueBuf, 0, valueLen), out ctx.ValueOfs, out ctx.ValueSize);
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
                                long keyIndex;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex, BitArrayManipulation.EmptyByteArray, keyBuf);
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
                                long keyIndex1;
                                var findResult = _nextRoot.FindKey(stack, out keyIndex1, BitArrayManipulation.EmptyByteArray, keyBuf);
                                if (findResult == FindResult.Previous) keyIndex1++;
                                key = new byte[keyLen2];
                                reader.ReadBlock(key);
                                keyBuf = ByteBuffer.NewAsync(key);
                                if ((command & KVCommandType.SecondParamCompressed) != 0)
                                {
                                    _compression.DecompressKey(ref keyBuf);
                                }
                                long keyIndex2;
                                findResult = _nextRoot.FindKey(stack, out keyIndex2, BitArrayManipulation.EmptyByteArray, keyBuf);
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
                            _nextRoot = LastCommited.NewTransactionRoot();
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
                    valueSize = 0x3000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 4:
                    valueOfs = inlineValueBuf[ofs + 3];
                    valueSize = 0x4000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 5:
                    valueOfs = inlineValueBuf[ofs + 3] | ((uint)inlineValueBuf[ofs + 4] << 8);
                    valueSize = 0x5000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 6:
                    valueOfs = inlineValueBuf[ofs + 3] | ((uint)inlineValueBuf[ofs + 4] << 8) | ((uint)inlineValueBuf[ofs + 5] << 16);
                    valueSize = 0x6000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
                    break;
                case 7:
                    valueOfs = inlineValueBuf[ofs + 3] | ((uint)inlineValueBuf[ofs + 4] << 8) | ((uint)inlineValueBuf[ofs + 5] << 16) | (((uint)inlineValueBuf[ofs + 6]) << 24);
                    valueSize = 0x7000000 | (inlineValueBuf[ofs] << 16) | (inlineValueBuf[ofs + 1] << 8) | inlineValueBuf[ofs + 2];
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
                if (fileInfo == null)
                {
                    break;
                }
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
            _compactorScheduler?.RemoveCompactAction(_compactFunc);
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBException("Cannot dispose KeyValueDB when writting transaction still running");
                while (_writeWaitingQueue.Count > 0)
                {
                    _writeWaitingQueue.Dequeue().TrySetCanceled();
                }
            }
            if (_writerWithTransactionLog != null)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.TemporaryEndOfFile);
                _writerWithTransactionLog.FlushBuffer();
                _fileWithTransactionLog.HardFlush();
                _fileWithTransactionLog.Truncate();
            }
        }

        public bool DurableTransactions { get; set; }

        internal IBTreeRootNode LastCommited => _lastCommited;

        internal IFileCollectionWithFileInfos FileCollection => _fileCollection;

        internal IBTreeRootNode OldestRoot
        {
            get
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
        }

        public IKeyValueDBTransaction StartTransaction()
        {
            return new KeyValueDBTransaction(this, LastCommited, false, false);
        }

        public IKeyValueDBTransaction StartReadOnlyTransaction()
        {
            return new KeyValueDBTransaction(this, LastCommited, false, true);
        }

        public Task<IKeyValueDBTransaction> StartWritingTransaction()
        {
            lock (_writeLock)
            {
                var tcs = new TaskCompletionSource<IKeyValueDBTransaction>();
                if (_writingTransaction == null)
                {
                    NewWrittingTransactionUnsafe(tcs);
                }
                else
                {
                    _writeWaitingQueue.Enqueue(tcs);
                }
                return tcs.Task;
            }
        }

        public string CalcStats()
        {
            var sb = new StringBuilder("KeyValueCount:" + LastCommited.CalcKeyCount() + Environment.NewLine
                                       + "FileCount:" + FileCollection.GetCount() + Environment.NewLine
                                       + "FileGeneration:" + FileCollection.LastFileGeneration + Environment.NewLine);
            foreach (var file in _fileCollection.FileInfos)
            {
                sb.AppendFormat("{0} Size:{1} Type:{2} Gen:{3}{4}", file.Key, FileCollection.GetSize(file.Key),
                                file.Value.FileType, file.Value.Generation, Environment.NewLine);
            }
            return sb.ToString();
        }

        public bool Compact(CancellationToken cancellation)
        {
            return new Compactor(this, cancellation).Run();
        }

        public IKeyValueDBLogger Logger { get; set; }

        public ulong? PreserveHistoryUpToCommitUlong
        {
            get
            {
                var preserveHistoryUpToCommitUlong = (ulong)Interlocked.Read(ref _preserveHistoryUpToCommitUlong);
                return preserveHistoryUpToCommitUlong == ulong.MaxValue ? null : (ulong?)preserveHistoryUpToCommitUlong;
            }
            set { Interlocked.Exchange(ref _preserveHistoryUpToCommitUlong, (long)(value ?? ulong.MaxValue)); }
        }

        internal IBTreeRootNode MakeWrittableTransaction(KeyValueDBTransaction keyValueDBTransaction, IBTreeRootNode btreeRoot)
        {
            lock (_writeLock)
            {
                if (_writingTransaction != null) throw new BTDBTransactionRetryException("Another writting transaction already running");
                if (LastCommited != btreeRoot) throw new BTDBTransactionRetryException("Another writting transaction already finished");
                _writingTransaction = keyValueDBTransaction;
                return btreeRoot.NewTransactionRoot();
            }
        }

        internal void CommitWrittingTransaction(IBTreeRootNode btreeRoot, bool temporaryCloseTransactionLog)
        {
            WriteUlongsDiff(btreeRoot.UlongsArray, _lastCommited.UlongsArray);
            var deltaUlong = unchecked(btreeRoot.CommitUlong - _lastCommited.CommitUlong);
            if (deltaUlong != 0)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.CommitWithDeltaUlong);
                _writerWithTransactionLog.WriteVUInt64(deltaUlong);
            }
            else
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.Commit);
            }
            if (DurableTransactions || !temporaryCloseTransactionLog)
                _writerWithTransactionLog.FlushBuffer();
            UpdateTransactionLogInBTreeRoot(btreeRoot);
            if (DurableTransactions)
                _fileWithTransactionLog.HardFlush();
            if (temporaryCloseTransactionLog)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.TemporaryEndOfFile);
                _writerWithTransactionLog.FlushBuffer();
                if (DurableTransactions)
                    _fileWithTransactionLog.HardFlush();
                _fileWithTransactionLog.Truncate();
            }
            lock (_writeLock)
            {
                _writingTransaction = null;
                _lastCommited = btreeRoot;
                TryDequeWaiterForWrittingTransaction();
            }
        }

        void WriteUlongsDiff(ulong[] newArray, ulong[] oldArray)
        {
            var newCount = newArray != null ? newArray.Length : 0;
            var oldCount = oldArray != null ? oldArray.Length : 0;
            var maxCount = Math.Max(newCount, oldCount);
            for (var i = 0; i < maxCount; i++)
            {
                var oldValue = i < oldCount ? oldArray[i] : 0;
                var newValue = i < newCount ? newArray[i] : 0;
                var deltaUlong = unchecked(newValue - oldValue);
                if (deltaUlong != 0)
                {
                    _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.DeltaUlongs);
                    _writerWithTransactionLog.WriteVUInt32((uint)i);
                    _writerWithTransactionLog.WriteVUInt64(deltaUlong);
                }
            }
        }

        void UpdateTransactionLogInBTreeRoot(IBTreeRootNode btreeRoot)
        {
            if (btreeRoot.TrLogFileId != _fileIdWithTransactionLog && btreeRoot.TrLogFileId != 0)
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

        void TryDequeWaiterForWrittingTransaction()
        {
            if (_writeWaitingQueue.Count == 0) return;
            var tcs = _writeWaitingQueue.Dequeue();
            NewWrittingTransactionUnsafe(tcs);
        }

        void NewWrittingTransactionUnsafe(TaskCompletionSource<IKeyValueDBTransaction> tcs)
        {
            var newTransactionRoot = LastCommited.NewTransactionRoot();
            _writingTransaction = new KeyValueDBTransaction(this, newTransactionRoot, true, false);
            tcs.SetResult(_writingTransaction);
        }

        internal void RevertWrittingTransaction(bool nothingWrittenToTransactionLog)
        {
            if (!nothingWrittenToTransactionLog)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.Rollback);
                _writerWithTransactionLog.FlushBuffer();
                var newRoot = _lastCommited.CloneRoot();
                UpdateTransactionLogInBTreeRoot(newRoot);
                lock (_writeLock)
                {
                    _writingTransaction = null;
                    _lastCommited = newRoot;
                    TryDequeWaiterForWrittingTransaction();
                }
            }
            else
            {
                lock (_writeLock)
                {
                    _writingTransaction = null;
                    TryDequeWaiterForWrittingTransaction();
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
                    _writerWithTransactionLog = _fileWithTransactionLog.GetAppenderWriter();
                }
                if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
                {
                    WriteStartOfNewTransactionLogFile();
                }
            }
            _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.TransactionStart);
            _writerWithTransactionLog.WriteByteArrayRaw(MagicStartOfTransaction);
        }

        void WriteStartOfNewTransactionLogFile()
        {
            if (_writerWithTransactionLog != null)
            {
                _writerWithTransactionLog.WriteUInt8((byte)KVCommandType.EndOfFile);
                FlushCurrentTrl();
                _fileIdWithPreviousTransactionLog = _fileIdWithTransactionLog;
            }
            _fileWithTransactionLog = FileCollection.AddFile("trl");
            _fileIdWithTransactionLog = _fileWithTransactionLog.Index;
            var transactionLog = new FileTransactionLog(FileCollection.NextGeneration(), FileCollection.Guid, _fileIdWithPreviousTransactionLog);
            _writerWithTransactionLog = _fileWithTransactionLog.GetAppenderWriter();
            transactionLog.WriteHeader(_writerWithTransactionLog);
            FileCollection.SetInfo(_fileIdWithTransactionLog, transactionLog);
        }

        void FlushCurrentTrl()
        {
            _writerWithTransactionLog.FlushBuffer();
            _fileWithTransactionLog.HardFlush();
            _fileWithTransactionLog.Truncate();
        }

        public void WriteCreateOrUpdateCommand(byte[] prefix, ByteBuffer key, ByteBuffer value, out uint valueFileId, out uint valueOfs, out int valueSize)
        {
            var command = KVCommandType.CreateOrUpdate;
            if (_compression.ShouldTryToCompressKey(prefix.Length + key.Length))
            {
                if (prefix.Length != 0)
                {
                    var fullkey = new byte[prefix.Length + key.Length];
                    Array.Copy(prefix, 0, fullkey, 0, prefix.Length);
                    Array.Copy(key.Buffer, key.Offset, fullkey, prefix.Length, key.Length);
                    prefix = BitArrayManipulation.EmptyByteArray;
                    key = ByteBuffer.NewAsync(fullkey);
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
            var trlPos = _writerWithTransactionLog.GetCurrentPosition();
            if (trlPos > 256 && trlPos + prefix.Length + key.Length + 16 + value.Length > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)command);
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
                _writerWithTransactionLog.WriteBlock(value);
            }
            else
            {
                valueFileId = 0;
                valueOfs = 0;
            }
        }

        public uint CalcValueSize(uint valueFileId, uint valueOfs, int valueSize)
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
                throw new BTDBException($"ReadValue({valueFileId},{valueOfs},{valueSize}) compressed: {compressed} file does not exist in {CalcStats()}");
            file.RandomRead(result.Buffer, 0, valueSize, valueOfs, false);
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
            if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)command);
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
            if (_writerWithTransactionLog.GetCurrentPosition() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
            _writerWithTransactionLog.WriteUInt8((byte)command);
            _writerWithTransactionLog.WriteVInt32(firstKey.Length);
            _writerWithTransactionLog.WriteVInt32(secondKey.Length);
            _writerWithTransactionLog.WriteBlock(firstKey);
            _writerWithTransactionLog.WriteBlock(secondKey);
        }

        uint CreateKeyIndexFile(IBTreeRootNode root, CancellationToken cancellation)
        {
            var start = DateTime.UtcNow;
            var file = FileCollection.AddFile("kvi");
            var writer = file.GetAppenderWriter();
            var keyCount = root.CalcKeyCount();
            if (root.TrLogFileId != 0)
                FileCollection.ConcurentTemporaryTruncate(root.TrLogFileId, root.TrLogOffset);
            var keyIndex = new FileKeyIndex(FileCollection.NextGeneration(), FileCollection.Guid, root.TrLogFileId, root.TrLogOffset, keyCount, root.CommitUlong, KeyIndexCompression.None, root.UlongsArray);
            keyIndex.WriteHeader(writer);
            HashSet<uint> usedFileIds = new HashSet<uint>();
            if (keyCount > 0)
            {
                var stack = new List<NodeIdxPair>();
                var prevKey = ByteBuffer.NewEmpty();
                root.FillStackByIndex(stack, 0);
                do
                {
                    cancellation.ThrowIfCancellationRequested();
                    var nodeIdxPair = stack[stack.Count - 1];
                    var memberValue = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
                    var key = ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx);
                    var prefixLen = 0;
                    var minLen = Math.Min(prevKey.Length, key.Length);
                    for (int i = 0; i < minLen; i++)
                    {
                        if (prevKey[i] != key[i])
                        {
                            prefixLen = i;
                            break;
                        }
                    }
                    writer.WriteVUInt32((uint)prefixLen);
                    writer.WriteVUInt32((uint)(key.Length - prefixLen));
                    writer.WriteBlock(key.SubBuffer(prefixLen));
                    var vFileId = memberValue.ValueFileId;
                    if (vFileId > 0) usedFileIds.Add(vFileId);
                    writer.WriteVUInt32(vFileId);
                    writer.WriteVUInt32(memberValue.ValueOfs);
                    writer.WriteVInt32(memberValue.ValueSize);
                    prevKey = key;
                } while (root.FindNextKey(stack));
            }
            writer.FlushBuffer();
            file.HardFlush();
            writer.WriteInt32(EndOfIndexFileMarker);
            writer.FlushBuffer();
            file.HardFlush();
            file.Truncate();
            var trlGeneration = GetGeneration(keyIndex.TrLogFileId);
            keyIndex.UsedFilesInOlderGenerations = usedFileIds.Select(fi => GetGeneration(fi)).Where(gen => gen < trlGeneration).OrderBy(a => a).ToArray();
            FileCollection.SetInfo(file.Index, keyIndex);
            Logger?.KeyValueIndexCreated(file.Index, keyIndex.KeyValueCount, file.GetSize(), DateTime.UtcNow - start);
            return file.Index;
        }

        internal bool ContainsValuesAndDoesNotTouchGeneration(uint fileId, long dontTouchGeneration)
        {
            var info = FileCollection.FileInfoByIdx(fileId);
            if (info == null) return false;
            if (info.Generation >= dontTouchGeneration) return false;
            return info.FileType == KVFileType.TransactionLog || info.FileType == KVFileType.PureValues;
        }

        internal AbstractBufferedWriter StartPureValuesFile(out uint fileId)
        {
            var fId = FileCollection.AddFile("pvl");
            fileId = fId.Index;
            var pureValues = new FilePureValues(FileCollection.NextGeneration(), FileCollection.Guid);
            var writer = fId.GetAppenderWriter();
            FileCollection.SetInfo(fId.Index, pureValues);
            pureValues.WriteHeader(writer);
            return writer;
        }

        internal long ReplaceBTreeValues(CancellationToken cancellation, Dictionary<ulong, ulong> newPositionMap)
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
                    var newRoot = ((KeyValueDBTransaction) tr).BtreeRoot;
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

        internal void MarkAsUnknown(IEnumerable<uint> fileIds)
        {
            foreach (var fileId in fileIds)
            {
                _fileCollection.MakeIdxUnknown(fileId);
            }
        }

        internal long GetGeneration(uint fileId)
        {
            if (fileId == 0) return -1;
            var fileInfo = FileCollection.FileInfoByIdx(fileId);
            if (fileInfo == null)
            {
                throw new ArgumentOutOfRangeException(nameof(fileId));
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

        internal bool AreAllTransactionsBeforeFinished(long transactionId)
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
            var keyIndex = FileCollection.FileInfos.Where(p => p.Value.FileType == KVFileType.KeyIndex).Select(p => (IKeyIndex)p.Value).FirstOrDefault();
            if (keyIndex == null)
            {
                if (FileCollection.FileInfos.Count(p => p.Value.SubDBId == 0) > 1) return ulong.MaxValue;
                return root.TrLogOffset;
            }
            if (root.TrLogFileId != keyIndex.TrLogFileId) return ulong.MaxValue;
            return root.TrLogOffset - keyIndex.TrLogOffset;
        }

        public T GetSubDB<T>(long id) where T : class
        {
            object subDB;
            if (_subDBs.TryGetValue(id, out subDB))
            {
                if (!(subDB is T)) throw new ArgumentException($"SubDB of id {id} is not type {typeof(T).FullName}");
                return (T)subDB;
            }
            if (typeof(T) == typeof(IChunkStorage))
            {
                subDB = new ChunkStorageInKV(id, _fileCollection, MaxTrLogFileSize);
            }
            _subDBs.Add(id, subDB);
            return (T)subDB;
        }
    }
}
