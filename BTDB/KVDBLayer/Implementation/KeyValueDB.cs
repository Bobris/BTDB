using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer.BTree;
using BTDB.KVDBLayer.Implementation;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class KeyValueDB : IHaveSubDB, IKeyValueDBInternal
{
    const int MaxValueSizeInlineInMemory = 7;
    const int EndOfIndexFileMarker = 0x1234DEAD;
    IBTreeRootNode _lastCommited;

    long
        _preserveHistoryUpToCommitUlong; // it is long only because Interlock.Read is just long capable, MaxValue means no preserving history

    IBTreeRootNode? _nextRoot;
    KeyValueDBTransaction? _writingTransaction;

    readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new();

    readonly object _writeLock = new object();
    uint _fileIdWithTransactionLog;
    uint _fileIdWithPreviousTransactionLog;
    IFileCollectionFile? _fileWithTransactionLog;
    ISpanWriter? _writerWithTransactionLog;
    static readonly byte[] MagicStartOfTransaction = { (byte)'t', (byte)'R' };
    readonly ICompressionStrategy _compression;
    readonly IKviCompressionStrategy _kviCompressionStrategy;
    readonly ICompactorScheduler? _compactorScheduler;

    readonly HashSet<IBTreeRootNode> _usedBTreeRootNodes = new(ReferenceEqualityComparer<IBTreeRootNode>.Instance);

    readonly object _usedBTreeRootNodesLock = new();
    readonly IFileCollectionWithFileInfos _fileCollection;
    readonly Dictionary<long, object> _subDBs = new();
    readonly Func<CancellationToken, bool>? _compactFunc;
    readonly bool _readOnly;
    readonly bool _lenientOpen;
    uint? _missingSomeTrlFiles;
    public uint CompactorRamLimitInMb { get; set; }
    public ulong CompactorReadBytesPerSecondLimit { get; set; }
    public ulong CompactorWriteBytesPerSecondLimit { get; set; }

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
        ICompactorScheduler? compactorScheduler)
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
        if (options.FileSplitSize is < 1024 or > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(options.FileSplitSize), "Allowed range 1024 - 2G");
        Logger = options.Logger;
        _compactorScheduler = options.CompactorScheduler;
        MaxTrLogFileSize = options.FileSplitSize;
        _compression = options.Compression ?? throw new ArgumentNullException(nameof(options.Compression));
        _kviCompressionStrategy = options.KviCompressionStrategy;
        DurableTransactions = false;
        _fileCollection = new FileCollectionWithFileInfos(options.FileCollection);
        _readOnly = options.ReadOnly;
        _lenientOpen = options.LenientOpen;
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

    Span<KeyIndexInfo> IKeyValueDBInternal.BuildKeyIndexInfos()
    {
        var keyIndexes = new StructList<KeyIndexInfo>();
        foreach (var fileInfo in _fileCollection.FileInfos)
        {
            var keyIndex = fileInfo.Value as IKeyIndex;
            if (keyIndex == null) continue;
            keyIndexes.Add(new() { Key = fileInfo.Key, Generation = keyIndex.Generation, CommitUlong = keyIndex.CommitUlong });
        }

        if (keyIndexes.Count > 1)
            keyIndexes.Sort(Comparer<KeyIndexInfo>.Create((l, r) =>
                Comparer<long>.Default.Compare(l.Generation, r.Generation)));
        return keyIndexes.AsSpan();
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
        while (keyIndexes.Length > 0)
        {
            var nearKeyIndex = keyIndexes.Length - 1;
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
            keyIndexes[(nearKeyIndex + 1)..].CopyTo(keyIndexes[nearKeyIndex..]);
            keyIndexes = keyIndexes[..^1];
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
            MarkFileForRemoval(keyIndex.Key);
        }

        while (keyIndexes.Length > 0)
        {
            var keyIndex = keyIndexes[^1];
            keyIndexes = keyIndexes[..^1];
            if (keyIndex.Key != preserveKeyIndexKey)
                MarkFileForRemoval(keyIndex.Key);
        }

        if (!hasKeyIndex && _missingSomeTrlFiles.HasValue)
        {
            if (_lenientOpen)
            {
                Logger?.LogWarning("No valid Kvi and lowest Trl in chain is not first. Missing " +
                                   _missingSomeTrlFiles.Value + ". LenientOpen is true, recovering data.");
                LoadTransactionLogs(firstTrLogId, firstTrLogOffset, openUpToCommitUlong);
            }
            else
            {
                Logger?.LogWarning("No valid Kvi and lowest Trl in chain is not first. Missing " +
                                   _missingSomeTrlFiles.Value);
                if (!_readOnly)
                {
                    foreach (var fileInfo in _fileCollection.FileInfos)
                    {
                        if (fileInfo.Value is not IFileTransactionLog trLog) continue;
                        MarkFileForRemoval(fileInfo.Key);
                    }

                    _fileCollection.DeleteAllUnknownFiles();
                    _fileIdWithTransactionLog = 0;
                    firstTrLogId = 0;
                    lastTrLogFileId = 0;
                }
            }
        }
        else
        {
            LoadTransactionLogs(firstTrLogId, firstTrLogOffset, openUpToCommitUlong);
        }

        if (!_readOnly)
        {
            if (openUpToCommitUlong.HasValue || lastTrLogFileId != firstTrLogId && firstTrLogId != 0 ||
                !hasKeyIndex && _fileCollection.FileInfos.Any(p => p.Value.SubDBId == 0))
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

                if (_writerWithTransactionLog.GetCurrentPositionWithoutWriter() > MaxTrLogFileSize)
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

    uint IKeyValueDBInternal.CalculatePreserveKeyIndexKeyFromKeyIndexInfos(ReadOnlySpan<KeyIndexInfo> keyIndexes)
    {
        var preserveKeyIndexKey = uint.MaxValue;
        var preserveHistoryUpToCommitUlong = (ulong)Interlocked.Read(ref _preserveHistoryUpToCommitUlong);
        if (preserveHistoryUpToCommitUlong != ulong.MaxValue &&
            _lastCommited.CommitUlong != preserveHistoryUpToCommitUlong)
        {
            var nearKeyIndex = keyIndexes.Length - 1;
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

    long[] CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration,
        bool fullSpeed = false)
    {
        var idxFileId = CreateKeyIndexFile(_lastCommited, cancellation, fullSpeed);
        MarkAsUnknown(_fileCollection.FileInfos.Where(p =>
            p.Value.FileType == KVFileType.KeyIndex && p.Key != idxFileId &&
            p.Value.Generation != preserveKeyIndexGeneration).Select(p => p.Key));
        return ((FileKeyIndex)_fileCollection.FileInfoByIdx(idxFileId))!.UsedFilesInOlderGenerations!;
    }

    internal bool LoadUsedFilesFromKeyIndex(uint fileId, IKeyIndex info)
    {
        try
        {
            var file = FileCollection.GetFile(fileId);
            var readerController = file!.GetExclusiveReader();
            var reader = new SpanReader(readerController);
            FileKeyIndex.SkipHeader(ref reader);
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
                reader.Sync();
            }
            else
            {
                reader.Sync();
                var decompressionController = _kviCompressionStrategy.StartDecompression(info.Compression, readerController);
                try
                {
                    reader = new(decompressionController);
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
                    reader.Sync();
                }
                finally
                {
                    _kviCompressionStrategy.FinishDecompression(info.Compression, decompressionController);
                }
            }
            reader = new(readerController);

            var trlGeneration = GetGeneration(info.TrLogFileId);
            info.UsedFilesInOlderGenerations = usedFileIds.Select(GetGenerationIgnoreMissing)
                .Where(gen => gen > 0 && gen < trlGeneration).OrderBy(a => a).ToArray();
            return TestKviMagicEndMarker(fileId, ref reader, file);
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
            var file = FileCollection.GetFile(fileId);
            var readerController = file!.GetExclusiveReader();
            var reader = new SpanReader(readerController);
            FileKeyIndex.SkipHeader(ref reader);
            var keyCount = info.KeyValueCount;
            _nextRoot!.TrLogFileId = info.TrLogFileId;
            _nextRoot.TrLogOffset = info.TrLogOffset;
            _nextRoot.CommitUlong = info.CommitUlong;
            _nextRoot.UlongsArray = info.Ulongs;
            var usedFileIds = new HashSet<uint>();
            if (info.Compression == KeyIndexCompression.Old)
            {
                _nextRoot.BuildTree(keyCount, ref reader, (ref SpanReader reader2) =>
                {
                    var keyLength = reader2.ReadVInt32();
                    var key = ByteBuffer.NewAsync(new byte[Math.Abs(keyLength)]);
                    reader2.ReadBlock(key);
                    if (keyLength < 0)
                    {
                        _compression.DecompressKey(ref key);
                    }

                    var vFileId = reader2.ReadVUInt32();
                    if (vFileId > 0) usedFileIds.Add(vFileId);
                    return new BTreeLeafMember
                    {
                        Key = key.ToByteArray(),
                        ValueFileId = vFileId,
                        ValueOfs = reader2.ReadVUInt32(),
                        ValueSize = reader2.ReadVInt32()
                    };
                });
                reader.Sync();
            }
            else
            {
                reader.Sync();
                var decompressionController =
                    _kviCompressionStrategy.StartDecompression(info.Compression, readerController);
                try
                {
                    reader = new(decompressionController);
                    var prevKey = ByteBuffer.NewEmpty();
                    _nextRoot.BuildTree(keyCount, ref reader, (ref SpanReader reader2) =>
                    {
                        var prefixLen = (int)reader2.ReadVUInt32();
                        var keyLengthWithoutPrefix = (int)reader2.ReadVUInt32();
                        var key = ByteBuffer.NewAsync(new byte[prefixLen + keyLengthWithoutPrefix]);
                        Array.Copy(prevKey.Buffer!, prevKey.Offset, key.Buffer!, key.Offset, prefixLen);
                        reader2.ReadBlock(key.Slice(prefixLen));
                        prevKey = key;
                        var vFileId = reader2.ReadVUInt32();
                        if (vFileId > 0) usedFileIds.Add(vFileId);
                        return new BTreeLeafMember
                        {
                            Key = key.ToByteArray(),
                            ValueFileId = vFileId,
                            ValueOfs = reader2.ReadVUInt32(),
                            ValueSize = reader2.ReadVInt32()
                        };
                    });
                    reader.Sync();
                }
                finally
                {
                    _kviCompressionStrategy.FinishDecompression(info.Compression, decompressionController);
                }
            }
            reader = new(readerController);

            var trlGeneration = GetGeneration(info.TrLogFileId);
            info.UsedFilesInOlderGenerations = usedFileIds.Select(GetGenerationIgnoreMissing)
                .Where(gen => gen > 0 && gen < trlGeneration).OrderBy(a => a).ToArray();

            return TestKviMagicEndMarker(fileId, ref reader, file);
        }
        catch (Exception)
        {
            return false;
        }
    }

    bool TestKviMagicEndMarker(uint fileId, ref SpanReader reader, IFileCollectionFile file)
    {
        if (reader.Eof) return true;
        if ((ulong)reader.GetCurrentPosition() + 4 == file.GetSize() &&
            reader.ReadInt32() == EndOfIndexFileMarker) return true;
        if (_lenientOpen)
        {
            Logger?.LogWarning("End of Kvi " + fileId + " had some garbage at " +
                               (reader.GetCurrentPosition() - 4) +
                               " ignoring that because of LenientOpen");
            return true;
        }

        return false;
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
        var reader = new SpanReader(collectionFile!.GetExclusiveReader());
        try
        {
            if (logOffset == 0)
            {
                FileTransactionLog.SkipHeader(ref reader);
            }
            else
            {
                reader.SetCurrentPosition(logOffset);
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

                            if (valueLen <= MaxValueSizeInlineInMemory &&
                                (command & KVCommandType.SecondParamCompressed) == 0)
                            {
                                var ctx = new CreateOrUpdateCtx
                                {
                                    Key = keyBuf.AsSyncReadOnlySpan()
                                };
                                reader.ReadBlock(inlineValueBuf, 0, valueLen);
                                StoreValueInlineInMemory(inlineValueBuf.AsSpan(0, valueLen),
                                    out ctx.ValueOfs, out ctx.ValueSize);
                                ctx.ValueFileId = 0;
                                _nextRoot.CreateOrUpdate(ref ctx);
                            }
                            else
                            {
                                var ctx = new CreateOrUpdateCtx
                                {
                                    Key = keyBuf.AsSyncReadOnlySpan(),
                                    ValueFileId = fileId,
                                    ValueOfs = (uint)reader.GetCurrentPosition(),
                                    ValueSize = (command & KVCommandType.SecondParamCompressed) != 0
                                        ? -valueLen
                                        : valueLen
                                };
                                reader.SkipBlock(valueLen);
                                _nextRoot.CreateOrUpdate(ref ctx);
                            }
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

                            var findResult = _nextRoot.FindKey(stack, out var keyIndex, keyBuf.AsSyncReadOnlySpan(), 0);
                            if (findResult == FindResult.Exact)
                                _nextRoot.EraseOne(keyIndex);
                            else if (!_lenientOpen)
                            {
                                _nextRoot = null;
                                return false;
                            }
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

                            var findResult =
                                _nextRoot.FindKey(stack, out var keyIndex1, keyBuf.AsSyncReadOnlySpan(), 0);
                            if (findResult != FindResult.Exact && !_lenientOpen)
                            {
                                _nextRoot = null;
                                return false;
                            }

                            if (findResult == FindResult.Previous) keyIndex1++;
                            key = new byte[keyLen2];
                            reader.ReadBlock(key);
                            keyBuf = ByteBuffer.NewAsync(key);
                            if ((command & KVCommandType.SecondParamCompressed) != 0)
                            {
                                _compression.DecompressKey(ref keyBuf);
                            }

                            findResult = _nextRoot.FindKey(stack, out var keyIndex2, keyBuf.AsSyncReadOnlySpan(), 0);
                            if (findResult != FindResult.Exact && !_lenientOpen)
                            {
                                _nextRoot = null;
                                return false;
                            }

                            if (findResult == FindResult.Next) keyIndex2--;
                            if (keyIndex1 > keyIndex2)
                            {
                                if (!_lenientOpen)
                                {
                                    _nextRoot = null;
                                    return false;
                                }
                            }
                            else
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
                        if (_nextRoot != null && !_lenientOpen)
                        {
                            _nextRoot = null;
                            return false;
                        }

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

    static void StoreValueInlineInMemory(in ReadOnlySpan<byte> value, out uint valueOfs, out int valueSize)
    {
        var valueLen = value.Length;
        switch (valueLen)
        {
            case 0:
                valueOfs = 0;
                valueSize = 0;
                break;
            case 1:
                valueOfs = 0;
                valueSize = 0x1000000 | (value[0] << 16);
                break;
            case 2:
                valueOfs = 0;
                valueSize = 0x2000000 | (value[0] << 16) | (value[1] << 8);
                break;
            case 3:
                valueOfs = 0;
                valueSize = 0x3000000 | (value[0] << 16) | (value[1] << 8) | value[2];
                break;
            case 4:
                valueOfs = value[3];
                valueSize = 0x4000000 | (value[0] << 16) | (value[1] << 8) | value[2];
                break;
            case 5:
                valueOfs = value[3] | ((uint)value[4] << 8);
                valueSize = 0x5000000 | (value[0] << 16) | (value[1] << 8) | value[2];
                break;
            case 6:
                valueOfs = value[3] | ((uint)value[4] << 8) | ((uint)value[5] << 16);
                valueSize = 0x6000000 | (value[0] << 16) | (value[1] << 8) | value[2];
                break;
            case 7:
                valueOfs = value[3] | ((uint)value[4] << 8) | ((uint)value[5] << 16) | (((uint)value[6]) << 24);
                valueSize = 0x7000000 | (value[0] << 16) | (value[1] << 8) | value[2];
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
            if (fileTransactionLog == null)
            {
                _missingSomeTrlFiles = currentId;
                break;
            }

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
            var writer = new SpanWriter(_writerWithTransactionLog);
            writer.WriteUInt8((byte)KVCommandType.TemporaryEndOfFile);
            writer.Sync();
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

    readonly ConditionalWeakTable<IKeyValueDBTransaction, object?> _transactions =
        new ConditionalWeakTable<IKeyValueDBTransaction, object?>();

    public long MaxTrLogFileSize { get; set; }

    public IEnumerable<IKeyValueDBTransaction> Transactions()
    {
        foreach (var keyValuePair in _transactions)
        {
            yield return keyValuePair.Key;
        }
    }

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
        var tr = new KeyValueDBTransaction(this, _lastCommited, false, false);
        _transactions.Add(tr, null);
        return tr;
    }

    public IKeyValueDBTransaction StartReadOnlyTransaction()
    {
        var tr = new KeyValueDBTransaction(this, _lastCommited, false, true);
        _transactions.Add(tr, null);
        return tr;
    }

    public ValueTask<IKeyValueDBTransaction> StartWritingTransaction()
    {
        lock (_writeLock)
        {
            if (_writingTransaction == null)
            {
                var tr = NewWritingTransactionUnsafe();
                _transactions.Add(tr, null);
                return new ValueTask<IKeyValueDBTransaction>(tr);
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
            sb.Append(
                $"LastTrId:{lastCommitted.TransactionId},TRL:{lastCommitted.TrLogFileId},ComUlong:{lastCommitted.CommitUlong}\n");
            sb.Append(
                $"OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong}\n");
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

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats()
    {
        return (0, 0, 0, 0);
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
        var writer = new SpanWriter(_writerWithTransactionLog!);
        WriteUlongsDiff(ref writer, btreeRoot.UlongsArray, _lastCommited.UlongsArray);
        var deltaUlong = unchecked(btreeRoot.CommitUlong - _lastCommited.CommitUlong);
        if (deltaUlong != 0)
        {
            writer.WriteUInt8((byte)KVCommandType.CommitWithDeltaUlong);
            writer.WriteVUInt64(deltaUlong);
        }
        else
        {
            writer.WriteUInt8((byte)KVCommandType.Commit);
        }

        writer.Sync();
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
            writer = new SpanWriter(_writerWithTransactionLog!);
            writer.WriteUInt8((byte)KVCommandType.TemporaryEndOfFile);
            writer.Sync();
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

    void WriteUlongsDiff(ref SpanWriter writer, ulong[]? newArray, ulong[]? oldArray)
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
                writer.WriteUInt8((byte)KVCommandType.DeltaUlongs);
                writer.WriteVUInt32((uint)i);
                writer.WriteVUInt64(deltaUlong);
            }
        }
    }

    void UpdateTransactionLogInBTreeRoot(IBTreeRootNode btreeRoot)
    {
        // Create new KVI file if new trl file was created, if preserve history is used it this is co
        if (btreeRoot.TrLogFileId != _fileIdWithTransactionLog && btreeRoot.TrLogFileId != 0 &&
            !PreserveHistoryUpToCommitUlong.HasValue)
        {
            _compactorScheduler?.AdviceRunning(false);
        }

        btreeRoot.TrLogFileId = _fileIdWithTransactionLog;
        if (_writerWithTransactionLog != null)
        {
            btreeRoot.TrLogOffset = (uint)_writerWithTransactionLog.GetCurrentPositionWithoutWriter();
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
        var tr = NewWritingTransactionUnsafe();
        _transactions.Add(tr, null);
        tcs.SetResult(tr);
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
            var writer = new SpanWriter(_writerWithTransactionLog!);
            writer.WriteUInt8((byte)KVCommandType.Rollback);
            writer.Sync();
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

            if (_writerWithTransactionLog.GetCurrentPositionWithoutWriter() > MaxTrLogFileSize)
            {
                WriteStartOfNewTransactionLogFile();
            }
        }

        var writer = new SpanWriter(_writerWithTransactionLog!);
        writer.WriteUInt8((byte)KVCommandType.TransactionStart);
        writer.WriteByteArrayRaw(MagicStartOfTransaction);
        writer.Sync();
    }

    void WriteStartOfNewTransactionLogFile()
    {
        SpanWriter writer;
        if (_writerWithTransactionLog != null)
        {
            writer = new SpanWriter(_writerWithTransactionLog);
            writer.WriteUInt8((byte)KVCommandType.EndOfFile);
            writer.Sync();
            _fileWithTransactionLog!.HardFlushTruncateSwitchToReadOnlyMode();
            _fileIdWithPreviousTransactionLog = _fileIdWithTransactionLog;
        }

        _fileWithTransactionLog = FileCollection.AddFile("trl");
        Logger?.TransactionLogCreated(_fileWithTransactionLog.Index);
        _fileIdWithTransactionLog = _fileWithTransactionLog.Index;
        var transactionLog = new FileTransactionLog(FileCollection.NextGeneration(), FileCollection.Guid,
            _fileIdWithPreviousTransactionLog);
        _writerWithTransactionLog = _fileWithTransactionLog.GetAppenderWriter();
        writer = new SpanWriter(_writerWithTransactionLog);
        transactionLog.WriteHeader(ref writer);
        writer.Sync();
        FileCollection.SetInfo(_fileIdWithTransactionLog, transactionLog);
    }

    public void WriteCreateOrUpdateCommand(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value,
        out uint valueFileId,
        out uint valueOfs, out int valueSize)
    {
        valueSize = value.Length;

        var trlPos = _writerWithTransactionLog!.GetCurrentPositionWithoutWriter();
        if (trlPos > 256 && trlPos + key.Length + 16 + value.Length > MaxTrLogFileSize)
        {
            WriteStartOfNewTransactionLogFile();
        }

        var writer = new SpanWriter(_writerWithTransactionLog!);
        writer.WriteUInt8((byte)KVCommandType.CreateOrUpdate);
        writer.WriteVInt32(key.Length);
        writer.WriteVInt32(value.Length);
        writer.WriteBlock(key);
        if (valueSize != 0)
        {
            if (valueSize > 0 && valueSize <= MaxValueSizeInlineInMemory)
            {
                StoreValueInlineInMemory(value, out valueOfs, out valueSize);
                valueFileId = 0;
            }
            else
            {
                valueFileId = _fileIdWithTransactionLog;
                valueOfs = (uint)writer.GetCurrentPosition();
            }

            writer.WriteBlock(value);
        }
        else
        {
            valueFileId = 0;
            valueOfs = 0;
        }

        writer.Sync();
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

    public ReadOnlyMemory<byte> ReadValueAsMemory(uint valueFileId, uint valueOfs, int valueSize)
    {
        if (valueSize == 0) return new();
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

            return buf;
        }

        var compressed = false;
        if (valueSize < 0)
        {
            compressed = true;
            valueSize = -valueSize;
        }

        var result = new byte[valueSize];
        var file = FileCollection.GetFile(valueFileId);
        if (file == null)
            throw new BTDBException(
                $"ReadValue({valueFileId},{valueOfs},{valueSize}) compressed: {compressed} file does not exist in {CalcStats()}");
        file.RandomRead(result, valueOfs, false);
        if (compressed)
            result = _compression.DecompressValue(result);
        return result;
    }

    public ReadOnlySpan<byte> ReadValue(uint valueFileId, uint valueOfs, int valueSize, ref byte buffer,
        int bufferLength)
    {
        if (valueSize == 0) return ReadOnlySpan<byte>.Empty;
        if (valueFileId == 0)
        {
            var len = valueSize >> 24;
            var buf = len > bufferLength ? new byte[len] : MemoryMarshal.CreateSpan(ref buffer, len);
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

            return buf;
        }

        var compressed = false;
        if (valueSize < 0)
        {
            compressed = true;
            valueSize = -valueSize;
        }

        var result = valueSize > bufferLength
            ? new byte[valueSize]
            : MemoryMarshal.CreateSpan(ref buffer, valueSize);
        var file = FileCollection.GetFile(valueFileId);
        if (file == null)
            throw new BTDBException(
                $"ReadValue({valueFileId},{valueOfs},{valueSize}) compressed: {compressed} file does not exist in {CalcStats()}");
        file.RandomRead(result, valueOfs, false);
        if (compressed)
            result = _compression.DecompressValue(result);
        return result;
    }

    public void WriteEraseOneCommand(in ReadOnlySpan<byte> key)
    {
        if (_writerWithTransactionLog!.GetCurrentPositionWithoutWriter() > MaxTrLogFileSize)
        {
            WriteStartOfNewTransactionLogFile();
        }

        var writer = new SpanWriter(_writerWithTransactionLog!);
        writer.WriteUInt8((byte)KVCommandType.EraseOne);
        writer.WriteVInt32(key.Length);
        writer.WriteBlock(key);
        writer.Sync();
    }

    public void WriteEraseRangeCommand(in ReadOnlySpan<byte> firstKey, in ReadOnlySpan<byte> secondKey)
    {
        if (_writerWithTransactionLog!.GetCurrentPositionWithoutWriter() > MaxTrLogFileSize)
        {
            WriteStartOfNewTransactionLogFile();
        }

        var writer = new SpanWriter(_writerWithTransactionLog!);
        writer.WriteUInt8((byte)KVCommandType.EraseRange);
        writer.WriteVInt32(firstKey.Length);
        writer.WriteVInt32(secondKey.Length);
        writer.WriteBlock(firstKey);
        writer.WriteBlock(secondKey);
        writer.Sync();
    }

    uint CreateKeyIndexFile(IBTreeRootNode root, CancellationToken cancellation, bool fullSpeed)
    {
        var bytesPerSecondLimiter = new BytesPerSecondLimiter(fullSpeed ? 0 : CompactorWriteBytesPerSecondLimit);
        var file = FileCollection.AddFile("kvi");
        var writerController = file.GetExclusiveAppenderWriter();
        var writer = new SpanWriter(writerController);
        var keyCount = root.CalcKeyCount();
        if (root.TrLogFileId != 0)
            FileCollection.ConcurentTemporaryTruncate(root.TrLogFileId, root.TrLogOffset);
        var (compressionType, compressionController) =
            _kviCompressionStrategy.StartCompression((ulong)keyCount, writerController);
        var keyIndex = new FileKeyIndex(FileCollection.NextGeneration(), FileCollection.Guid, root.TrLogFileId,
            root.TrLogOffset, keyCount, root.CommitUlong, compressionType, root.UlongsArray);
        keyIndex.WriteHeader(ref writer);
        writer.Sync();
        ulong originalSize;
        var usedFileIds = new HashSet<uint>();
        try
        {
            writer = new(compressionController);
            if (keyCount > 0)
            {
                var stack = new List<NodeIdxPair>();
                var prevKey = new ReadOnlySpan<byte>();
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
                    writer.WriteBlock(key[prefixLen..]);
                    var vFileId = memberValue.ValueFileId;
                    if (vFileId > 0) usedFileIds.Add(vFileId);
                    writer.WriteVUInt32(vFileId);
                    writer.WriteVUInt32(memberValue.ValueOfs);
                    writer.WriteVInt32(memberValue.ValueSize);
                    prevKey = key;
                    bytesPerSecondLimiter.Limit((ulong)writer.GetCurrentPosition());
                } while (root.FindNextKey(stack));
            }

            writer.Sync();
        }
        finally
        {
            originalSize = (ulong)compressionController.GetCurrentPositionWithoutWriter();
            _kviCompressionStrategy.FinishCompression(compressionType, compressionController);
        }
        file.HardFlush();
        writer = new(writerController);
        writer.WriteInt32(EndOfIndexFileMarker);
        writer.Sync();
        file.HardFlushTruncateSwitchToDisposedMode();
        var trlGeneration = GetGeneration(keyIndex.TrLogFileId);
        keyIndex.UsedFilesInOlderGenerations = usedFileIds.Select(GetGenerationIgnoreMissing)
            .Where(gen => gen < trlGeneration).OrderBy(a => a).ToArray();
        FileCollection.SetInfo(file.Index, keyIndex);
        Logger?.KeyValueIndexCreated(file.Index, keyIndex.KeyValueCount, file.GetSize(),
            TimeSpan.FromMilliseconds(bytesPerSecondLimiter.TotalTimeInMs), originalSize);
        return file.Index;
    }

    internal bool ContainsValuesAndDoesNotTouchGeneration(uint fileId, long dontTouchGeneration)
    {
        var info = FileCollection.FileInfoByIdx(fileId);
        if (info == null) return false;
        if (info.Generation >= dontTouchGeneration) return false;
        return info.FileType is KVFileType.TransactionLog or KVFileType.PureValues;
    }

    long[] IKeyValueDBInternal.CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration)
    {
        return CreateIndexFile(cancellation, preserveKeyIndexGeneration);
    }

    ISpanWriter IKeyValueDBInternal.StartPureValuesFile(out uint fileId)
    {
        var fId = FileCollection.AddFile("pvl");
        fileId = fId.Index;
        var pureValues = new FilePureValues(FileCollection.NextGeneration(), FileCollection.Guid);
        var writerController = fId.GetExclusiveAppenderWriter();
        FileCollection.SetInfo(fId.Index, pureValues);
        var writer = new SpanWriter(writerController);
        pureValues.WriteHeader(ref writer);
        writer.Sync();
        return writerController;
    }

    public long ReplaceBTreeValues(CancellationToken cancellation, RefDictionary<ulong, uint> newPositionMap, uint targetFileId)
    {
        var ctx = new ReplaceValuesCtx
        {
            _cancellation = cancellation,
            _newPositionMap = newPositionMap,
            _targetFileId = targetFileId
        };
        while (true)
        {
            ctx._iterationTimeOut = DateTime.UtcNow + TimeSpan.FromMilliseconds(50);
            ctx._interrupt = false;
            using (var tr = StartWritingTransaction().Result)
            {
                var newRoot = ((KeyValueDBTransaction)tr).BtreeRoot;
                newRoot!.ReplaceValues(ctx);
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
            MarkFileForRemoval(fileId);
        }
    }

    void MarkFileForRemoval(uint fileId)
    {
        var file = _fileCollection.GetFile(fileId);
        if (file != null)
            Logger?.FileMarkedForDelete(file.Index);
        else
            Logger?.LogWarning($"Marking for delete file id {fileId} unknown in file collection.");
        _fileCollection.MakeIdxUnknown(fileId);
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
            subDB = new ChunkStorageInKV(id, _fileCollection, MaxTrLogFileSize);
        }

        _subDBs.Add(id, subDB);
        return (T)subDB;
    }

    public void DereferenceRootNodeInternal(IRootNodeInternal root)
    {
        // Managed implementation does not need reference counting => nothing to do
    }

    public bool IsCorruptedValue(uint valueFileId, uint valueOfs, int valueSize)
    {
        if (valueSize == 0) return false;
        if (valueFileId == 0)
        {
            var len = valueSize >> 24;
            switch (len)
            {
                case 7:
                case 6:
                case 5:
                case 4:
                case 3:
                case 2:
                case 1:
                    return false;
                default:
                    return true;
            }
        }

        if (valueSize < 0)
        {
            valueSize = -valueSize;
        }

        var file = FileCollection.GetFile(valueFileId);
        if (file == null)
            return true;
        if (valueOfs + (ulong)valueSize > file.GetSize())
            return true;
        return false;
    }
}
