using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Collections;
using BTDB.KVDBLayer.Implementation;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

class Compactor
{
    readonly IKeyValueDBInternal _keyValueDB;
    IRootNodeInternal? _root;
    RefDictionary<uint, FileStat> _fileStats;

    RefDictionary<ulong, uint> _newPositionMap;
    BytesPerSecondLimiter _writerBytesPerSecondLimiter;
    readonly CancellationToken _cancellation;

    struct FileStat
    {
        uint _valueLength;
        uint _totalLength;
        bool _forbidToDelete;

        internal FileStat(uint size)
        {
            _totalLength = size;
            _valueLength = 0;
            _forbidToDelete = false;
        }

        internal void AddLength(uint length)
        {
            _valueLength += length;
        }

        internal uint CalcWasteIgnoreUseless()
        {
            if (_totalLength == 0) return 0;
            if (_valueLength == 0) return 0;
            return _totalLength - _valueLength;
        }

        internal bool Useless()
        {
            return _totalLength != 0 && _valueLength == 0 && !_forbidToDelete;
        }

        internal uint CalcUsed()
        {
            return _valueLength;
        }

        internal void MarkForbidToDelete()
        {
            _forbidToDelete = true;
        }

        internal bool IsFreeToDelete()
        {
            return !_forbidToDelete;
        }
    }

    internal Compactor(IKeyValueDBInternal keyValueDB, CancellationToken cancellation)
    {
        _keyValueDB = keyValueDB;
        _cancellation = cancellation;
    }

    void ForbidDeletePreservingHistory(long dontTouchGeneration, long[]? usedFilesFromOldGenerations)
    {
        foreach (var fileStat in _fileStats.Index)
        {
            if (!_keyValueDB.ContainsValuesAndDoesNotTouchGeneration(_fileStats.KeyRef(fileStat),
                    dontTouchGeneration)
                || (usedFilesFromOldGenerations != null && Array.BinarySearch(usedFilesFromOldGenerations,
                    _keyValueDB.GetGeneration(_fileStats.KeyRef(fileStat))) >= 0))
                _fileStats.ValueRef(fileStat).MarkForbidToDelete();
        }
    }

    void MarkTotallyUselessFilesAsUnknown()
    {
        var toRemoveFileIds = new StructList<uint>();
        foreach (var fileStat in _fileStats.Index)
        {
            if (_fileStats.ValueRef(fileStat).Useless())
            {
                toRemoveFileIds.Add(_fileStats.KeyRef(fileStat));
            }
        }

        if (toRemoveFileIds.Count > 0) _keyValueDB.MarkAsUnknown(toRemoveFileIds);
    }

    internal async ValueTask<bool> Run()
    {
        try
        {
            return await RunCore();
        }
        catch (Exception e)
        {
            if (_keyValueDB.Logger?.ReportCompactorException(e) ?? true) throw;
        }

        return false;
    }

    async ValueTask<bool> RunCore()
    {
        if (_keyValueDB.FileCollection.GetCount() == 0) return false;
        _root = _keyValueDB.ReferenceAndGetOldestRoot();
        try
        {
            var dontTouchGeneration = _keyValueDB.GetGeneration(_keyValueDB.GetTrLogFileId(_root));
            var preserveKeyIndexKey =
                _keyValueDB.CalculatePreserveKeyIndexKeyFromKeyIndexInfos(_keyValueDB.BuildKeyIndexInfos());
            var preserveKeyIndexGeneration = _keyValueDB.CalculatePreserveKeyIndexGeneration(preserveKeyIndexKey);
            InitFileStats(dontTouchGeneration);
            long[] usedFilesFromOldGenerations = null;
            if (preserveKeyIndexKey < uint.MaxValue)
            {
                var dontTouchGenerationDueToPreserve = -1L;
                if (_keyValueDB.FileCollection.FileInfoByIdx(preserveKeyIndexKey) is IKeyIndex fileInfo)
                {
                    dontTouchGenerationDueToPreserve = fileInfo.Generation;
                    dontTouchGenerationDueToPreserve = Math.Min(dontTouchGenerationDueToPreserve,
                        _keyValueDB.GetGeneration(fileInfo.TrLogFileId));
                    if (fileInfo.UsedFilesInOlderGenerations == null)
                        _keyValueDB.LoadUsedFilesFromKeyIndex(preserveKeyIndexKey, fileInfo);
                    usedFilesFromOldGenerations = fileInfo.UsedFilesInOlderGenerations;
                }

                dontTouchGeneration = Math.Min(dontTouchGeneration, dontTouchGenerationDueToPreserve);
            }

            var lastCommitted = _keyValueDB.ReferenceAndGetLastCommitted();
            try
            {
                if (_root != lastCommitted) ForbidDeleteOfFilesUsedByStillRunningOldTransaction();
                ForbidDeletePreservingHistory(dontTouchGeneration, usedFilesFromOldGenerations);
                CalculateFileUsefulness(lastCommitted);
            }
            finally
            {
                _keyValueDB.DereferenceRootNodeInternal(lastCommitted);
            }

            {
                using var flushingTransaction = await _keyValueDB.StartWritingTransaction();
                flushingTransaction.NextCommitTemporaryCloseTransactionLog();
                flushingTransaction.Commit();
            }
            MarkTotallyUselessFilesAsUnknown();
            _keyValueDB.FileCollection.DeleteAllUnknownFiles();
            var (totalWaste, maxInOneFile) = CalcTotalWaste();
            _keyValueDB.Logger?.CompactionStart(totalWaste);
            if (IsWasteSmall(totalWaste, maxInOneFile))
            {
                if (_keyValueDB.DistanceFromLastKeyIndex(_root) > (ulong)(_keyValueDB.MaxTrLogFileSize / 4))
                    _keyValueDB.CreateIndexFile(_cancellation, preserveKeyIndexGeneration);
                _keyValueDB.FileCollection.DeleteAllUnknownFiles();
                return false;
            }

            long btreesCorrectInTransactionId;
            var toRemoveFileIds = new StructList<uint>();
            var pvlCreated = 0;
            do
            {
                _newPositionMap = new();
                var pvlFileId = CompactOnePureValueFileIteration(ref toRemoveFileIds);
                btreesCorrectInTransactionId =
                    await _keyValueDB.ReplaceBTreeValues(_cancellation, _newPositionMap, pvlFileId)
                        .ConfigureAwait(false);
                await Task.Delay(TimeSpan.FromMilliseconds(10), _cancellation).ConfigureAwait(false);
                pvlCreated++;
                (totalWaste, maxInOneFile) = CalcTotalWaste();
                if (pvlCreated >= 20)
                {
                    _keyValueDB.Logger?.LogWarning("Compactor didn't removed all waste (" + totalWaste +
                                                   "), because it created 20 PVL files already. Remaining waste left to next compaction.");
                    break;
                }
            } while (!IsWasteSmall(totalWaste, maxInOneFile));

            var usedFileGens = _keyValueDB.CreateIndexFile(_cancellation, preserveKeyIndexGeneration);
            for (var i = (int)toRemoveFileIds.Count - 1; i >= 0; i--)
            {
                if (usedFileGens.IndexOf(_keyValueDB.FileCollection.FileInfoByIdx(toRemoveFileIds[i])?.Generation ??
                                         0) >= 0)
                {
                    _keyValueDB.Logger?.LogWarning("Disaster prevented by skipping delete of " +
                                                   toRemoveFileIds[i] + " file which is used by new kvi");
                    toRemoveFileIds.RemoveAt(i);
                    Debug.Fail("Should not get here");
                }
            }

            _keyValueDB.DereferenceRootNodeInternal(_root);
            _root = null;
            if (_keyValueDB.AreAllTransactionsBeforeFinished(btreesCorrectInTransactionId))
            {
                _keyValueDB.MarkAsUnknown(toRemoveFileIds);
            }
            else
            {
                _keyValueDB.Logger?.LogWarning(
                    "Long running transaction detected, skipping deletion of still used files");
            }

            if (!_cancellation.IsCancellationRequested)
                _keyValueDB.FileCollection.DeleteAllUnknownFiles();
            return true;
        }
        finally
        {
            if (_root != null) _keyValueDB.DereferenceRootNodeInternal(_root);
            _root = null;
        }
    }

    uint CompactOnePureValueFileIteration(ref StructList<uint> toRemoveFileIds)
    {
        _cancellation.ThrowIfCancellationRequested();
        _writerBytesPerSecondLimiter = new(_keyValueDB.CompactorWriteBytesPerSecondLimit);
        var writer = _keyValueDB.StartPureValuesFile(out var valueFileId);
        var firstIteration = true;
        while (true)
        {
            var wastefulFileId =
                FindMostWastefulFile(firstIteration
                    ? uint.MaxValue
                    : _keyValueDB.MaxTrLogFileSize - writer.GetCurrentPosition());
            firstIteration = false;
            if (wastefulFileId == 0) break;
            MoveValuesContent(ref writer, wastefulFileId);
            if (_fileStats.GetOrFakeValueRef(wastefulFileId).IsFreeToDelete())
                toRemoveFileIds.Add(wastefulFileId);
            _fileStats.GetOrFakeValueRef(wastefulFileId) = new FileStat(0);
        }

        writer.Flush();
        var valueFile = _keyValueDB.FileCollection.GetFile(valueFileId);
        valueFile!.HardFlushTruncateSwitchToReadOnlyMode();
        _keyValueDB.Logger?.CompactionCreatedPureValueFile(valueFileId, valueFile.GetSize(),
            (uint)_newPositionMap.Count, 20 * (ulong)_newPositionMap.Capacity
        );
        return valueFileId;
    }

    void ForbidDeleteOfFilesUsedByStillRunningOldTransaction()
    {
        var usedSet = new HashSet<uint>();
        _keyValueDB.GatherUsedFiles(_cancellation, _root!, usedSet);
        foreach (var usedId in usedSet)
        {
            _fileStats.GetOrFakeValueRef(usedId).MarkForbidToDelete();
        }
    }

    bool IsWasteSmall(ulong totalWaste, ulong maxInOneFile)
    {
        return maxInOneFile < (ulong)_keyValueDB.MaxTrLogFileSize / 4 ||
               totalWaste < (ulong)_keyValueDB.MaxTrLogFileSize;
    }

    void MoveValuesContent(ref MemWriter writerIn, uint wastefulFileId)
    {
        const uint blockSize = 256 * 1024;
        var wastefulStream = _keyValueDB.FileCollection.GetFile(wastefulFileId);
        var totalSize = wastefulStream!.GetSize();
        var blocks = (int)((totalSize + blockSize - 1) / blockSize);
        var wasteInMemory = new byte[blocks][];
        var pos = 0UL;
        var readLimiter = new BytesPerSecondLimiter(_keyValueDB.CompactorReadBytesPerSecondLimit);
        for (var i = 0; i < blocks; i++)
        {
            _cancellation.ThrowIfCancellationRequested();
            wasteInMemory[i] = new byte[blockSize];
            var readSize = totalSize - pos;
            if (readSize > blockSize) readSize = blockSize;
            wastefulStream.RandomRead(wasteInMemory[i].AsSpan(0, (int)readSize), pos, true);
            pos += readSize;
            readLimiter.Limit(pos);
        }

        var writer = writerIn;
        _keyValueDB.IterateRoot(_root!, (valueFileId, valueOfs, valueSize) =>
        {
            if (valueFileId != wastefulFileId) return;
            var size = (uint)Math.Abs(valueSize);
            _newPositionMap.GetOrAddValueRef(((ulong)wastefulFileId << 32) | valueOfs) =
                (uint)writer.GetCurrentPosition();
            pos = valueOfs;
            while (size > 0)
            {
                _cancellation.ThrowIfCancellationRequested();
                var blockId = pos / blockSize;
                var blockStart = pos % blockSize;
                var writeSize = (uint)(blockSize - blockStart);
                if (writeSize > size) writeSize = size;
                writer.WriteBlock(
                    ref MemoryMarshal.GetReference(wasteInMemory[blockId].AsSpan((int)blockStart, (int)writeSize)),
                    writeSize);
                size -= writeSize;
                pos += writeSize;
                _writerBytesPerSecondLimiter.Limit((ulong)writer.GetCurrentPosition());
            }
        });
        writerIn = writer;
    }

    (ulong Total, ulong MaxInOneFile) CalcTotalWaste()
    {
        var total = 0ul;
        var maxInOneFile = 0ul;
        foreach (var fileStat in _fileStats.Index)
        {
            ref var stat = ref _fileStats.ValueRef(fileStat);
            var waste = stat.CalcWasteIgnoreUseless();
            if (waste > maxInOneFile) maxInOneFile = waste;
            if (waste > stat.CalcUsed() / 8) total += waste;
        }

        return (total, maxInOneFile);
    }

    uint FindMostWastefulFile(long space)
    {
        if (space <= 0) return 0;
        var bestWaste = 0u;
        var bestFile = 0u;
        foreach (var fileStat in _fileStats.Index)
        {
            var waste = _fileStats.ValueRef(fileStat).CalcWasteIgnoreUseless();
            if (waste <= bestWaste || space < _fileStats.ValueRef(fileStat).CalcUsed()) continue;
            bestWaste = waste;
            bestFile = _fileStats.KeyRef(fileStat);
        }

        return bestFile;
    }

    void InitFileStats(long dontTouchGeneration)
    {
        _fileStats = new();
        foreach (var (key, value) in _keyValueDB.FileCollection.FileInfos)
        {
            if (value.SubDBId != 0) continue;
            if (!_keyValueDB.ContainsValuesAndDoesNotTouchGeneration(key, dontTouchGeneration))
            {
                continue;
            }

            _fileStats.GetOrAddValueRef(key) =
                new FileStat((uint)_keyValueDB.FileCollection.GetSize(key));
        }
    }

    void CalculateFileUsefulness(IRootNodeInternal root)
    {
        _keyValueDB.IterateRoot(root, (valueFileId, valueOfs, valueSize) =>
        {
            _cancellation.ThrowIfCancellationRequested();
            _fileStats.GetOrFakeValueRef(valueFileId).AddLength((uint)Math.Abs(valueSize));
        });
    }

    public static uint CalculateIdealFileSplitSize(IFileCollectionWithFileInfos fc)
    {
        var totalSize = 0ul;
        foreach (var (key, value) in fc.FileInfos)
        {
            if (value.FileType is KVFileType.PureValues or KVFileType.PureValuesWithId or KVFileType.TransactionLog)
                totalSize += fc.GetSize(key);
        }

        // there should be ideally not much more than 6 splits
        // Minimum size of split is 32MB
        // Maximum size of split is 1GB
        var oneSplitIn32OfGiga = (uint)(totalSize / 6 / (1024ul * 1024 * 1024 / 32));
        if (oneSplitIn32OfGiga >= 32) oneSplitIn32OfGiga = 32;
        else if (oneSplitIn32OfGiga <= 1) oneSplitIn32OfGiga = 1;
        else oneSplitIn32OfGiga = NextPowerOfTwo(oneSplitIn32OfGiga);

        return oneSplitIn32OfGiga * (1024u * 1024 * 1024 / 32);

        static uint NextPowerOfTwo(uint input)
        {
            Debug.Assert(input >= 2);
            var leadingZeros = BitOperations.LeadingZeroCount(input - 1);
            Debug.Assert(leadingZeros != 0);
            return 1u << (32 - leadingZeros);
        }
    }
}
