using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
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

    internal bool Run()
    {
        try
        {
            return RunCore();
        }
        catch (Exception e)
        {
            if (_keyValueDB.Logger?.ReportCompactorException(e) ?? true) throw;
        }

        return false;
    }

    bool RunCore()
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

            MarkTotallyUselessFilesAsUnknown();
            var totalWaste = CalcTotalWaste();
            _keyValueDB.Logger?.CompactionStart(totalWaste);
            if (IsWasteSmall(totalWaste))
            {
                if (_keyValueDB.DistanceFromLastKeyIndex(_root) > (ulong)(_keyValueDB.MaxTrLogFileSize / 4))
                    _keyValueDB.CreateIndexFile(_cancellation, preserveKeyIndexGeneration);
                _keyValueDB.FileCollection.DeleteAllUnknownFiles();
                return false;
            }

            long btreesCorrectInTransactionId;
            var toRemoveFileIds = new StructList<uint>();
            do
            {
                _newPositionMap = new();
                var pvlFileId = CompactOnePureValueFileIteration(ref toRemoveFileIds);
                btreesCorrectInTransactionId = _keyValueDB.ReplaceBTreeValues(_cancellation, _newPositionMap, pvlFileId);
                totalWaste = CalcTotalWaste();

            } while (!IsWasteSmall(totalWaste));

            var usedFileGens = _keyValueDB.CreateIndexFile(_cancellation, preserveKeyIndexGeneration);
            for (var i = (int)toRemoveFileIds.Count - 1; i >= 0; i--)
            {
                if (usedFileGens.IndexOf(_keyValueDB.FileCollection.FileInfoByIdx(toRemoveFileIds[i])?.Generation ??
                                         0) >= 0)
                {
                    _keyValueDB.Logger?.LogWarning("Disaster prevented by skipping delete of " +
                                                   toRemoveFileIds[i] + " file which is used by new kvi");
                    Debug.Fail("Should not get here");
                    toRemoveFileIds.RemoveAt(i);
                }
            }

            _keyValueDB.DereferenceRootNodeInternal(_root);
            _root = null;
            if (_keyValueDB.AreAllTransactionsBeforeFinished(btreesCorrectInTransactionId))
            {
                _keyValueDB.MarkAsUnknown(toRemoveFileIds);
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
                    : _keyValueDB.MaxTrLogFileSize - writer.GetCurrentPositionWithoutWriter());
            firstIteration = false;
            if (wastefulFileId == 0) break;
            MoveValuesContent(writer, wastefulFileId);
            if (_fileStats.GetOrFakeValueRef(wastefulFileId).IsFreeToDelete())
                toRemoveFileIds.Add(wastefulFileId);
            _fileStats.GetOrFakeValueRef(wastefulFileId) = new FileStat(0);
        }

        var valueFile = _keyValueDB.FileCollection.GetFile(valueFileId);
        valueFile!.HardFlushTruncateSwitchToReadOnlyMode();
        _keyValueDB.Logger?.CompactionCreatedPureValueFile(valueFileId, valueFile.GetSize(),
            (uint)_newPositionMap.Count, 20 * (ulong)_newPositionMap.Capacity
        );
        return valueFileId;
    }

    void ForbidDeleteOfFilesUsedByStillRunningOldTransaction()
    {
        _keyValueDB.IterateRoot(_root, (valueFileId, valueOfs, valueSize) =>
        {
            _cancellation.ThrowIfCancellationRequested();
            _fileStats.GetOrFakeValueRef(valueFileId).MarkForbidToDelete();
        });
    }

    bool IsWasteSmall(ulong totalWaste)
    {
        return totalWaste < (ulong)_keyValueDB.MaxTrLogFileSize / 4;
    }

    void MoveValuesContent(ISpanWriter writer, uint wastefulFileId)
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

        _keyValueDB.IterateRoot(_root, (valueFileId, valueOfs, valueSize) =>
        {
            if (valueFileId != wastefulFileId) return;
            var size = (uint)Math.Abs(valueSize);
            _newPositionMap.GetOrAddValueRef(((ulong)wastefulFileId << 32) | valueOfs) = (uint)writer.GetCurrentPositionWithoutWriter();
            pos = valueOfs;
            while (size > 0)
            {
                _cancellation.ThrowIfCancellationRequested();
                var blockId = pos / blockSize;
                var blockStart = pos % blockSize;
                var writeSize = (uint)(blockSize - blockStart);
                if (writeSize > size) writeSize = size;
                writer.WriteBlockWithoutWriter(
                    ref MemoryMarshal.GetReference(wasteInMemory[blockId].AsSpan((int)blockStart, (int)writeSize)),
                    writeSize);
                size -= writeSize;
                pos += writeSize;
                _writerBytesPerSecondLimiter.Limit((ulong)writer.GetCurrentPositionWithoutWriter());
            }
        });
    }

    ulong CalcTotalWaste()
    {
        var total = 0ul;
        foreach (var fileStat in _fileStats.Index)
        {
            var waste = _fileStats.ValueRef(fileStat).CalcWasteIgnoreUseless();
            if (waste > 1024) total += waste;
        }

        return total;
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
}
