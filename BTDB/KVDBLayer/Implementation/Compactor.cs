using System;
using System.Collections.Generic;
using System.Threading;
using BTDB.Collections;
using BTDB.KVDBLayer.Implementation;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class Compactor
    {
        readonly IKeyValueDBInternal _keyValueDB;
        IRootNodeInternal _root;
        RefDictionary<uint, FileStat> _fileStats;

        Dictionary<ulong, ulong> _newPositionMap;
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

        void ForbidDeletePreservingHistory(long dontTouchGeneration, long[] usedFilesFromOldGenerations)
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
            List<uint> toRemoveFileIds = null;
            foreach (var fileStat in _fileStats.Index)
            {
                if (_fileStats.ValueRef(fileStat).Useless())
                {
                    if (toRemoveFileIds == null)
                        toRemoveFileIds = new List<uint>();
                    toRemoveFileIds.Add(_fileStats.KeyRef(fileStat));
                }
            }

            if (toRemoveFileIds != null)
                _keyValueDB.MarkAsUnknown(toRemoveFileIds);
        }

        internal bool Run()
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

                var lastCommited = _keyValueDB.ReferenceAndGetLastCommitted();
                try
                {
                    if (_root != lastCommited) ForbidDeleteOfFilesUsedByStillRunningOldTransaction();
                    ForbidDeletePreservingHistory(dontTouchGeneration, usedFilesFromOldGenerations);
                    CalculateFileUsefullness(lastCommited);
                }
                finally
                {
                    _keyValueDB.DereferenceRootNodeInternal(lastCommited);
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
                var toRemoveFileIds = new List<uint>();
                do
                {
                    _newPositionMap = new Dictionary<ulong, ulong>();
                    do
                    {
                        CompactOnePureValueFileIteration(toRemoveFileIds);
                        totalWaste = CalcTotalWaste();
                    } while (_newPositionMap.Count * 50 / (1024 * 1024) < _keyValueDB.CompactorRamLimitInMb &&
                             !IsWasteSmall(totalWaste));

                    btreesCorrectInTransactionId = _keyValueDB.ReplaceBTreeValues(_cancellation, _newPositionMap);
                } while (!IsWasteSmall(totalWaste));

                _keyValueDB.CreateIndexFile(_cancellation, preserveKeyIndexGeneration);
                if (_keyValueDB.AreAllTransactionsBeforeFinished(btreesCorrectInTransactionId))
                {
                    _keyValueDB.MarkAsUnknown(toRemoveFileIds);
                }

                _keyValueDB.FileCollection.DeleteAllUnknownFiles();
                return true;
            }
            finally
            {
                _keyValueDB.DereferenceRootNodeInternal(_root);
                _root = null;
            }
        }

        void CompactOnePureValueFileIteration(List<uint> toRemoveFileIds)
        {
            _cancellation.ThrowIfCancellationRequested();
            _writerBytesPerSecondLimiter = new BytesPerSecondLimiter(_keyValueDB.CompactorWriteBytesPerSecondLimit);
            var writer = _keyValueDB.StartPureValuesFile(out var valueFileId);
            var firstIteration = true;
            while (true)
            {
                var wastefullFileId =
                    FindMostWastefullFile(firstIteration ? uint.MaxValue : _keyValueDB.MaxTrLogFileSize - writer.GetCurrentPosition());
                firstIteration = false;
                if (wastefullFileId == 0) break;
                MoveValuesContent(writer, wastefullFileId, valueFileId);
                if (_fileStats.GetOrFakeValueRef(wastefullFileId).IsFreeToDelete())
                    toRemoveFileIds.Add(wastefullFileId);
                _fileStats.GetOrFakeValueRef(wastefullFileId) = new FileStat(0);
            }

            var valueFile = _keyValueDB.FileCollection.GetFile(valueFileId);
            valueFile.HardFlushTruncateSwitchToReadOnlyMode();
            _keyValueDB.Logger?.CompactionCreatedPureValueFile(valueFileId, valueFile.GetSize(),
                (uint)_newPositionMap.Count, 28 *
#if NETFRAMEWORK
                                              (ulong) _newPositionMap.Count
#else
                                              (ulong)_newPositionMap.EnsureCapacity(0)
#endif
            );
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

        void MoveValuesContent(AbstractBufferedWriter writer, uint wastefullFileId, uint pvlFileId)
        {
            const uint blockSize = 256 * 1024;
            var wasteFullStream = _keyValueDB.FileCollection.GetFile(wastefullFileId);
            var totalSize = wasteFullStream.GetSize();
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
                wasteFullStream.RandomRead(wasteInMemory[i].AsSpan(0, (int)readSize), pos, true);
                pos += readSize;
                readLimiter.Limit(pos);
            }

            _keyValueDB.IterateRoot(_root, (valueFileId, valueOfs, valueSize) =>
            {
                if (valueFileId != wastefullFileId) return;
                var size = (uint)Math.Abs(valueSize);
                _newPositionMap.Add(((ulong)wastefullFileId << 32) | valueOfs,
                    ((ulong)pvlFileId << 32) + (ulong)writer.GetCurrentPosition());
                pos = valueOfs;
                while (size > 0)
                {
                    _cancellation.ThrowIfCancellationRequested();
                    var blockId = pos / blockSize;
                    var blockStart = pos % blockSize;
                    var writeSize = (uint)(blockSize - blockStart);
                    if (writeSize > size) writeSize = size;
                    writer.WriteBlock(wasteInMemory[blockId], (int)blockStart, (int)writeSize);
                    size -= writeSize;
                    pos += writeSize;
                    _writerBytesPerSecondLimiter.Limit((ulong)writer.GetCurrentPosition());
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

        uint FindMostWastefullFile(long space)
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
            _fileStats = new RefDictionary<uint, FileStat>();
            foreach (var file in _keyValueDB.FileCollection.FileInfos)
            {
                if (file.Value.SubDBId != 0) continue;
                if (!_keyValueDB.ContainsValuesAndDoesNotTouchGeneration(file.Key, dontTouchGeneration))
                {
                    continue;
                }

                _fileStats.GetOrAddValueRef(file.Key) =
                    new FileStat((uint)_keyValueDB.FileCollection.GetSize(file.Key));
            }
        }

        void CalculateFileUsefullness(IRootNodeInternal root)
        {
            _keyValueDB.IterateRoot(root, (valueFileId, valueOfs, valueSize) =>
            {
                _cancellation.ThrowIfCancellationRequested();
                _fileStats.GetOrFakeValueRef(valueFileId).AddLength((uint)Math.Abs(valueSize));
            });
        }
    }
}