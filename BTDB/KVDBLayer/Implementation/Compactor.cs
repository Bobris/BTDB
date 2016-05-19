using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BTDB.KVDBLayer.BTree;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class Compactor
    {
        readonly KeyValueDB _keyValueDB;
        IBTreeRootNode _root;
        FileStat[] _fileStats;
        Dictionary<ulong, uint> _newPositionMap;
        readonly CancellationToken _cancellation;

        struct FileStat
        {
            uint _valueLength;
            readonly uint _totalLength;

            internal FileStat(uint size)
            {
                _totalLength = size;
                _valueLength = 0;
            }

            internal void AddLength(uint length)
            {
                _valueLength += length;
            }

            internal uint CalcWaste()
            {
                if (_totalLength == 0) return 0;
                return _totalLength - _valueLength;
            }

            internal bool Useless()
            {
                return _totalLength != 0 && _valueLength == 0;
            }

            internal uint CalcUsed()
            {
                return _valueLength;
            }
        }

        internal Compactor(KeyValueDB keyValueDB, CancellationToken cancellation)
        {
            _keyValueDB = keyValueDB;
            _cancellation = cancellation;
        }

        internal void FastStartCleanUp()
        {
            if (_keyValueDB.FileCollection.GetCount() == 0) return;
            _root = _keyValueDB.LastCommited;
            var dontTouchGeneration = _keyValueDB.GetGeneration(_root.TrLogFileId);
            InitFileStats(dontTouchGeneration);
            CalculateFileUsefullness();
            var toRemoveFileIds = new List<uint>();
            for (var i = 0; i < _fileStats.Length; i++)
            {
                if (_fileStats[i].Useless()) toRemoveFileIds.Add((uint) i);
            }
            _keyValueDB.MarkAsUnknown(toRemoveFileIds);
        }

        internal bool Run()
        {
            if (_keyValueDB.FileCollection.GetCount() == 0) return false;
            _root = _keyValueDB.LastCommited;
            var dontTouchGeneration = _keyValueDB.GetGeneration(_root.TrLogFileId);
            InitFileStats(dontTouchGeneration);
            CalculateFileUsefullness();
            var totalWaste = CalcTotalWaste();
            if (totalWaste < (ulong)_keyValueDB.MaxTrLogFileSize / 4)
            {
                if (_keyValueDB.DistanceFromLastKeyIndex(_root) < (ulong)_keyValueDB.MaxTrLogFileSize / 4) return false;
                _keyValueDB.CreateIndexFile(_cancellation);
                _keyValueDB.FileCollection.DeleteAllUnknownFiles();
                return false;
            }
            _cancellation.ThrowIfCancellationRequested();
            uint valueFileId;
            var writer = _keyValueDB.StartPureValuesFile(out valueFileId);
            _newPositionMap = new Dictionary<ulong, uint>();
            var toRemoveFileIds = new List<uint>();
            while (true)
            {
                var wastefullFileId = FindMostWastefullFile(_keyValueDB.MaxTrLogFileSize - writer.GetCurrentPosition());
                if (wastefullFileId == 0) break;
                MoveValuesContent(writer, wastefullFileId);
                _fileStats[wastefullFileId] = new FileStat(0);
                toRemoveFileIds.Add(wastefullFileId);
            }
            var valueFile = _keyValueDB.FileCollection.GetFile(valueFileId);
            valueFile.HardFlush();
            valueFile.Truncate();
            var btreesCorrectInTransactionId = _keyValueDB.AtomicallyChangeBTree(root => root.RemappingIterate((uint oldFileId, uint oldOffset, out uint newFileId, out uint newOffset) =>
                {
                    newFileId = valueFileId;
                    _cancellation.ThrowIfCancellationRequested();
                    return _newPositionMap.TryGetValue(((ulong)oldFileId << 32) | oldOffset, out newOffset);
                }));
            _keyValueDB.CreateIndexFile(_cancellation);
            _keyValueDB.WaitForFinishingTransactionsBefore(btreesCorrectInTransactionId, _cancellation);
            if (_newPositionMap.Count == 0)
            {
                toRemoveFileIds.Add(valueFileId);
            }
            _keyValueDB.MarkAsUnknown(toRemoveFileIds);
            _keyValueDB.FileCollection.DeleteAllUnknownFiles();
            return true;
        }

        void MoveValuesContent(AbstractBufferedWriter writer, uint wastefullFileId)
        {
            const uint blockSize = 128 * 1024;
            var wasteFullStream = _keyValueDB.FileCollection.GetFile(wastefullFileId);
            var totalSize = wasteFullStream.GetSize();
            var blocks = (int)((totalSize + blockSize - 1) / blockSize);
            var wasteInMemory = new byte[blocks][];
            var pos = 0UL;
            for (var i = 0; i < blocks; i++)
            {
                _cancellation.ThrowIfCancellationRequested();
                wasteInMemory[i] = new byte[blockSize];
                var readSize = totalSize - pos;
                if (readSize > blockSize) readSize = blockSize;
                wasteFullStream.RandomRead(wasteInMemory[i], 0, (int)readSize, pos, true);
                pos += readSize;
            }
            _root.Iterate((valueFileId, valueOfs, valueSize) =>
                {
                    if (valueFileId != wastefullFileId) return;
                    var size = (uint)Math.Abs(valueSize);
                    _newPositionMap.Add(((ulong)wastefullFileId << 32) | valueOfs, (uint)writer.GetCurrentPosition());
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
                    }
                });
        }

        ulong CalcTotalWaste()
        {
            var total = 0ul;
            foreach (var fileStat in _fileStats)
            {
                var waste = fileStat.CalcWaste();
                if (waste > 1024) total += waste;
            }
            return total;
        }

        uint FindMostWastefullFile(long space)
        {
            if (space <= 0) return 0;
            var bestWaste = 0u;
            var bestFile = 0u;
            for (var index = 0u; index < _fileStats.Length; index++)
            {
                var waste = _fileStats[index].CalcWaste();
                if (waste <= bestWaste || space < _fileStats[index].CalcUsed()) continue;
                bestWaste = waste;
                bestFile = index;
            }
            return bestFile;
        }

        void InitFileStats(long dontTouchGeneration)
        {
            _fileStats = new FileStat[_keyValueDB.FileCollection.FileInfos.Max(f => f.Key) + 1];
            foreach (var file in _keyValueDB.FileCollection.FileInfos)
            {
                if (file.Key >= _fileStats.Length) continue;
                if (file.Value.SubDBId != 0) continue;
                if (!_keyValueDB.ContainsValuesAndDoesNotTouchGeneration(file.Key, dontTouchGeneration)) continue;
                _fileStats[file.Key] = new FileStat((uint)_keyValueDB.FileCollection.GetSize(file.Key));
            }
        }

        void CalculateFileUsefullness()
        {
            _root.Iterate((valueFileId, valueOfs, valueSize) =>
                {
                    var id = valueFileId;
                    var fileStats = _fileStats;
                    _cancellation.ThrowIfCancellationRequested();
                    if (id < fileStats.Length) fileStats[id].AddLength((uint)Math.Abs(valueSize));
                });
        }
    }
}