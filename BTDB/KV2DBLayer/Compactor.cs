using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BTDB.KV2DBLayer.BTree;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class Compactor
    {
        readonly KeyValue2DB _keyValue2DB;
        IBTreeRootNode _root;
        FileStat[] _fileStats;
        Dictionary<ulong, uint> _newPositionMap;
        CancellationToken _cancellation;

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

        internal Compactor(KeyValue2DB keyValue2DB, CancellationToken cancellation)
        {
            _keyValue2DB = keyValue2DB;
            _cancellation = cancellation;
        }

        internal void FastStartCleanUp()
        {
            if (_keyValue2DB.FileCollection.GetCount() == 0) return;
            _root = _keyValue2DB.LastCommited;
            var dontTouchGeneration = _keyValue2DB.GetGeneration(_root.TrLogFileId);
            InitFileStats(dontTouchGeneration);
            CalculateFileUsefullness();
            var toRemoveFileIds = new List<uint>();
            for (var i = 0; i < _fileStats.Length; i++)
            {
                if (_fileStats[i].Useless()) toRemoveFileIds.Add((uint) i);
            }
            _keyValue2DB.MarkAsUnknown(toRemoveFileIds);
        }

        internal bool Run()
        {
            if (_keyValue2DB.FileCollection.GetCount() == 0) return false;
            _root = _keyValue2DB.LastCommited;
            var dontTouchGeneration = _keyValue2DB.GetGeneration(_root.TrLogFileId);
            InitFileStats(dontTouchGeneration);
            CalculateFileUsefullness();
            var totalWaste = CalcTotalWaste();
            if (totalWaste < (ulong)_keyValue2DB.MaxTrLogFileSize / 4)
            {
                if (_keyValue2DB.DistanceFromLastKeyIndex(_root) < (ulong)_keyValue2DB.MaxTrLogFileSize / 4) return false;
                _keyValue2DB.CreateIndexFile(_cancellation);
                _keyValue2DB.DeleteAllUnknownFiles();
                return false;
            }
            _cancellation.ThrowIfCancellationRequested();
            uint valueFileId;
            var writer = _keyValue2DB.StartPureValuesFile(out valueFileId);
            _newPositionMap = new Dictionary<ulong, uint>();
            var toRemoveFileIds = new List<uint>();
            while (true)
            {
                var wastefullFileId = FindMostWastefullFile(_keyValue2DB.MaxTrLogFileSize - writer.GetCurrentPosition());
                if (wastefullFileId == 0) break;
                MoveValuesContent(writer, wastefullFileId);
                _fileStats[wastefullFileId] = new FileStat(0);
                toRemoveFileIds.Add(wastefullFileId);
            }
            _keyValue2DB.FileCollection.GetFile(valueFileId).HardFlush();
            var btreesCorrectInTransactionId = _keyValue2DB.AtomicallyChangeBTree(root => root.RemappingIterate((uint oldFileId, uint oldOffset, out uint newFileId, out uint newOffset) =>
                {
                    newFileId = valueFileId;
                    _cancellation.ThrowIfCancellationRequested();
                    return _newPositionMap.TryGetValue(((ulong)oldFileId << 32) | oldOffset, out newOffset);
                }));
            _keyValue2DB.CreateIndexFile(_cancellation);
            _keyValue2DB.WaitForFinishingTransactionsBefore(btreesCorrectInTransactionId, _cancellation);
            if (_newPositionMap.Count == 0)
            {
                toRemoveFileIds.Add(valueFileId);
            }
            _keyValue2DB.MarkAsUnknown(toRemoveFileIds);
            _keyValue2DB.DeleteAllUnknownFiles();
            return true;
        }

        void MoveValuesContent(AbstractBufferedWriter writer, uint wastefullFileId)
        {
            const uint blockSize = 128 * 1024;
            var wasteFullStream = _keyValue2DB.FileCollection.GetFile(wastefullFileId);
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
                wasteFullStream.RandomRead(wasteInMemory[i], 0, (int)readSize, pos);
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
            _fileStats = new FileStat[_keyValue2DB.FileCollection.Enumerate().Max(f => f.Index) + 1];
            foreach (var file in _keyValue2DB.FileCollection.Enumerate())
            {
                if (file.Index >= _fileStats.Length) continue;
                if (!_keyValue2DB.ContainsValuesAndDoesNotTouchGeneration(file.Index, dontTouchGeneration)) continue;
                _fileStats[file.Index] = new FileStat((uint)file.GetSize());
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