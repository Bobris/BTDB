using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KV2DBLayer.BTree;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class Compactor
    {
        readonly KeyValue2DB _keyValue2DB;
        IBTreeRootNode _root;
        FileStat[] _fileStats;
        Dictionary<ulong, uint> _newPositionMap;

        struct FileStat
        {
            uint _valueLength;
            readonly uint _totalLength;

            internal FileStat(uint size)
            {
                _totalLength = size;
                _valueLength = 0;
            }

            internal void Increment(uint length)
            {
                _valueLength += length;
            }

            internal uint CalcWaste()
            {
                if (_totalLength == 0) return 0;
                return _totalLength - _valueLength;
            }

            internal uint CalcUsed()
            {
                return _valueLength;
            }
        }

        public Compactor(KeyValue2DB keyValue2DB)
        {
            _keyValue2DB = keyValue2DB;
        }

        public void Run()
        {
            if (_keyValue2DB.FileCollection.GetCount() == 0) return;
            _root = _keyValue2DB.LastCommited;
            var dontTouchGeneration = _keyValue2DB.GetGeneration(_root.TrLogFileId);
            InitFileStats(dontTouchGeneration);
            CalculateFileUsefullness();
            var totalWaste = CalcTotalWaste();
            if (totalWaste < (ulong)_keyValue2DB.MaxTrLogFileSize / 8) return;
            uint valueFileId;
            var writer = _keyValue2DB.StartPureValuesFile(out valueFileId);
            _newPositionMap = new Dictionary<ulong, uint>();
            var removedFileIds = new List<uint>();
            while (true)
            {
                var wastefullFileId = FindMostWastefullFile(_keyValue2DB.MaxTrLogFileSize - writer.GetCurrentPosition());
                if (wastefullFileId == 0) break;
                MoveValuesContent(writer, wastefullFileId);
                _fileStats[wastefullFileId] = new FileStat(0);
                removedFileIds.Add(wastefullFileId);
            }
            ((IDisposable)writer).Dispose();
            _keyValue2DB.AtomicallyChangeBTree(root => root.RemappingIterate((ref BTreeLeafMember m, out uint newFileId, out uint newOffset) =>
                {
                    newFileId = valueFileId;
                    return _newPositionMap.TryGetValue(((ulong)m.ValueFileId << 32) | m.ValueOfs, out newOffset);
                }));
            _keyValue2DB.CreateIndexFile();
            _keyValue2DB.MarkAsUnknown(removedFileIds);
            _keyValue2DB.DeleteAllUnknownFiles();
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
                wasteInMemory[i] = new byte[blockSize];
                var readSize = totalSize - pos;
                if (readSize > blockSize) readSize = blockSize;
                if (wasteFullStream.Read(wasteInMemory[i], 0, (int)readSize, pos) != (int)readSize)
                    throw new BTDBException("Corrupted DB");
                pos += readSize;
            }
            _root.Iterate((ref BTreeLeafMember m) =>
                {
                    if (m.ValueFileId == wastefullFileId)
                    {
                        var size = (uint)Math.Abs(m.ValueSize);
                        _newPositionMap.Add(((ulong)wastefullFileId << 32) | m.ValueOfs, (uint)writer.GetCurrentPosition());
                        pos = m.ValueOfs;
                        while (size > 0)
                        {
                            var blockId = pos/blockSize;
                            var blockStart = pos % blockSize;
                            var writeSize = (uint)(blockSize - blockStart);
                            if (writeSize > size) writeSize = size;
                            writer.WriteBlock(wasteInMemory[blockId], (int) blockStart, (int)writeSize);
                            size -= writeSize;
                            pos += writeSize;
                        }
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
            _fileStats = new FileStat[_keyValue2DB.FileCollection.Enumerate().Max()];
            foreach (var id in _keyValue2DB.FileCollection.Enumerate())
            {
                if (id >= _fileStats.Length) continue;
                if (!_keyValue2DB.ContainsValuesAndDoesNotTouchGneration(id, dontTouchGeneration)) continue;
                _fileStats[id] = new FileStat((uint)_keyValue2DB.FileCollection.GetFile(id).GetSize());
            }
        }

        void CalculateFileUsefullness()
        {
            _root.Iterate((ref BTreeLeafMember m) => _fileStats[m.ValueFileId].Increment((uint)Math.Abs(m.ValueSize)));
        }
    }
}