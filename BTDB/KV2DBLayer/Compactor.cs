using System;
using System.Linq;
using BTDB.KV2DBLayer.BTree;

namespace BTDB.KV2DBLayer
{
    internal class Compactor
    {
        readonly KeyValue2DB _keyValue2DB;
        IBTreeRootNode _root;
        FileStat[] _fileStats;

        struct FileStat
        {
            uint _valueCount;
            uint _valueLength;
            readonly uint _totalLength;

            internal FileStat(uint size)
            {
                _totalLength = size;
                _valueCount = 0;
                _valueLength = 0;
            }

            internal void Increment(uint length)
            {
                _valueCount++;
                _valueLength += length;
            }

            internal uint CalcWaste()
            {
                if (_totalLength == 0) return 0;
                return _totalLength - _valueLength;
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
            InitFileStats();
            CalculateFileUsefullness();
            var totalWaste = CalcTotalWaste();
            if (totalWaste < 10240) return;
            uint valueFileId;
            var writer = _keyValue2DB.StartPureValuesFile(out valueFileId);
            var wastefullFileId = FindMostWastefullFile();
            while (wastefullFileId != 0)
            {

            }
            ((IDisposable)writer).Dispose();
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

        uint FindMostWastefullFile()
        {
            var bestWaste = 0u;
            var bestFile = 0u;
            for (var index = 0u; index < _fileStats.Length; index++)
            {
                var waste = _fileStats[index].CalcWaste();
                if (waste <= bestWaste) continue;
                bestWaste = waste;
                bestFile = index;
            }
            return bestFile;
        }

        void InitFileStats()
        {
            _fileStats = new FileStat[_keyValue2DB.FileCollection.Enumerate().Max()];
            foreach (var id in _keyValue2DB.FileCollection.Enumerate())
            {
                if (id >= _fileStats.Length) continue;
                if (!_keyValue2DB.ContainsValues(id)) continue;
                _fileStats[id] = new FileStat((uint)_keyValue2DB.FileCollection.GetFile(id).GetSize());
            }
        }

        void CalculateFileUsefullness()
        {
            _root.Iterate((ref BTreeLeafMember m) => _fileStats[m.ValueFileId].Increment((uint)Math.Abs(m.ValueSize)));
        }
    }
}