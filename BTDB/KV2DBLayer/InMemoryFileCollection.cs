using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class InMemoryFileCollection : IFileCollection
    {
        // disable invalid warning about using volatile inside Interlocked.CompareExchange
#pragma warning disable 420

        volatile Dictionary<int, IPositionLessStream> _files = new Dictionary<int, IPositionLessStream>();
        int _maxFileId;

        public InMemoryFileCollection()
        {
            _maxFileId = -1;
        }

        public int AddFile(string humanHint)
        {
            var index = Interlocked.Increment(ref _maxFileId) + 1;
            var stream = new MemoryPositionLessStream();
            Dictionary<int, IPositionLessStream> newFiles;
            Dictionary<int, IPositionLessStream> oldFiles;
            do
            {
                oldFiles = _files;
                newFiles = new Dictionary<int, IPositionLessStream>(oldFiles) { { index, stream } };
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);
            return index;
        }

        public int GetCount()
        {
            return _maxFileId + 1;
        }

        public IPositionLessStream GetFile(int index)
        {
            IPositionLessStream value;
            return _files.TryGetValue(index, out value) ? value : null;
        }

        public void RemoveFile(int index)
        {
            IPositionLessStream value;
            Dictionary<int, IPositionLessStream> newFiles;
            Dictionary<int, IPositionLessStream> oldFiles;
            do
            {
                oldFiles = _files;
                if (!oldFiles.TryGetValue(index, out value)) return;
                newFiles = new Dictionary<int, IPositionLessStream>(oldFiles);
                newFiles.Remove(index);
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);
            value.Dispose();
        }

        public IEnumerable<int> Enumerate()
        {
            return _files.Keys;
        }

        public void Dispose()
        {
        }
    }
}
