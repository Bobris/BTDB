using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class InMemoryFileCollection : IFileCollection
    {
        readonly ConcurrentDictionary<int, IPositionLessStream> _files = new ConcurrentDictionary<int, IPositionLessStream>();
        int _maxFileId;

        public InMemoryFileCollection()
        {
            _maxFileId = -1;
        }

        public int AddFile(string humanHint)
        {
            var index = Interlocked.Increment(ref _maxFileId) + 1;
            var stream = new MemoryPositionLessStream();
            _files.TryAdd(index, stream);
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
            if (!_files.TryRemove(index, out value)) return;
            value.Dispose();
        }

        public IEnumerable<int> Enumerate()
        {
            return _files.Keys;
        }
    }
}
