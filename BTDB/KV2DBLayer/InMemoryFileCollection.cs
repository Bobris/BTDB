using System.Collections.Generic;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class InMemoryFileCollection : IFileCollection
    {
        // disable invalid warning about using volatile inside Interlocked.CompareExchange
#pragma warning disable 420

        volatile Dictionary<uint, IPositionLessStream> _files = new Dictionary<uint, IPositionLessStream>();
        int _maxFileId;

        public InMemoryFileCollection()
        {
            _maxFileId = 0;
        }

        public uint AddFile(string humanHint)
        {
            var index = (uint)Interlocked.Increment(ref _maxFileId);
            var stream = new MemoryPositionLessStream();
            Dictionary<uint, IPositionLessStream> newFiles;
            Dictionary<uint, IPositionLessStream> oldFiles;
            do
            {
                oldFiles = _files;
                newFiles = new Dictionary<uint, IPositionLessStream>(oldFiles) { { index, stream } };
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);
            return index;
        }

        public uint GetCount()
        {
            return (uint) _files.Count;
        }

        public IPositionLessStream GetFile(uint index)
        {
            IPositionLessStream value;
            return _files.TryGetValue(index, out value) ? value : null;
        }

        public void RemoveFile(uint index)
        {
            IPositionLessStream value;
            Dictionary<uint, IPositionLessStream> newFiles;
            Dictionary<uint, IPositionLessStream> oldFiles;
            do
            {
                oldFiles = _files;
                if (!oldFiles.TryGetValue(index, out value)) return;
                newFiles = new Dictionary<uint, IPositionLessStream>(oldFiles);
                newFiles.Remove(index);
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);
            value.Dispose();
        }

        public IEnumerable<uint> Enumerate()
        {
            return _files.Keys;
        }

        public void Dispose()
        {
        }
    }
}
