using System.Collections.Generic;
using System.IO;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class OnDiskFileCollection : IFileCollection
    {
        readonly string _directory;

        // disable invalid warning about using volatile inside Interlocked.CompareExchange
#pragma warning disable 420

        volatile Dictionary<uint, KeyValuePair<string, IPositionLessStream>> _files = new Dictionary<uint, KeyValuePair<string, IPositionLessStream>>();
        int _maxFileId;

        public OnDiskFileCollection(string directory)
        {
            _directory = directory;
            _maxFileId = 0;
            foreach (var filePath in Directory.EnumerateFiles(directory))
            {
                var fileName = Path.GetFileNameWithoutExtension(filePath);
                var id = GetFileId(fileName);
                if (id == 0) continue;
                var stream = new PositionLessStreamProxy(filePath);
                _files.Add(id, new KeyValuePair<string, IPositionLessStream>(fileName, stream));
                if (id > _maxFileId) _maxFileId = (int) id;
            }
        }

        static uint GetFileId(string fileName)
        {
            uint result;
            if (uint.TryParse(fileName, out result))
            {
                return result;
            }
            return 0;
        }

        public uint AddFile(string humanHint)
        {
            var index = (uint)Interlocked.Increment(ref _maxFileId);
            var fileName = index.ToString("D8") + "." + (humanHint ?? "");
            var stream = new PositionLessStreamProxy(Path.Combine(_directory, fileName));
            Dictionary<uint, KeyValuePair<string, IPositionLessStream>> newFiles;
            Dictionary<uint, KeyValuePair<string, IPositionLessStream>> oldFiles;
            do
            {
                oldFiles = _files;
                newFiles = new Dictionary<uint, KeyValuePair<string, IPositionLessStream>>(oldFiles) 
                    { { index, new KeyValuePair<string, IPositionLessStream>(fileName, stream) } };
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);
            return index;
        }

        public uint GetCount()
        {
            return (uint) _files.Count;
        }

        public IPositionLessStream GetFile(uint index)
        {
            KeyValuePair<string, IPositionLessStream> value;
            return _files.TryGetValue(index, out value) ? value.Value : null;
        }

        public void RemoveFile(uint index)
        {
            KeyValuePair<string, IPositionLessStream> value;
            Dictionary<uint, KeyValuePair<string, IPositionLessStream>> newFiles;
            Dictionary<uint, KeyValuePair<string, IPositionLessStream>> oldFiles;
            do
            {
                oldFiles = _files;
                if (!oldFiles.TryGetValue(index, out value)) return;
                newFiles = new Dictionary<uint, KeyValuePair<string, IPositionLessStream>>(oldFiles);
                newFiles.Remove(index);
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);
            value.Value.Dispose();
            File.Delete(Path.Combine(_directory, value.Key));
        }

        public IEnumerable<uint> Enumerate()
        {
            return _files.Keys;
        }

        public void Dispose()
        {
            foreach (var file in _files.Values)
            {
                file.Value.Dispose();
            }
        }
    }
}