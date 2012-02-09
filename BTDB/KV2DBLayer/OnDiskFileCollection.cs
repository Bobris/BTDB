using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    public class OnDiskFileCollection : IFileCollection, IDisposable
    {
        readonly string _directory;
        readonly ConcurrentDictionary<int, KeyValuePair<string, IPositionLessStream>> _files = new ConcurrentDictionary<int, KeyValuePair<string, IPositionLessStream>>();
        int _maxFileId;

        public OnDiskFileCollection(string directory)
        {
            _directory = directory;
            _maxFileId = -1;
            foreach (var fileName in Directory.EnumerateFiles(directory))
            {
                int id = GetFileId(fileName);
                var stream = new PositionLessStreamProxy(Path.Combine(directory, fileName));
                _files.TryAdd(id, new KeyValuePair<string, IPositionLessStream>(fileName, stream));
                if (id > _maxFileId) _maxFileId = id;
            }
        }

        static int GetFileId(string fileName)
        {
            var numberEnd = fileName.IndexOf('.');
            if (numberEnd == -1) numberEnd = fileName.Length;
            int result;
            if (int.TryParse(fileName.Substring(0,numberEnd),out result))
            {
                return result;
            }
            return -1;
        }

        public int AddFile(string humanHint)
        {
            var index = Interlocked.Increment(ref _maxFileId) + 1;
            var fileName = index.ToString("D8")+"."+(humanHint??"");
            var stream = new PositionLessStreamProxy(Path.Combine(_directory, fileName));
            _files.TryAdd(index, new KeyValuePair<string, IPositionLessStream>(fileName, stream));
            return index;
        }

        public int GetCount()
        {
            return _maxFileId + 1;
        }

        public IPositionLessStream GetFile(int index)
        {
            KeyValuePair<string, IPositionLessStream> value;
            return _files.TryGetValue(index, out value) ? value.Value : null;
        }

        public void RemoveFile(int index)
        {
            KeyValuePair<string, IPositionLessStream> value;
            if (!_files.TryRemove(index, out value)) return;
            value.Value.Dispose();
            File.Delete(Path.Combine(_directory,value.Key));
        }

        public IEnumerable<int> Enumerate()
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