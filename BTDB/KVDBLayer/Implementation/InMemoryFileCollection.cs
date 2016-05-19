using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class InMemoryFileCollection : IFileCollection
    {
        // disable invalid warning about using volatile inside Interlocked.CompareExchange
#pragma warning disable 420

        volatile Dictionary<uint, File> _files = new Dictionary<uint, File>();
        int _maxFileId;

        public InMemoryFileCollection()
        {
            _maxFileId = 0;
        }

        class File : IFileCollectionFile
        {
            readonly InMemoryFileCollection _owner;
            readonly uint _index;
            readonly List<byte[]> _data = new List<byte[]>();
            readonly object _lock = new object();
            readonly Writer _writer;
            const int OneBufSize = 128 * 1024;

            public File(InMemoryFileCollection owner, uint index)
            {
                _owner = owner;
                _index = index;
                _writer = new Writer(this);
            }

            public uint Index => _index;

            sealed class Reader : AbstractBufferedReader
            {
                readonly File _file;
                ulong _ofs;
                readonly ulong _totalSize;

                public Reader(File file)
                {
                    _file = file;
                    _totalSize = file.GetSize();
                    _ofs = 0;
                    FillBuffer();
                }

                protected override void FillBuffer()
                {
                    if (_ofs == _totalSize)
                    {
                        Pos = -1;
                        End = -1;
                        return;
                    }
                    Buf = _file._data[(int)(_ofs / OneBufSize)];
                    End = (int)Math.Min(_totalSize - _ofs, OneBufSize);
                    _ofs += (ulong)End;
                    Pos = 0;
                }

                public override long GetCurrentPosition()
                {
                    return (long)_ofs - End + Pos;
                }
            }

            public AbstractBufferedReader GetExclusiveReader()
            {
                return new Reader(this);
            }

            public void RandomRead(byte[] data, int offset, int size, ulong position, bool doNotCache)
            {
                while (size > 0)
                {
                    byte[] buf;
                    lock (_lock)
                    {
                        if (position + (ulong)size > (ulong)_writer.GetCurrentPosition()) throw new EndOfStreamException();
                        buf = _data[(int)(position / OneBufSize)];
                    }
                    var bufofs = (int)(position % OneBufSize);
                    var copy = Math.Min(size, OneBufSize - bufofs);
                    Array.Copy(buf, bufofs, data, offset, copy);
                    offset += copy;
                    size -= copy;
                    position += (ulong)copy;
                }
            }

            sealed class Writer : AbstractBufferedWriter
            {
                readonly File _file;
                ulong _ofs;

                public Writer(File file)
                {
                    _file = file;
                    Pos = 0;
                    Buf = new byte[OneBufSize];
                    End = OneBufSize;
                    lock (_file._lock)
                    {
                        _file._data.Add(Buf);
                    }
                }

                public override void FlushBuffer()
                {
                    if (Pos != End) return;
                    _ofs += OneBufSize;
                    Pos = 0;
                    Buf = new byte[OneBufSize];
                    lock (_file._lock)
                    {
                        _file._data.Add(Buf);
                    }
                }

                public override long GetCurrentPosition()
                {
                    return (long)(_ofs + (ulong)Pos);
                }

                internal void SimulateCorruptionBySetSize(int size)
                {
                    if (size > OneBufSize || _ofs!=0) throw new ArgumentOutOfRangeException();
                    Pos = size;
                }
            }

            public AbstractBufferedWriter GetAppenderWriter()
            {
                return _writer;
            }

            public void HardFlush()
            {
            }

            public void SetSize(long size)
            {
                if ((ulong)size!=GetSize())
                    throw new InvalidOperationException("For in memory collection SetSize should never be set to something else than GetSize");
            }

            public void Truncate()
            {
            }

            public ulong GetSize()
            {
                lock (_lock)
                {
                    return (ulong)_writer.GetCurrentPosition();
                }
            }

            public void Remove()
            {
                Dictionary<uint, File> newFiles;
                Dictionary<uint, File> oldFiles;
                do
                {
                    oldFiles = _owner._files;
                    File value;
                    if (!oldFiles.TryGetValue(_index, out value)) return;
                    newFiles = new Dictionary<uint, File>(oldFiles);
                    newFiles.Remove(_index);
                } while (Interlocked.CompareExchange(ref _owner._files, newFiles, oldFiles) != oldFiles);
            }

            internal void SimulateCorruptionBySetSize(int size)
            {
                _writer.SimulateCorruptionBySetSize(size);
            }
        }

        public IFileCollectionFile AddFile(string humanHint)
        {
            var index = (uint)Interlocked.Increment(ref _maxFileId);
            var file = new File(this, index);
            Dictionary<uint, File> newFiles;
            Dictionary<uint, File> oldFiles;
            do
            {
                oldFiles = _files;
                newFiles = new Dictionary<uint, File>(oldFiles) { { index, file } };
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);
            return file;
        }

        public uint GetCount()
        {
            return (uint)_files.Count;
        }

        public IFileCollectionFile GetFile(uint index)
        {
            File value;
            return _files.TryGetValue(index, out value) ? value : null;
        }

        public IEnumerable<IFileCollectionFile> Enumerate()
        {
            return _files.Values;
        }

        public void Dispose()
        {
        }

        internal void SimulateCorruptionBySetSize(int size)
        {
            _files[1].SimulateCorruptionBySetSize(size);
        }
    }
}
