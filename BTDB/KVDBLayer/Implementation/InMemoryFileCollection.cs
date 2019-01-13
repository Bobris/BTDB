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
            byte[][] _data = Array.Empty<byte[]>();
            readonly Writer _writer;
            long _flushedSize;
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

                    Buf = _file._data[(int) (_ofs / OneBufSize)];
                    End = (int) Math.Min(_totalSize - _ofs, OneBufSize);
                    _ofs += (ulong) End;
                    Pos = 0;
                }

                public override long GetCurrentPosition()
                {
                    return (long) _ofs - End + Pos;
                }
            }

            public AbstractBufferedReader GetExclusiveReader()
            {
                return new Reader(this);
            }

            public void AdvisePrefetch()
            {
            }

            public void RandomRead(Span<byte> data, ulong position, bool doNotCache)
            {
                var storage = Volatile.Read(ref _data);
                while (!data.IsEmpty)
                {
                    var buf = storage[(int) (position / OneBufSize)];
                    var bufOfs = (int) (position % OneBufSize);
                    var copy = Math.Min(data.Length, OneBufSize - bufOfs);
                    buf.AsSpan(bufOfs,copy).CopyTo(data);
                    data = data.Slice(copy);
                    position += (ulong) copy;
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
                    Buf = null;
                    End = 0;
                }

                public override void FlushBuffer()
                {
                    if (Pos != End) return;
                    _ofs += (ulong)End;
                    Pos = 0;
                    Buf = new byte[OneBufSize];
                    End = OneBufSize;
                    var storage = _file._data;
                    Array.Resize(ref storage, storage.Length + 1);
                    storage[storage.Length - 1] = Buf;
                    Volatile.Write(ref _file._data, storage);
                }

                public override long GetCurrentPosition()
                {
                    return (long) (_ofs + (ulong) Pos);
                }

                internal void SimulateCorruptionBySetSize(int size)
                {
                    if (size > OneBufSize || _ofs != 0) throw new ArgumentOutOfRangeException();
                    Array.Clear(Buf, size, Pos - size);
                    Pos = size;
                }
            }

            public AbstractBufferedWriter GetAppenderWriter()
            {
                return _writer;
            }

            public AbstractBufferedWriter GetExclusiveAppenderWriter()
            {
                return _writer;
            }

            public void Flush()
            {
                _flushedSize = _writer.GetCurrentPosition();
                Interlocked.MemoryBarrier();
            }

            public void HardFlush()
            {
                Flush();
            }

            public void SetSize(long size)
            {
                if ((ulong) size != GetSize())
                    throw new InvalidOperationException(
                        "For in memory collection SetSize should never be set to something else than GetSize");
            }

            public void Truncate()
            {
            }

            public void HardFlushTruncateSwitchToReadOnlyMode()
            {
                Flush();
            }

            public void HardFlushTruncateSwitchToDisposedMode()
            {
                Flush();
            }

            public ulong GetSize()
            {
                Volatile.Read(ref _data);
                return (ulong) _flushedSize;
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
            var index = (uint) Interlocked.Increment(ref _maxFileId);
            var file = new File(this, index);
            Dictionary<uint, File> newFiles;
            Dictionary<uint, File> oldFiles;
            do
            {
                oldFiles = _files;
                newFiles = new Dictionary<uint, File>(oldFiles) {{index, file}};
            } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);

            return file;
        }

        public uint GetCount()
        {
            return (uint) _files.Count;
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

        public void ConcurrentTemporaryTruncate(uint index, uint offset)
        {
            // Nothing to do
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
