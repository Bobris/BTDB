using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public class OnDiskMemoryMappedFileCollection : IFileCollection
    {
        [DllImport("kernel32.dll", EntryPoint = "RtlMoveMemory")]
        static extern unsafe void CopyMemory(byte* dst, byte* src, long size);

        public IDeleteFileCollectionStrategy DeleteFileCollectionStrategy
        {
            get
            {
                return _deleteFileCollectionStrategy ??
                       (_deleteFileCollectionStrategy = new JustDeleteFileCollectionStrategy());
            }
            set { _deleteFileCollectionStrategy = value; }
        }

        readonly string _directory;

        // disable invalid warning about using volatile inside Interlocked.CompareExchange
#pragma warning disable 420

        volatile Dictionary<uint, File> _files = new Dictionary<uint, File>();
        int _maxFileId;
        IDeleteFileCollectionStrategy _deleteFileCollectionStrategy;

        sealed unsafe class File : IFileCollectionFile
        {
            readonly OnDiskMemoryMappedFileCollection _owner;
            readonly uint _index;
            readonly string _fileName;
            long _trueLength;
            readonly FileStream _stream;
            readonly object _lock = new object();
            readonly Writer _writer;
            MemoryMappedFile _memoryMappedFile;
            MemoryMappedViewAccessor _accessor;
            byte* _pointer;
            const long ResizeChunkSize = 4 * 1024 * 1024;

            public File(OnDiskMemoryMappedFileCollection owner, uint index, string fileName)
            {
                _owner = owner;
                _index = index;
                _fileName = fileName;
                _stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 1, FileOptions.None);
                _trueLength = _stream.Length;
                _writer = new Writer(this);
            }

            internal void Dispose()
            {
                _writer.FlushBuffer();
                UnmapContent();
                _stream.SetLength(_trueLength);
                _stream.Dispose();
            }

            public uint Index => _index;

            void MapContent()
            {
                if (_accessor != null) return;
                _memoryMappedFile = MemoryMappedFile.CreateFromFile(_stream, null, 0, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, true);
                _accessor = _memoryMappedFile.CreateViewAccessor();
                _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
            }

            void UnmapContent()
            {
                if (_accessor == null) return;
                _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                _accessor.Dispose();
                _accessor = null;
                _memoryMappedFile.Dispose();
                _memoryMappedFile = null;
            }

            sealed class Reader : AbstractBufferedReader
            {
                readonly File _owner;
                readonly ulong _valueSize;
                ulong _ofs;

                public Reader(File owner)
                {
                    _owner = owner;
                    _valueSize = _owner.GetSize();
                    if (_valueSize != 0)
                    {
                        _owner.MapContent();
                    }
                    _ofs = 0;
                    Buf = new byte[32768];
                    FillBuffer();
                }

                protected override void FillBuffer()
                {
                    if (_ofs == _valueSize)
                    {
                        Pos = -1;
                        End = -1;
                        return;
                    }
                    End = (int)Math.Min((long)(_valueSize - _ofs), Buf.Length);
                    fixed (byte* dst = Buf)
                    {
                        CopyMemory(dst, _owner._pointer + _ofs, End);
                    }
                    _ofs += (ulong)End;
                    Pos = 0;
                }

                public override void ReadBlock(byte[] data, int offset, int length)
                {
                    if (length < Buf.Length)
                    {
                        base.ReadBlock(data, offset, length);
                        return;
                    }
                    var l = End - Pos;
                    Array.Copy(Buf, Pos, data, offset, l);
                    offset += l;
                    length -= l;
                    Pos += l;
                    if ((long)_valueSize - (long)_ofs < length)
                        throw new EndOfStreamException();
                    fixed (byte* dst = data)
                    {
                        CopyMemory(dst + offset, _owner._pointer + _ofs, length);
                    }
                    _ofs += (ulong)length;
                }

                public override void SkipBlock(int length)
                {
                    if (length < Buf.Length)
                    {
                        base.SkipBlock(length);
                        return;
                    }
                    if (GetCurrentPosition() + length > (long)_valueSize)
                    {
                        _ofs = _valueSize;
                        Pos = 0;
                        End = -1;
                        throw new EndOfStreamException();
                    }
                    var l = End - Pos;
                    Pos = End;
                    length -= l;
                    _ofs += (ulong)length;
                }

                public override long GetCurrentPosition()
                {
                    return (long)_ofs - End + Pos;
                }
            }

            sealed class Writer : AbstractBufferedWriter
            {
                readonly File _file;
                internal ulong Ofs;

                public Writer(File file)
                {
                    _file = file;
                    Buf = new byte[1024 * 32];
                    End = Buf.Length;
                    Ofs = (ulong)_file._trueLength;
                }

                public override void FlushBuffer()
                {
                    if (Pos != 0) lock (_file._lock)
                        {
                            ExpandIfNeeded((long)Ofs + Pos);
                            fixed (byte* src = Buf)
                            {
                                CopyMemory(_file._pointer + Ofs, src, Pos);
                            }
                            Ofs += (ulong)Pos;
                            _file._trueLength = (long)Ofs;
                            Pos = 0;
                        }
                }

                void ExpandIfNeeded(long size)
                {
                    if (_file._stream.Length < size)
                    {
                        _file.UnmapContent();
                        var newsize = ((size - 1) / ResizeChunkSize + 1) * ResizeChunkSize;
                        _file._stream.SetLength(newsize);
                    }
                    _file.MapContent();
                }

                public override void WriteBlock(byte[] data, int offset, int length)
                {
                    if (length < Buf.Length)
                    {
                        base.WriteBlock(data, offset, length);
                        return;
                    }
                    FlushBuffer();
                    lock (_file._lock)
                    {
                        fixed (byte* src = data)
                        {
                            CopyMemory(_file._pointer + Ofs, src + offset, length);
                        }
                        Ofs += (ulong)length;
                        _file._trueLength = (long)Ofs;
                    }
                }

                public override long GetCurrentPosition()
                {
                    return (long)(Ofs + (ulong)Pos);
                }

                internal byte[] GetBuffer()
                {
                    return Buf;
                }
            }

            public AbstractBufferedReader GetExclusiveReader()
            {
                return new Reader(this);
            }

            public void RandomRead(byte[] data, int offset, int size, ulong position, bool doNotCache)
            {
                lock (_lock)
                {
                    if (size > 0 && position < _writer.Ofs)
                    {
                        MapContent();
                        var read = size;
                        if (_writer.Ofs - position < (ulong)read) read = (int)(_writer.Ofs - position);
                        fixed (byte* dst = data)
                        {
                            CopyMemory(dst + offset, _pointer + position, read);
                        }
                        size -= read;
                        offset += read;
                        position += (ulong)read;
                    }
                    if (size == 0) return;
                    if ((ulong)_writer.GetCurrentPosition() < position + (ulong)size)
                        throw new EndOfStreamException();
                    Array.Copy(_writer.GetBuffer(), (int)(position - _writer.Ofs), data, offset, size);
                }
            }

            public AbstractBufferedWriter GetAppenderWriter()
            {
                return _writer;
            }

            public void HardFlush()
            {
                _writer.FlushBuffer();
                var fileStream = _stream as FileStream;
                if (fileStream != null)
                {
                    fileStream.Flush(true);
                }
                else
                {
                    _stream.Flush();
                }
            }

            public void SetSize(long size)
            {
                _writer.FlushBuffer();
                lock (_lock)
                {
                    _writer.Ofs = (ulong) size;
                    _trueLength = size;
                }
            }

            public void Truncate()
            {
                UnmapContent();
                _stream.SetLength(_trueLength);
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
                _stream.Dispose();
                _owner.DeleteFileCollectionStrategy.DeleteFile(_fileName);
            }
        }

        public OnDiskMemoryMappedFileCollection(string directory)
        {
            _directory = directory;
            _maxFileId = 0;
            foreach (var filePath in Directory.EnumerateFiles(directory))
            {
                var id = GetFileId(Path.GetFileNameWithoutExtension(filePath));
                if (id == 0) continue;
                var file = new File(this, id, filePath);
                _files.Add(id, file);
                if (id > _maxFileId) _maxFileId = (int)id;
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

        public IFileCollectionFile AddFile(string humanHint)
        {
            var index = (uint)Interlocked.Increment(ref _maxFileId);
            var fileName = index.ToString("D8") + "." + (humanHint ?? "");
            var file = new File(this, index, Path.Combine(_directory, fileName));
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

        public void ConcurentTemporaryTruncate(uint index, uint offset)
        {
            // Nothing to do
        }

        public void Dispose()
        {
            foreach (var file in _files.Values)
            {
                file.Dispose();
            }
        }
    }
}