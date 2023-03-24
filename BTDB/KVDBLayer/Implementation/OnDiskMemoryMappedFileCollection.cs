using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public class OnDiskMemoryMappedFileCollection : IFileCollection
{
    public IDeleteFileCollectionStrategy DeleteFileCollectionStrategy
    {
        get => _deleteFileCollectionStrategy ??= new JustDeleteFileCollectionStrategy();
        set => _deleteFileCollectionStrategy = value;
    }

    readonly string _directory;

    // disable invalid warning about using volatile inside Interlocked.CompareExchange
#pragma warning disable 420

    volatile Dictionary<uint, File> _files = new Dictionary<uint, File>();
    int _maxFileId;
    IDeleteFileCollectionStrategy? _deleteFileCollectionStrategy;

    sealed unsafe class File : IFileCollectionFile
    {
        readonly OnDiskMemoryMappedFileCollection _owner;
        readonly uint _index;
        readonly string _fileName;
        long _trueLength;
        long _cachedLength;
        readonly FileStream _stream;
        readonly object _lock = new object();
        readonly Writer _writer;
        MemoryMappedFile? _memoryMappedFile;
        MemoryMappedViewAccessor? _accessor;
        byte* _pointer;
        const long ResizeChunkSize = 4 * 1024 * 1024;

        public File(OnDiskMemoryMappedFileCollection owner, uint index, string fileName)
        {
            _owner = owner;
            _index = index;
            _fileName = fileName;
            _stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 1,
                FileOptions.None);
            _trueLength = _stream.Length;
            _cachedLength = _trueLength;
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
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(_stream, null, 0, MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None, true);
            _accessor = _memoryMappedFile!.CreateViewAccessor();
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
        }

        void UnmapContent()
        {
            if (_accessor == null) return;
            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            _accessor.Dispose();
            _accessor = null;
            _memoryMappedFile!.Dispose();
            _memoryMappedFile = null;
        }

        sealed class Reader : ISpanReader
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
            }

            public void Init(ref SpanReader spanReader)
            {
                spanReader.Buf = new Span<byte>(_owner._pointer + _ofs,
                    (int)Math.Min(_valueSize - _ofs, int.MaxValue));
                spanReader.Original = spanReader.Buf;
            }

            public bool FillBufAndCheckForEof(ref SpanReader spanReader)
            {
                _ofs += (ulong)(spanReader.Original.Length - spanReader.Buf.Length);
                spanReader.Buf = new Span<byte>(_owner._pointer + _ofs,
                    (int)Math.Min(_valueSize - _ofs, int.MaxValue));
                spanReader.Original = spanReader.Buf;
                return 0 == spanReader.Buf.Length;
            }

            public long GetCurrentPosition(in SpanReader spanReader)
            {
                return (long)_ofs + spanReader.Original.Length - spanReader.Buf.Length;
            }

            public bool ReadBlock(ref SpanReader spanReader, ref byte buffer, uint length)
            {
                if (length <= _valueSize - _ofs)
                {
                    Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>(_owner._pointer + _ofs), length);
                    _ofs += length;
                    return false;
                }

                _ofs = _valueSize;
                return true;
            }

            public bool SkipBlock(ref SpanReader spanReader, uint length)
            {
                if (length <= _valueSize - _ofs)
                {
                    _ofs += length;
                    return false;
                }

                _ofs = _valueSize;
                return true;
            }

            public void SetCurrentPosition(ref SpanReader spanReader, long position)
            {
                throw new NotSupportedException();
            }

            public void Sync(ref SpanReader spanReader)
            {
                _ofs += (uint)(spanReader.Original.Length - spanReader.Buf.Length);
            }
        }

        sealed class Writer : ISpanWriter
        {
            readonly File _file;
            internal ulong Ofs;

            public Writer(File file)
            {
                _file = file;
                Ofs = (ulong)_file._trueLength;
            }

            public void FlushBuffer()
            {
                lock (_file._lock)
                {
                    _file._trueLength = (long)Ofs;
                }
            }

            void ExpandIfNeeded(long size)
            {
                if (_file._cachedLength < size)
                {
                    _file.UnmapContent();
                    var newSize = ((size - 1) / ResizeChunkSize + 1) * ResizeChunkSize;
                    _file._stream.SetLength(newSize);
                    _file._cachedLength = newSize;
                }

                _file.MapContent();
            }

            public void Init(ref SpanWriter spanWriter)
            {
                spanWriter.Buf = new(_file._pointer + Ofs,
                    (int)Math.Min((ulong)_file._cachedLength - Ofs, int.MaxValue));
                spanWriter.InitialBuffer = spanWriter.Buf;
            }

            public void Sync(ref SpanWriter spanWriter)
            {
                Ofs += (ulong)(spanWriter.InitialBuffer.Length - spanWriter.Buf.Length);
            }

            public bool Flush(ref SpanWriter spanWriter)
            {
                Sync(ref spanWriter);
                ExpandIfNeeded((long)Ofs + ResizeChunkSize);
                Init(ref spanWriter);
                return true;
            }

            public long GetCurrentPosition(in SpanWriter spanWriter)
            {
                return (long)(Ofs + (ulong)(spanWriter.InitialBuffer.Length - spanWriter.Buf.Length));
            }

            public long GetCurrentPositionWithoutWriter()
            {
                return (long)Ofs;
            }

            public void WriteBlock(ref SpanWriter spanWriter, ref byte buffer, uint length)
            {
                Sync(ref spanWriter);
                WriteBlockWithoutWriter(ref buffer, length);
                Init(ref spanWriter);
            }

            public void WriteBlockWithoutWriter(ref byte buffer, uint length)
            {
                ExpandIfNeeded((long)Ofs + length);
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(_file._pointer + Ofs), ref buffer,
                    length);
                Ofs += length;
            }

            public void SetCurrentPosition(ref SpanWriter spanWriter, long position)
            {
                throw new NotSupportedException();
            }
        }

        public ISpanReader GetExclusiveReader()
        {
            return new Reader(this);
        }

        public void AdvisePrefetch()
        {
        }

        public void RandomRead(Span<byte> data, ulong position, bool doNotCache)
        {
            lock (_lock)
            {
                if (data.Length > 0 && position < _writer.Ofs)
                {
                    MapContent();
                    var read = data.Length;
                    if (_writer.Ofs - position < (ulong)read) read = (int)(_writer.Ofs - position);
                    new Span<byte>(_pointer + position, read).CopyTo(data);
                    data = data[read..];
                }

                if (data.Length == 0) return;
                throw new EndOfStreamException();
            }
        }

        public ISpanWriter GetAppenderWriter()
        {
            return _writer;
        }

        public ISpanWriter GetExclusiveAppenderWriter()
        {
            return _writer;
        }

        public void Flush()
        {
            _writer.FlushBuffer();
        }

        public void HardFlush()
        {
            Flush();
            _stream.Flush(true);
        }

        public void SetSize(long size)
        {
            _writer.FlushBuffer();
            lock (_lock)
            {
                _writer.Ofs = (ulong)size;
                _trueLength = size;
            }
        }

        public void Truncate()
        {
            UnmapContent();
            _stream.SetLength(_trueLength);
        }

        public void HardFlushTruncateSwitchToReadOnlyMode()
        {
            HardFlush();
            Truncate();
        }

        public void HardFlushTruncateSwitchToDisposedMode()
        {
            HardFlush();
            Truncate();
        }

        public ulong GetSize()
        {
            lock (_lock)
            {
                return (ulong)_writer.GetCurrentPositionWithoutWriter();
            }
        }

        public void Remove()
        {
            Dictionary<uint, File> newFiles;
            Dictionary<uint, File> oldFiles;
            do
            {
                oldFiles = _owner._files;
                if (!oldFiles!.TryGetValue(_index, out _)) return;
                newFiles = new Dictionary<uint, File>(oldFiles);
                newFiles.Remove(_index);
            } while (Interlocked.CompareExchange(ref _owner._files, newFiles, oldFiles) != oldFiles);

            UnmapContent();
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
        return uint.TryParse(fileName, out var result) ? result : 0;
    }

    public IFileCollectionFile AddFile(string? humanHint)
    {
        var index = (uint)Interlocked.Increment(ref _maxFileId);
        var fileName = index.ToString("D8") + "." + (humanHint ?? "");
        var file = new File(this, index, Path.Combine(_directory, fileName));
        Dictionary<uint, File> newFiles;
        Dictionary<uint, File> oldFiles;
        do
        {
            oldFiles = _files;
            newFiles = new(oldFiles!) { { index, file } };
        } while (Interlocked.CompareExchange(ref _files, newFiles, oldFiles) != oldFiles);

        return file;
    }

    public uint GetCount()
    {
        return (uint)_files.Count;
    }

    public IFileCollectionFile? GetFile(uint index)
    {
        return _files.TryGetValue(index, out var value) ? value : null;
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
        foreach (var file in _files.Values)
        {
            file.Dispose();
        }
    }
}
