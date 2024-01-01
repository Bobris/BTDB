using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.Buffer;
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

        sealed class Reader : IMemReader
        {
            readonly File _owner;
            readonly ulong _valueSize;

            public Reader(File owner)
            {
                _owner = owner;
                _valueSize = _owner.GetSize();
                if (_valueSize != 0)
                {
                    _owner.MapContent();
                }
            }

            public void Init(ref MemReader reader)
            {
                reader.Start = (nint)_owner._pointer;
                reader.Current = reader.Start;
                reader.End = reader.Start + (nint)_valueSize;
            }

            public void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
            {
                if (memReader.Current == memReader.End) PackUnpack.ThrowEndOfStreamException();
            }

            public long GetCurrentPosition(in MemReader memReader)
            {
                return memReader.Current - memReader.Start;
            }

            public void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
            {
                PackUnpack.ThrowEndOfStreamException();
            }

            public void SkipBlock(ref MemReader memReader, nuint length)
            {
                PackUnpack.ThrowEndOfStreamException();
            }

            public void SetCurrentPosition(ref MemReader memReader, long position)
            {
                throw new NotSupportedException();
            }

            public bool Eof(ref MemReader memReader)
            {
                return memReader.Current == memReader.End;
            }
        }

        sealed class Writer : IMemWriter
        {
            readonly File _file;
            internal ulong Ofs;

            public Writer(File file)
            {
                _file = file;
                Ofs = (ulong)_file._trueLength;
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

            public void Init(ref MemWriter memWriter)
            {
                memWriter.Start = (nint)_file._pointer;
                memWriter.Current = memWriter.Start + (nint)Ofs;
                memWriter.End = memWriter.Start + (nint)_file._cachedLength;
            }

            public void Flush(ref MemWriter memWriter, uint spaceNeeded)
            {
                Ofs = (ulong)(memWriter.Current - memWriter.Start);
                lock (_file._lock)
                {
                    _file._trueLength = (long)Ofs;
                }

                if (spaceNeeded == 0) return;
                ExpandIfNeeded((long)Ofs + ResizeChunkSize);
                Init(ref memWriter);
            }

            public long GetCurrentPosition(in MemWriter memWriter)
            {
                return memWriter.Current - memWriter.Start;
            }

            public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
            {
                Ofs = (ulong)(memWriter.Current - memWriter.Start);
                ExpandIfNeeded((long)Ofs + (long)length);
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(_file._pointer + Ofs), ref buffer,
                    (uint)length);
                Ofs += length;
                Init(ref memWriter);
            }

            public void SetCurrentPosition(ref MemWriter memWriter, long position)
            {
                throw new NotSupportedException();
            }
        }

        public IMemReader GetExclusiveReader()
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
                MapContent();
                new Span<byte>(_pointer + position, data.Length).CopyTo(data);
            }
        }

        public IMemWriter GetAppenderWriter()
        {
            return _writer;
        }

        public IMemWriter GetExclusiveAppenderWriter()
        {
            return _writer;
        }

        public void HardFlush()
        {
            _stream.Flush(true);
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
                return _writer.Ofs;
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
                newFiles = new(oldFiles);
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
