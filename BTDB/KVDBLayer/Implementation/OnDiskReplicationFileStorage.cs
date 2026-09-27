using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

/// Durable node-local replication storage: one "{id:D8}.{hint}" file per ID in a directory, written and read through
/// memory mapping. Appends grow the mapping in chunks; exclusive readers and RandomRead copy under the file lock, so
/// growth may remap while other threads read. The single appender keeps raw pointers between writer calls, therefore
/// HardFlush only flushes; the physical file is truncated to its logical length when it becomes read-only or on
/// disposal. After a crash a file may end with zero padding: replication validates cached files against the remote
/// inventory and discards local-only files before opening a database, so padding never becomes history.
public sealed class OnDiskReplicationFileStorage : IReplicationFileStorage
{
    const long GrowthChunk = 4 * 1024 * 1024;
    readonly string _directory;
    readonly object _creationLock = new();
    volatile Dictionary<uint, File> _files = new();
    FileIdAllocator _fileIdAllocator;

    public OnDiskReplicationFileStorage(string directory)
    {
        _directory = directory;
        Directory.CreateDirectory(directory);
        var files = new Dictionary<uint, File>();
        foreach (var path in Directory.EnumerateFiles(directory))
        {
            var name = Path.GetFileName(path);
            var dot = name.IndexOf('.');
            if (dot <= 0 || !uint.TryParse(name.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out var id) ||
                id == 0) continue;
            if (files.ContainsKey(id))
            {
                foreach (var file in files.Values) file.Dispose();
                throw new InvalidDataException($"Replication storage contains file ID {id} more than once.");
            }
            files.Add(id, new(this, id, path, name[(dot + 1)..]));
            _fileIdAllocator.Observe(id);
        }
        _files = files;
    }

    public IFileCollectionFile AddFile(string humanHint) => AddFile(humanHint, FileIdParity.Any);

    public IFileCollectionFile AddFile(string humanHint, FileIdParity parity)
    {
        lock (_creationLock) return CreateFile(_fileIdAllocator.Allocate(parity), humanHint);
    }

    public IFileCollectionFile ImportFile(uint fileId, string humanHint)
    {
        if (fileId == 0) throw new ArgumentOutOfRangeException(nameof(fileId));
        lock (_creationLock)
        {
            if (_files.ContainsKey(fileId)) throw new InvalidOperationException("The file ID already exists.");
            var file = CreateFile(fileId, humanHint);
            _fileIdAllocator.Observe(fileId);
            return file;
        }
    }

    // Caller holds _creationLock.
    File CreateFile(uint id, string humanHint)
    {
        var path = Path.Combine(_directory, id.ToString("D8", CultureInfo.InvariantCulture) + "." + humanHint);
        if (System.IO.File.Exists(path)) throw new InvalidOperationException($"File {path} already exists.");
        var file = new File(this, id, path, humanHint);
        _files = new(_files) { { id, file } };
        return file;
    }

    public KVFileType? GetFileType(uint fileId) => _files.GetValueOrDefault(fileId)?.FileType;
    public uint GetCount() => (uint)_files.Count;
    public IFileCollectionFile GetFile(uint index) => _files.GetValueOrDefault(index)!;
    public IEnumerable<IFileCollectionFile> Enumerate() => _files.Values;

    public void ConcurrentTemporaryTruncate(uint index, uint offset)
    {
        // Readers are bounded by the logical length; nothing to truncate.
    }

    public void Dispose()
    {
        lock (_creationLock)
        {
            foreach (var file in _files.Values) file.Dispose();
            _files = new();
        }
    }

    void Forget(File file)
    {
        lock (_creationLock)
        {
            if (!_files.TryGetValue(file.Index, out var current) || !ReferenceEquals(current, file)) return;
            var files = new Dictionary<uint, File>(_files);
            files.Remove(file.Index);
            _files = files;
        }
    }

    sealed unsafe class File : IFileCollectionFile
    {
        readonly OnDiskReplicationFileStorage _owner;
        readonly string _path;
        readonly FileStream _stream;
        readonly object _lock = new();
        readonly Writer _writer;
        MemoryMappedFile? _map;
        MemoryMappedViewAccessor? _view;
        byte* _pointer;
        long _capacity;
        long _length;
        bool _readOnly, _removed;

        public File(OnDiskReplicationFileStorage owner, uint index, string path, string humanHint)
        {
            _owner = owner;
            Index = index;
            _path = path;
            FileType = FileCollectionWithFileInfos.FileTypeFromHint(humanHint);
            _stream = new(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 1, FileOptions.None);
            _length = _stream.Length;
            _writer = new(this);
        }

        public uint Index { get; }
        internal KVFileType? FileType { get; }

        // Caller holds _lock. Remapping invalidates only pointers held by the appender, which re-initializes.
        void EnsureMapped(long capacity)
        {
            if (_removed) throw new FileNotFoundException("The replication file was removed.", _path);
            if (_view != null && _capacity >= capacity) return;
            Unmap();
            capacity = Math.Max(capacity, _stream.Length);
            if (capacity == 0) return;
            _map = MemoryMappedFile.CreateFromFile(_stream, null, capacity, MemoryMappedFileAccess.ReadWrite,
                HandleInheritability.None, true);
            _view = _map.CreateViewAccessor(0, capacity, MemoryMappedFileAccess.ReadWrite);
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
            _capacity = capacity;
        }

        // Caller holds _lock. Grows by whole chunks, so the appender remaps only about once per GrowthChunk.
        void EnsureSpace(long needed)
        {
            if (_view != null && _capacity >= needed) return;
            EnsureMapped(needed + GrowthChunk);
        }

        // Caller holds _lock.
        void Unmap()
        {
            if (_view == null) return;
            _view.Flush();
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _view.Dispose();
            _view = null;
            _map!.Dispose();
            _map = null;
            _pointer = null;
            _capacity = 0;
        }

        internal void Dispose()
        {
            lock (_lock)
            {
                if (_removed) return;
                _removed = true;
                Unmap();
                _stream.SetLength(_length);
                _stream.Dispose();
            }
        }

        public IMemReader GetExclusiveReader() => new Reader(this);

        public void AdvisePrefetch()
        {
        }

        public void RandomRead(Span<byte> data, ulong position, bool doNotCache)
        {
            lock (_lock)
            {
                if (_removed) throw new FileNotFoundException("The replication file was removed.", _path);
                if (position > (ulong)_length || (ulong)data.Length > (ulong)_length - position)
                    throw new EndOfStreamException();
                if (data.IsEmpty) return;
                EnsureMapped(_length);
                new ReadOnlySpan<byte>(_pointer + position, data.Length).CopyTo(data);
            }
        }

        public IMemWriter GetAppenderWriter() => _writer;
        public IMemWriter GetExclusiveAppenderWriter() => _writer;

        public void HardFlush()
        {
            lock (_lock)
            {
                if (_removed) return;
                _view?.Flush();
                _stream.Flush(true);
            }
        }

        public void HardFlushTruncateSwitchToReadOnlyMode()
        {
            lock (_lock)
            {
                if (_removed) return;
                _readOnly = true;
                Unmap();
                _stream.SetLength(_length);
                _stream.Flush(true);
            }
        }

        public void HardFlushTruncateSwitchToDisposedMode() => HardFlushTruncateSwitchToReadOnlyMode();

        public ulong GetSize()
        {
            lock (_lock) return (ulong)_length;
        }

        public void Remove()
        {
            _owner.Forget(this);
            lock (_lock)
            {
                if (_removed) return;
                _removed = true;
                Unmap();
                _stream.Dispose();
            }
            System.IO.File.Delete(_path);
        }

        // Buffered copy through RandomRead: no raw pointer into a mapping survives a call.
        sealed class Reader(File file) : IMemReader
        {
            const int BufferSize = 64 * 1024;
            readonly byte[] _buffer = GC.AllocateUninitializedArray<byte>(BufferSize, pinned: true);
            readonly ulong _size = file.GetSize();
            ulong _bufferStart;

            public void Init(ref MemReader reader) => Fill(ref reader, _bufferStart);

            void Fill(ref MemReader reader, ulong position)
            {
                _bufferStart = position;
                var count = (int)Math.Min((ulong)BufferSize, _size - Math.Min(_size, position));
                if (count != 0) file.RandomRead(_buffer.AsSpan(0, count), position, false);
                reader.Start = (nint)Unsafe.AsPointer(ref _buffer[0]);
                reader.Current = reader.Start;
                reader.End = reader.Start + count;
            }

            public void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
            {
                if (memReader.Current != memReader.End) return;
                Fill(ref memReader, _bufferStart + (ulong)(memReader.Current - memReader.Start));
                if (memReader.Current == memReader.End) PackUnpack.ThrowEndOfStreamException();
            }

            public long GetCurrentPosition(in MemReader memReader) =>
                (long)_bufferStart + (memReader.Current - memReader.Start);

            public void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
            {
                while (length > 0)
                {
                    FillBuf(ref memReader, 1);
                    var count = (int)Math.Min((nint)length, memReader.End - memReader.Current);
                    Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((void*)memReader.Current), (uint)count);
                    buffer = ref Unsafe.AddByteOffset(ref buffer, count);
                    memReader.Current += count;
                    length -= (nuint)count;
                }
            }

            public void SkipBlock(ref MemReader memReader, nuint length)
            {
                var position = _bufferStart + (ulong)(memReader.Current - memReader.Start) + length;
                if (position > _size)
                {
                    Fill(ref memReader, _size);
                    PackUnpack.ThrowEndOfStreamException();
                }
                Fill(ref memReader, position);
            }

            public void SetCurrentPosition(ref MemReader memReader, long position)
            {
                ArgumentOutOfRangeException.ThrowIfNegative(position);
                Fill(ref memReader, (ulong)position);
            }

            public bool Eof(ref MemReader memReader) => memReader.Current == memReader.End &&
                _bufferStart + (ulong)(memReader.Current - memReader.Start) >= _size;
        }

        // The single appender writes straight into the mapping between calls; each call publishes the logical length.
        sealed class Writer(File file) : IMemWriter
        {
            public void Init(ref MemWriter memWriter)
            {
                lock (file._lock)
                {
                    if (file._readOnly) throw new InvalidOperationException("The replication file is read-only.");
                    file.EnsureSpace(file._length + 1);
                    memWriter.Start = (nint)file._pointer;
                    memWriter.Current = memWriter.Start + (nint)file._length;
                    memWriter.End = memWriter.Start + (nint)file._capacity;
                }
            }

            public void Flush(ref MemWriter memWriter, uint spaceNeeded)
            {
                lock (file._lock) file._length = memWriter.Current - memWriter.Start;
                if (spaceNeeded == 0) return;
                lock (file._lock) file.EnsureSpace(file._length + spaceNeeded);
                Init(ref memWriter);
            }

            public long GetCurrentPosition(in MemWriter memWriter) => memWriter.Current - memWriter.Start;

            public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
            {
                lock (file._lock)
                {
                    file._length = memWriter.Current - memWriter.Start;
                    file.EnsureSpace(file._length + (long)length);
                    Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(file._pointer + file._length), ref buffer, (uint)length);
                    file._length += (long)length;
                }
                Init(ref memWriter);
            }

            public void SetCurrentPosition(ref MemWriter memWriter, long position) => throw new NotSupportedException();
        }
    }
}
