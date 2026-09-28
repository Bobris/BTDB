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

/// Durable node-local replication storage: one "{id:D8}.{hint}" file per ID in a directory. A file being appended
/// keeps its unmapped tail in pinned 1 MiB blocks and its persisted prefix in a read-only mapping that grows in
/// 8–64 MiB steps; the single appender writes every completed block to disk before moving on. Readers copy from an
/// immutable snapshot of mapping and blocks without a lock: a mapping stays alive until a copy that started before
/// its replacement or Remove finishes, and a released block returns to a small pool only after its generation
/// changes, so a reader that copied from a reused block copies again. Sealing (switch to read-only) persists and
/// truncates the file without an fsync and maps it whole; files that are never appended are mapped on first read. After a
/// process crash a file may lack its last unflushed block: replication validates cached files against the remote
/// inventory and discards local-only or active files before opening a database, so a local tail never becomes
/// history. Memory use is up to one mapping step plus the current block per file being appended.
public sealed class OnDiskReplicationFileStorage : IReplicationFileStorage
{
    const int BlockSize = 1024 * 1024;
    // Released blocks kept for reuse: without them every appended MiB is a pinned allocation and gen2 pressure.
    const int MaximumPooledBlocks = 64;
    readonly Stack<Block> _freeBlocks = new();
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

    Block RentBlock()
    {
        lock (_freeBlocks) if (_freeBlocks.TryPop(out var block)) return block;
        return new();
    }

    // The generation changes before a block can be reused, so a reader still copying from it retries.
    void ReturnBlock(Block block)
    {
        Interlocked.Increment(ref block.Generation);
        lock (_freeBlocks) if (_freeBlocks.Count < MaximumPooledBlocks) _freeBlocks.Push(block);
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

    sealed unsafe class Mapping : IDisposable
    {
        readonly MemoryMappedFile _file;
        readonly MemoryMappedViewAccessor _view;

        // Maps the first length bytes; the section spans the whole current file, which may already be longer.
        public Mapping(FileStream stream, long length)
        {
            _file = MemoryMappedFile.CreateFromFile(stream, null, 0, MemoryMappedFileAccess.Read,
                HandleInheritability.None, true);
            _view = _file.CreateViewAccessor(0, length, MemoryMappedFileAccess.Read);
        }

        // The pointer reference defers an unmap by a concurrent replacement or Remove until this copy finishes;
        // a mapping already disposed throws ObjectDisposedException.
        public void Read(Span<byte> destination, ulong position)
        {
            var handle = _view.SafeMemoryMappedViewHandle;
            byte* pointer = null;
            handle.AcquirePointer(ref pointer);
            try { new ReadOnlySpan<byte>(pointer + _view.PointerOffset + (long)position, destination.Length).CopyTo(destination); }
            finally { handle.ReleasePointer(); }
        }

        public void Dispose()
        {
            _view.Dispose();
            _file.Dispose();
        }
    }

    // A pinned buffer of one block. Its generation changes before reuse, so a reader that copied from it under an
    // older snapshot detects that the bytes may belong to another position.
    sealed class Block
    {
        public readonly byte[] Bytes = GC.AllocateUninitializedArray<byte>(BlockSize, pinned: true);
        public int Generation;
    }

    readonly record struct BlockRef(Block? Block, int Generation);

    // What readers copy from, replaced as a whole. Every byte below the file length lies below Mapped or in a
    // non-null block; a snapshot is published before a length that needs it.
    sealed class Content(Mapping? mapping, long mapped, BlockRef[] blocks)
    {
        public static readonly Content Empty = new(null, 0, []);
        public readonly Mapping? Mapping = mapping;
        public readonly long Mapped = mapped;
        public readonly BlockRef[] Blocks = blocks;
    }

    sealed unsafe class File : IFileCollectionFile
    {
        // While appending, persisted blocks move under a longer read-only mapping in steps of 8 MiB to 64 MiB, so
        // memory stays bounded and remaps stay rare.
        const long MinimumMappingStep = 8L * 1024 * 1024;
        const long MaximumMappingStep = 64L * 1024 * 1024;
        readonly OnDiskReplicationFileStorage _owner;
        readonly string _path;
        readonly FileStream _stream;
        // Serializes the appender's persistence and remapping with sealing, removal, disposal and lazy mapping;
        // readers never take it.
        readonly object _lock = new();
        readonly Writer _writer;
        Content _content = Content.Empty;
        long _length;
        long _persisted;
        volatile bool _removed;
        bool _readOnly;

        public File(OnDiskReplicationFileStorage owner, uint index, string path, string humanHint)
        {
            _owner = owner;
            Index = index;
            _path = path;
            FileType = FileCollectionWithFileInfos.FileTypeFromHint(humanHint);
            _stream = new(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 1, FileOptions.None);
            _length = _persisted = _stream.Length;
            _writer = new(this);
        }

        public uint Index { get; }
        internal KVFileType? FileType { get; }

        void ThrowIfRemoved()
        {
            if (_removed) throw new FileNotFoundException("The replication file was removed.", _path);
        }

        // Caller holds _lock. Replaces the snapshot and releases what it no longer references: the previous mapping
        // (readers still copying keep it) and dropped blocks (readers still copying retry).
        void Publish(Content content)
        {
            var previous = _content;
            Volatile.Write(ref _content, content);
            if (previous.Mapping != null && !ReferenceEquals(previous.Mapping, content.Mapping)) previous.Mapping.Dispose();
            for (var i = 0; i < previous.Blocks.Length; i++)
                if (previous.Blocks[i].Block is { } block &&
                    (i >= content.Blocks.Length || !ReferenceEquals(content.Blocks[i].Block, block)))
                    _owner.ReturnBlock(block);
        }

        // Caller holds _lock. Writes appended bytes below upTo that are not on disk yet.
        void Persist(long upTo)
        {
            var blocks = _content.Blocks;
            while (_persisted < upTo)
            {
                var block = (int)(_persisted / BlockSize);
                var start = (int)(_persisted % BlockSize);
                var count = (int)Math.Min(BlockSize - start, upTo - _persisted);
                RandomAccess.Write(_stream.SafeFileHandle, blocks[block].Block!.Bytes.AsSpan(start, count), _persisted);
                _persisted += count;
            }
        }

        // Caller holds _lock. Only a flush of the whole logical length may drop bytes beyond it on disk.
        void PersistAndTruncate()
        {
            Persist(_length);
            if (_stream.Length != _length) _stream.SetLength(_length);
        }

        // Caller holds _lock. Maps every persisted byte and drops the blocks it covers, keeping the block from
        // keepFrom on: the appender writes into it through a raw pointer.
        void MapPersisted(long keepFrom)
        {
            var content = _content;
            if (_persisted <= content.Mapped) return;
            var mapping = new Mapping(_stream, _persisted);
            var blocks = (BlockRef[])content.Blocks.Clone();
            var keep = (int)Math.Min(keepFrom / BlockSize, _persisted / BlockSize);
            for (var i = 0; i < Math.Min(keep, blocks.Length); i++) blocks[i] = default;
            Publish(new(mapping, _persisted, blocks));
        }

        // Caller holds _lock; appender only. The block holding position, loaded with its persisted bytes if needed.
        byte[] EnsureBlock(long position)
        {
            var content = _content;
            var index = (int)(position / BlockSize);
            // Persisted bytes before this block must stay readable: map them unless blocks already hold them.
            if (!Covered(content, Math.Min(_persisted, (long)index * BlockSize)))
            {
                MapPersisted(position);
                content = _content;
            }
            if (index < content.Blocks.Length && content.Blocks[index].Block is { } existing) return existing.Bytes;
            var block = _owner.RentBlock();
            var blockStart = (long)index * BlockSize;
            var onDisk = (int)Math.Clamp(_persisted - blockStart, 0, BlockSize);
            for (var read = 0; read < onDisk;)
            {
                var chunk = RandomAccess.Read(_stream.SafeFileHandle, block.Bytes.AsSpan(read, onDisk - read), blockStart + read);
                if (chunk == 0) throw new EndOfStreamException();
                read += chunk;
            }
            var blocks = new BlockRef[Math.Max(content.Blocks.Length, index + 1)];
            content.Blocks.CopyTo(blocks, 0);
            blocks[index] = new(block, Volatile.Read(ref block.Generation));
            Publish(new(content.Mapping, content.Mapped, blocks));
            return block.Bytes;
        }

        // Whether every byte below upTo lies in the mapping or in a block.
        static bool Covered(Content content, long upTo)
        {
            for (var offset = content.Mapped; offset < upTo; offset = (offset / BlockSize + 1) * BlockSize)
            {
                var index = (int)(offset / BlockSize);
                if (index >= content.Blocks.Length || content.Blocks[index].Block == null) return false;
            }
            return true;
        }

        // A file that nobody appends to is read through one mapping of its persisted bytes.
        void MapForReading()
        {
            lock (_lock)
            {
                ThrowIfRemoved();
                if (_persisted <= _content.Mapped)
                    throw new InvalidOperationException("Replication file content is neither mapped nor buffered.");
                MapPersisted(long.MaxValue);
            }
        }

        internal void Dispose()
        {
            lock (_lock)
            {
                if (_removed) return;
                PersistAndTruncate();
                _removed = true;
                Publish(Content.Empty);
                _stream.Dispose();
            }
        }

        public IMemReader GetExclusiveReader() => new Reader(this);

        public void AdvisePrefetch()
        {
        }

        public void RandomRead(Span<byte> data, ulong position, bool doNotCache)
        {
            ThrowIfRemoved();
            var length = (ulong)Volatile.Read(ref _length);
            if (position > length || (ulong)data.Length > length - position) throw new EndOfStreamException();
            while (!data.IsEmpty)
            {
                var content = Volatile.Read(ref _content);
                if (position < (ulong)content.Mapped)
                {
                    var count = (int)Math.Min((ulong)data.Length, (ulong)content.Mapped - position);
                    try { content.Mapping!.Read(data[..count], position); }
                    catch (ObjectDisposedException)
                    {
                        // Replaced by a longer mapping or removed: read the current snapshot.
                        ThrowIfRemoved();
                        continue;
                    }
                    data = data[count..];
                    position += (ulong)count;
                    continue;
                }
                var index = (int)(position / BlockSize);
                if (index >= content.Blocks.Length || content.Blocks[index] is not { Block: { } block } reference)
                {
                    // Only a file nobody appends to lacks both; map its persisted bytes once.
                    MapForReading();
                    continue;
                }
                var start = (int)(position % BlockSize);
                var copied = Math.Min(BlockSize - start, data.Length);
                block.Bytes.AsSpan(start, copied).CopyTo(data);
                // The copy must complete before the generation check, also on weakly ordered CPUs.
                Interlocked.MemoryBarrier();
                if (Volatile.Read(ref block.Generation) != reference.Generation) continue; // Reused meanwhile: copy again.
                data = data[copied..];
                position += (ulong)copied;
            }
        }

        public IMemWriter GetAppenderWriter() => _writer;
        public IMemWriter GetExclusiveAppenderWriter() => _writer;

        public void HardFlush()
        {
            lock (_lock)
            {
                if (_removed) return;
                PersistAndTruncate();
                _stream.Flush(true);
                // Keep only the appender's current block in memory.
                MapPersisted(_length);
            }
        }

        // No fsync: sealing happens on the commit path at every TRL rotation, and replication never trusts local bytes
        // it has not validated against the remote inventory, so waiting for the device would only stall commits.
        public void HardFlushTruncateSwitchToReadOnlyMode()
        {
            lock (_lock)
            {
                if (_removed) return;
                _readOnly = true;
                PersistAndTruncate();
                if (_content.Blocks.Length == 0 && _content.Mapped == 0) return; // Mapped on first read.
                Publish(_length == 0 ? Content.Empty : new(_content.Mapped == _length ? _content.Mapping : new Mapping(_stream, _length), _length, []));
            }
        }

        public void HardFlushTruncateSwitchToDisposedMode() => HardFlushTruncateSwitchToReadOnlyMode();

        public ulong GetSize() => (ulong)Volatile.Read(ref _length);

        public void Remove()
        {
            _owner.Forget(this);
            lock (_lock)
            {
                if (_removed) return;
                _removed = true;
                Publish(Content.Empty);
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

        // The single appender writes straight into the current block between calls; each call publishes the logical
        // length. A completed block goes to disk before the appender moves on, and persisted blocks periodically
        // move under a longer mapping.
        sealed class Writer(File file) : IMemWriter
        {
            // File position of memWriter.Start.
            long _base;

            public void Init(ref MemWriter memWriter)
            {
                lock (file._lock)
                {
                    if (file._readOnly) throw new InvalidOperationException("The replication file is read-only.");
                    file.ThrowIfRemoved();
                    _base = file._length;
                    var block = file.EnsureBlock(_base);
                    var start = (int)(_base % BlockSize);
                    memWriter.Start = (nint)Unsafe.AsPointer(ref block[start]);
                    memWriter.Current = memWriter.Start;
                    memWriter.End = memWriter.Start + (BlockSize - start);
                }
            }

            // Publish the bytes written since Start; move to the next block when this one is full.
            void Publish(ref MemWriter memWriter)
            {
                var length = _base + (memWriter.Current - memWriter.Start);
                Volatile.Write(ref file._length, length);
                if (memWriter.Current != memWriter.End)
                {
                    _base = length;
                    memWriter.Start = memWriter.Current;
                    return;
                }
                lock (file._lock)
                {
                    file.Persist(length);
                    var mapped = file._content.Mapped;
                    if (file._persisted - mapped >= Math.Clamp(mapped, MinimumMappingStep, MaximumMappingStep))
                        file.MapPersisted(length);
                }
                Init(ref memWriter);
            }

            public void Flush(ref MemWriter memWriter, uint spaceNeeded) => Publish(ref memWriter);

            public long GetCurrentPosition(in MemWriter memWriter) => _base + (memWriter.Current - memWriter.Start);

            public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
            {
                while (length > 0)
                {
                    if (memWriter.Current == memWriter.End) Publish(ref memWriter);
                    var count = (uint)Math.Min((nuint)(memWriter.End - memWriter.Current), length);
                    Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>((void*)memWriter.Current), ref buffer, count);
                    memWriter.Current += (nint)count;
                    buffer = ref Unsafe.AddByteOffset(ref buffer, count);
                    length -= count;
                }
                Publish(ref memWriter);
            }

            public void SetCurrentPosition(ref MemWriter memWriter, long position)
            {
                lock (file._lock)
                {
                    if (position < 0 || position > file._length) throw new ArgumentOutOfRangeException(nameof(position));
                    Volatile.Write(ref file._length, position);
                    file._persisted = Math.Min(file._persisted, position);
                    // Mapped bytes at or after the new end will be rewritten through blocks.
                    var content = file._content;
                    if (content.Mapped > position)
                        file.Publish(new(position == 0 ? null : new Mapping(file._stream, position), position, content.Blocks));
                }
                Init(ref memWriter);
            }
        }
    }
}
