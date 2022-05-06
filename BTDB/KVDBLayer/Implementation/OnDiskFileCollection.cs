using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.Buffer;
using BTDB.StreamLayer;
using Microsoft.Win32.SafeHandles;

namespace BTDB.KVDBLayer;

public class OnDiskFileCollection : IFileCollection
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
    readonly FileAccess _fileAccess;
    IDeleteFileCollectionStrategy? _deleteFileCollectionStrategy;

    sealed class File : IFileCollectionFile
    {
        readonly OnDiskFileCollection _owner;
        readonly uint _index;
        readonly string _fileName;
        readonly FileStream _stream;
        readonly SafeFileHandle _handle;
        readonly Writer _writer;
        readonly ReaderWriterLockSlim _readerWriterLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        public File(OnDiskFileCollection owner, uint index, string fileName, FileAccess fileAccess)
        {
            _owner = owner;
            _index = index;
            _fileName = fileName;
            _stream = new FileStream(fileName, FileMode.OpenOrCreate, fileAccess, FileShare.Read, 1,
                FileOptions.None);
            _handle = _stream.SafeFileHandle!;
            _writer = new Writer(this);
        }

        internal void Dispose()
        {
            _writer.FlushBuffer();
            _handle.Dispose();
            _stream.Dispose();
        }

        public uint Index => _index;

        sealed class Reader : ISpanReader
        {
            readonly File _owner;
            readonly ulong _valueSize;
            ulong _ofs;
            readonly byte[] _buf;
            uint _usedOfs;
            uint _usedLen;
            const int BufLength = 32768;

            public Reader(File owner)
            {
                _owner = owner;
                _valueSize = _owner.GetSize();
                _ofs = 0;
                _buf = new byte[BufLength];
                _usedOfs = 0;
                _usedLen = 0;
            }

            public void Init(ref SpanReader spanReader)
            {
                if (_usedLen == 0)
                {
                    var read = RandomAccess.Read(_owner._handle, _buf.AsSpan(), (long)_ofs);
                    spanReader.Buf = _buf.AsSpan(0, read);
                    _usedOfs = 0;
                    _usedLen = (uint)read;
                    _ofs += (uint)read;
                    return;
                }
                spanReader.Buf = _buf.AsSpan((int)_usedOfs, (int)_usedLen);
            }

            public bool FillBufAndCheckForEof(ref SpanReader spanReader)
            {
                if (0 != spanReader.Buf.Length)
                    return false;
                var read = RandomAccess.Read(_owner._handle, _buf.AsSpan(), (long)_ofs);
                spanReader.Buf = _buf.AsSpan(0, read);
                _usedOfs = 0;
                _usedLen = (uint)read;
                _ofs += (uint)read;
                return read == 0;
            }

            public long GetCurrentPosition(in SpanReader spanReader)
            {
                return (long)_ofs - spanReader.Buf.Length;
            }

            public bool ReadBlock(ref SpanReader spanReader, ref byte buffer, uint length)
            {
                if (length < BufLength)
                {
                    if (FillBufAndCheckForEof(ref spanReader) || length > (uint)spanReader.Buf.Length) return true;
                    Unsafe.CopyBlockUnaligned(ref buffer,
                        ref PackUnpack.UnsafeGetAndAdvance(ref spanReader.Buf, (int)length), length);
                    return false;
                }

                var read = RandomAccess.Read(_owner._handle,
                    MemoryMarshal.CreateSpan(ref buffer, (int)length), (long)_ofs);
                _ofs += (uint)read;
                return read < length;
            }

            public bool SkipBlock(ref SpanReader spanReader, uint length)
            {
                _ofs += length;
                _usedLen = 0;
                if (_ofs <= _valueSize) return false;
                _ofs = _valueSize;
                return true;
            }

            public void SetCurrentPosition(ref SpanReader spanReader, long position)
            {
                spanReader.Buf = new ReadOnlySpan<byte>();
                _usedOfs = 0;
                _usedLen = 0;
                _ofs = (ulong)position;
            }

            public void Sync(ref SpanReader spanReader)
            {
                var curLen = (uint)spanReader.Buf.Length;
                _usedOfs += _usedLen - curLen;
                _usedLen = curLen;
            }
        }

        sealed class Writer : ISpanWriter
        {
            readonly File _file;
            internal ulong Ofs;
            readonly byte[] _buf;
            int _pos;
            const int BufLength = 32768;

            public Writer(File file)
            {
                _file = file;
                _buf = new byte[BufLength];
                _pos = 0;
                using (_file._readerWriterLock.WriteLock())
                {
                    Ofs = (ulong)_file._stream.Length;
                }
            }

            public void FlushBuffer()
            {
                if (_pos == 0) return;
                RandomAccess.Write(_file._handle, _buf.AsSpan(0, _pos), (long)Ofs);
                using (_file._readerWriterLock.WriteLock())
                {
                    Ofs += (ulong)_pos;
                    _pos = 0;
                }
            }

            internal byte[] GetBuffer()
            {
                return _buf;
            }

            public void Init(ref SpanWriter spanWriter)
            {
                spanWriter.Buf = _buf.AsSpan(_pos);
                spanWriter.HeapBuffer = _buf;
            }

            public void Sync(ref SpanWriter spanWriter)
            {
                _pos = BufLength - spanWriter.Buf.Length;
            }

            public bool Flush(ref SpanWriter spanWriter)
            {
                _pos = BufLength - spanWriter.Buf.Length;
                FlushBuffer();
                spanWriter.Buf = _buf;
                return true;
            }

            public long GetCurrentPosition(in SpanWriter spanWriter)
            {
                return (long)Ofs + BufLength - spanWriter.Buf.Length;
            }

            public long GetCurrentPositionWithoutWriter()
            {
                return (long)Ofs + _pos;
            }

            public void WriteBlock(ref SpanWriter spanWriter, ref byte buffer, uint length)
            {
                RandomAccess.Write(_file._handle, MemoryMarshal.CreateReadOnlySpan(ref buffer, (int)length),
                    (long)Ofs);
                using (_file._readerWriterLock.WriteLock())
                {
                    Ofs += length;
                }
            }

            public void WriteBlockWithoutWriter(ref byte buffer, uint length)
            {
                if (length <= (uint)(BufLength - _pos))
                {
                    Unsafe.CopyBlockUnaligned(ref MemoryMarshal.GetReference(_buf.AsSpan(_pos, (int)length)), ref buffer,
                        length);
                    _pos += (int)length;
                }
                else
                {
                    var writer = new SpanWriter(this);
                    writer.WriteBlock(ref buffer, length);
                    writer.Sync();
                }
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
            using (_readerWriterLock.ReadLock())
            {
                if (data.Length > 0 && position < _writer.Ofs)
                {
                    var read = data.Length;
                    if (_writer.Ofs - position < (ulong)read) read = (int)(_writer.Ofs - position);
                    if (RandomAccess.Read(_handle, data[..read], (long)position) != read)
                        throw new EndOfStreamException();
                    data = data[read..];
                    position += (ulong)read;
                }

                if (data.Length == 0) return;
                if ((ulong)_writer.GetCurrentPositionWithoutWriter() < position + (ulong)data.Length)
                    throw new EndOfStreamException();
                _writer.GetBuffer().AsSpan((int)(position - _writer.Ofs), data.Length).CopyTo(data);
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
            _writer.FlushBuffer();
            _stream.Flush(true);
        }

        public void SetSize(long size)
        {
        }

        public void Truncate()
        {
        }

        public void HardFlushTruncateSwitchToReadOnlyMode()
        {
            HardFlush();
        }

        public void HardFlushTruncateSwitchToDisposedMode()
        {
            HardFlush();
        }

        public ulong GetSize()
        {
            using (_readerWriterLock.ReadLock())
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

            _stream.Dispose();
            _owner.DeleteFileCollectionStrategy.DeleteFile(_fileName);
        }
    }

    public OnDiskFileCollection(string directory, FileAccess fileAccess = FileAccess.ReadWrite)
    {
        _directory = directory;
        _fileAccess = fileAccess;
        _maxFileId = 0;
        foreach (var filePath in Directory.EnumerateFiles(directory))
        {
            var id = GetFileId(Path.GetFileNameWithoutExtension(filePath));
            if (id == 0) continue;
            var file = new File(this, id, filePath, _fileAccess);
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
        var file = new File(this, index, Path.Combine(_directory, fileName), _fileAccess);
        Dictionary<uint, File> newFiles;
        Dictionary<uint, File> oldFiles;
        do
        {
            oldFiles = _files;
            newFiles = new Dictionary<uint, File>(oldFiles!) { { index, file } };
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
