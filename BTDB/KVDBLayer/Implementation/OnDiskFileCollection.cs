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
            _handle.Dispose();
            _stream.Dispose();
        }

        public uint Index => _index;

        sealed class Reader : IMemReader
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
                _buf = GC.AllocateUninitializedArray<byte>(BufLength, pinned: true);
                _usedOfs = 0;
                _usedLen = 0;
            }

            public unsafe void Init(ref MemReader reader)
            {
                if (_usedLen == 0)
                {
                    var read = RandomAccess.Read(_owner._handle, _buf.AsSpan(), (long)_ofs);
                    reader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
                    reader.Current = reader.Start;
                    reader.End = reader.Start + read;
                    _usedOfs = 0;
                    _usedLen = (uint)read;
                    _ofs += (uint)read;
                    return;
                }

                reader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
                reader.Current = reader.Start + (int)_usedOfs;
                reader.End = reader.Current + (int)_usedLen;
            }

            public unsafe void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
            {
                var curLen = (uint)(memReader.End - memReader.Current);
                _usedOfs += _usedLen - curLen;
                _usedLen = curLen;
                if (advisePrefetchLength == 0) return;
                if (memReader.Current < memReader.End) return;
                var read = RandomAccess.Read(_owner._handle, _buf.AsSpan(), (long)_ofs);
                memReader.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
                memReader.Current = memReader.Start;
                memReader.End = memReader.Start + read;
                _usedOfs = 0;
                _usedLen = (uint)read;
                _ofs += (uint)read;
                if (read == 0) PackUnpack.ThrowEndOfStreamException();
            }

            public long GetCurrentPosition(in MemReader memReader)
            {
                return (long)_ofs - (memReader.End - memReader.Current);
            }

            public unsafe void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
            {
                if (length < BufLength)
                {
                    FillBuf(ref memReader, length);
                    if ((nint)length > memReader.End - memReader.Current) PackUnpack.ThrowEndOfStreamException();
                    Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((void*)memReader.Current),
                        (uint)length);
                    memReader.Current += (nint)length;
                    return;
                }

                var read = RandomAccess.Read(_owner._handle,
                    MemoryMarshal.CreateSpan(ref buffer, (int)length), (long)_ofs);
                _ofs += (uint)read;
                if (read < (nint)length) PackUnpack.ThrowEndOfStreamException();
            }

            public void SkipBlock(ref MemReader memReader, nuint length)
            {
                _ofs += length;
                _usedLen = 0;
                if (_ofs <= _valueSize) return;
                _ofs = _valueSize;
                PackUnpack.ThrowEndOfStreamException();
            }

            public void SetCurrentPosition(ref MemReader memReader, long position)
            {
                ArgumentOutOfRangeException.ThrowIfNegative(position);
                memReader.Start = 0;
                memReader.Current = 0;
                memReader.End = 0;
                _usedOfs = 0;
                _usedLen = 0;
                _ofs = (ulong)position;
            }

            public bool Eof(ref MemReader memReader)
            {
                return GetCurrentPosition(memReader) == (long)_valueSize;
            }
        }

        sealed class Writer : IMemWriter
        {
            readonly File _file;
            internal ulong Ofs;
            readonly byte[] _buf;
            const int BufLength = 32768;

            public Writer(File file)
            {
                _file = file;
                _buf = GC.AllocateUninitializedArray<byte>(BufLength, pinned: true);
                using (_file._readerWriterLock.WriteLock())
                {
                    Ofs = (ulong)_file._stream.Length;
                }
            }

            public void FlushBuffer(int pos)
            {
                if (pos == 0) return;
                RandomAccess.Write(_file._handle, _buf.AsSpan(0, pos), (long)Ofs);
                using (_file._readerWriterLock.WriteLock())
                {
                    Ofs += (ulong)pos;
                }
            }

            internal byte[] GetBuffer()
            {
                return _buf;
            }

            public unsafe void Init(ref MemWriter memWriter)
            {
                memWriter.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
                memWriter.Current = memWriter.Start;
                memWriter.End = memWriter.Start + BufLength;
            }

            public unsafe void Flush(ref MemWriter memWriter, uint spaceNeeded)
            {
                FlushBuffer((int)(memWriter.Current - memWriter.Start));
                memWriter.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buf));
                memWriter.Current = memWriter.Start;
                memWriter.End = memWriter.Start + BufLength;
            }

            public long GetCurrentPosition(in MemWriter memWriter)
            {
                return (long)Ofs + (memWriter.Current - memWriter.Start);
            }

            public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
            {
                Flush(ref memWriter, 0);
                RandomAccess.Write(_file._handle, MemoryMarshal.CreateReadOnlySpan(ref buffer, (int)length),
                    (long)Ofs);
                using (_file._readerWriterLock.WriteLock())
                {
                    Ofs += length;
                }
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
                _writer.GetBuffer().AsSpan((int)(position - _writer.Ofs), data.Length).CopyTo(data);
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
