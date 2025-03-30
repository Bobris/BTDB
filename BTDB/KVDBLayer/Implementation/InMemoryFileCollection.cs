using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

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

    public void SimulateDataLossOfNotFlushedData()
    {
        foreach (var filesValue in _files.Values)
        {
            filesValue.SimulateDataLossOfNotFushedData();
        }
    }

    class File : IFileCollectionFile
    {
        readonly InMemoryFileCollection _owner;
        readonly uint _index;
        byte[][] _data = Array.Empty<byte[]>();
        readonly Writer _writer;
        long _flushedSize;
        long _lastTrueFlushedSize;
        const int OneBufSize = 128 * 1024;

        public File(InMemoryFileCollection owner, uint index)
        {
            _owner = owner;
            _index = index;
            _writer = new Writer(this);
        }

        internal void SimulateDataLossOfNotFushedData()
        {
            _flushedSize = _lastTrueFlushedSize;
            _writer.SimulateCorruptionBySetSize((int)_flushedSize);
        }

        public uint Index => _index;

        sealed class Reader : IMemReader
        {
            readonly File _file;
            ulong _ofs;
            uint _pos;
            readonly ulong _totalSize;

            public Reader(File file)
            {
                _file = file;
                _totalSize = file.GetSize();
                _ofs = 0;
                _pos = 0;
            }

            public unsafe void Init(ref MemReader reader)
            {
                _ofs += _pos;
                _pos = 0;
                var bufOfs = (int)(_ofs % OneBufSize);
                reader.Start = (nint)Unsafe.AsPointer(ref _file._data[(int)(_ofs / OneBufSize)][0]);
                reader.Current = reader.Start + bufOfs;
                reader.End = reader.Current + (int)Math.Min(_totalSize - _ofs, (ulong)(OneBufSize - bufOfs));
            }

            public void FillBuf(ref MemReader memReader, nuint advisePrefetchLength)
            {
                _pos = (uint)(memReader.Current - memReader.Start);
                if (memReader.Current == memReader.End)
                {
                    Init(ref memReader);
                    if (memReader.Current == memReader.End) PackUnpack.ThrowEndOfStreamException();
                }
            }

            public long GetCurrentPosition(in MemReader memReader)
            {
                return (long)_ofs + (memReader.Current - memReader.Start);
            }

            public unsafe void ReadBlock(ref MemReader memReader, ref byte buffer, nuint length)
            {
                while (length > 0)
                {
                    FillBuf(ref memReader, 1);
                    var lenTillEnd = (int)Math.Min((nint)length, memReader.End - memReader.Current);
                    Unsafe.CopyBlockUnaligned(ref buffer,
                        ref Unsafe.AsRef<byte>((void*)memReader.Current), (uint)lenTillEnd);
                    buffer = ref Unsafe.AddByteOffset(ref buffer, lenTillEnd);
                    memReader.Current += lenTillEnd;
                    length -= (nuint)lenTillEnd;
                }
            }

            public void SkipBlock(ref MemReader memReader, nuint length)
            {
                Debug.Assert(memReader.Current == memReader.End);
                _ofs += (uint)(memReader.Current - memReader.Start) + length;
                _pos = 0;
                if (_ofs <= _totalSize)
                {
                    memReader.Start = 0;
                    memReader.Current = 0;
                    memReader.End = 0;
                    return;
                }

                _ofs = _totalSize;
                PackUnpack.ThrowEndOfStreamException();
            }

            public void SetCurrentPosition(ref MemReader memReader, long position)
            {
                ArgumentOutOfRangeException.ThrowIfNegative(position);
                _ofs = (ulong)position;
                _pos = 0;
                memReader.Start = 0;
                memReader.Current = 0;
                memReader.End = 0;
            }

            public bool Eof(ref MemReader memReader)
            {
                if (memReader.Current < memReader.End) return false;
                _pos = (uint)(memReader.Current - memReader.Start);
                return _ofs + _pos >= _totalSize;
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
            var storage = Volatile.Read(ref _data);
            while (!data.IsEmpty)
            {
                var buf = storage[(int)(position / OneBufSize)];
                var bufOfs = (int)(position % OneBufSize);
                var copy = Math.Min(data.Length, OneBufSize - bufOfs);
                buf.AsSpan(bufOfs, copy).CopyTo(data);
                data = data[copy..];
                position += (ulong)copy;
            }
        }

        sealed class Writer : IMemWriter
        {
            readonly File _file;
            ulong _ofs;

            public Writer(File file)
            {
                _file = file;
                _ofs = 0;
            }

            internal void SimulateCorruptionBySetSize(int size)
            {
                if (size > OneBufSize || _ofs > OneBufSize) throw new ArgumentOutOfRangeException();
                Array.Clear(_file._data[0], size, (int)_ofs - size);
                _ofs = (uint)size;
            }

            public unsafe void Init(ref MemWriter memWriter)
            {
                var ofs = _ofs;
                if (ofs == (ulong)_file._data.Length * OneBufSize)
                {
                    memWriter.Start = 0;
                    memWriter.Current = 0;
                    memWriter.End = 0;
                    return;
                }

                var ofsbuf = (int)(ofs % OneBufSize);
                memWriter.Start = (nint)Unsafe.AsPointer(ref _file._data[^1][0]) + ofsbuf;
                memWriter.Current = memWriter.Start;
                memWriter.End = memWriter.Start + OneBufSize - ofsbuf;
            }

            public unsafe void Flush(ref MemWriter memWriter, uint spaceNeeded)
            {
                _ofs += (ulong)(memWriter.Current - memWriter.Start);
                Interlocked.MemoryBarrier(); // all written data will be visible before publishing new _flushedSize
                _file._flushedSize = (long)_ofs;
                if (_ofs == (ulong)_file._data.Length * OneBufSize)
                {
                    var buf = GC.AllocateUninitializedArray<byte>(OneBufSize, pinned: true);
                    var storage = _file._data;
                    Array.Resize(ref storage, storage.Length + 1);
                    storage[^1] = buf;
                    Volatile.Write(ref _file._data, storage);
                    memWriter.Start = (nint)Unsafe.AsPointer(ref buf[0]);
                    memWriter.Current = memWriter.Start;
                    memWriter.End = memWriter.Start + OneBufSize;
                    return;
                }

                var ofs = (int)(_ofs % OneBufSize);
                memWriter.Start = (nint)Unsafe.AsPointer(ref _file._data[^1][0]) + ofs;
                memWriter.Current = memWriter.Start;
                memWriter.End = memWriter.Start + OneBufSize - ofs;
            }

            public long GetCurrentPosition(in MemWriter memWriter)
            {
                return (long)_ofs + (memWriter.Current - memWriter.Start);
            }

            public unsafe void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
            {
                while (length > 0)
                {
                    if (memWriter.Current == memWriter.End)
                    {
                        Flush(ref memWriter, 1);
                    }

                    var l = (uint)Math.Min((uint)(memWriter.End - memWriter.Current), length);
                    Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>((void*)memWriter.Current),
                        ref buffer, l);
                    memWriter.Current += (nint)l;
                    buffer = ref Unsafe.AddByteOffset(ref buffer, (nint)l);
                    length -= l;
                }
            }

            public void SetCurrentPosition(ref MemWriter memWriter, long position)
            {
                throw new NotSupportedException();
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
            // We are only in memory, so nothing to do
        }

        public void Truncate()
        {
            _lastTrueFlushedSize = _flushedSize;
        }

        public void HardFlushTruncateSwitchToReadOnlyMode()
        {
            // We are only in memory, so nothing to do, only remember last true flushed size to be able to simulate data loss
            _lastTrueFlushedSize = _flushedSize;
        }

        public void HardFlushTruncateSwitchToDisposedMode()
        {
            // We are only in memory, so nothing to do, only remember last true flushed size to be able to simulate data loss
            _lastTrueFlushedSize = _flushedSize;
        }

        public ulong GetSize()
        {
            Volatile.Read(ref _data);
            return (ulong)_flushedSize;
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
    }

    internal void SimulateCorruptionBySetSize(int size)
    {
        _files[1].SimulateCorruptionBySetSize(size);
    }
}
