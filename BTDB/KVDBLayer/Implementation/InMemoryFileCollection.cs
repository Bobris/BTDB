using System;
using System.Collections.Generic;
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

        sealed class Reader : ISpanReader
        {
            readonly File _file;
            ulong _ofs;
            readonly ulong _totalSize;

            public Reader(File file)
            {
                _file = file;
                _totalSize = file.GetSize();
                _ofs = 0;
            }

            public void Init(ref SpanReader spanReader)
            {
                var bufOfs = (int)(_ofs % OneBufSize);
                spanReader.Buf = _file._data[(int)(_ofs / OneBufSize)]
                    .AsSpan(bufOfs, (int)Math.Min(_totalSize - _ofs, (ulong)(OneBufSize - bufOfs)));
                _ofs += (uint)spanReader.Buf.Length;
            }

            public bool FillBufAndCheckForEof(ref SpanReader spanReader)
            {
                if (spanReader.Buf.Length != 0) return false;
                Init(ref spanReader);
                return 0 == spanReader.Buf.Length;
            }

            public long GetCurrentPosition(in SpanReader spanReader)
            {
                return (long)_ofs - spanReader.Buf.Length;
            }

            public bool ReadBlock(ref SpanReader spanReader, ref byte buffer, uint length)
            {
                while (length > 0)
                {
                    if (FillBufAndCheckForEof(ref spanReader)) return true;
                    var lenTillEnd = (uint)Math.Min(length, spanReader.Buf.Length);
                    Unsafe.CopyBlockUnaligned(ref buffer,
                        ref PackUnpack.UnsafeGetAndAdvance(ref spanReader.Buf, (int)lenTillEnd), lenTillEnd);
                    length -= lenTillEnd;
                }

                return false;
            }

            public bool SkipBlock(ref SpanReader spanReader, uint length)
            {
                _ofs += length;
                if (_ofs <= _totalSize) return false;
                _ofs = _totalSize;
                return true;
            }

            public void SetCurrentPosition(ref SpanReader spanReader, long position)
            {
                _ofs = (ulong)position;
                spanReader.Buf = new ReadOnlySpan<byte>();
            }

            public void Sync(ref SpanReader spanReader)
            {
                _ofs -= (uint)spanReader.Buf.Length;
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
            var storage = Volatile.Read(ref _data);
            while (!data.IsEmpty)
            {
                var buf = storage[(int)(position / OneBufSize)];
                var bufOfs = (int)(position % OneBufSize);
                var copy = Math.Min(data.Length, OneBufSize - bufOfs);
                buf.AsSpan(bufOfs, copy).CopyTo(data);
                data = data.Slice(copy);
                position += (ulong)copy;
            }
        }

        sealed class Writer : ISpanWriter
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

            public void Init(ref SpanWriter spanWriter)
            {
                if (_ofs == (ulong)_file._data.Length * OneBufSize)
                {
                    var buf = new byte[OneBufSize];
                    var storage = _file._data;
                    Array.Resize(ref storage, storage.Length + 1);
                    storage[^1] = buf;
                    Volatile.Write(ref _file._data, storage);
                    spanWriter.InitialBuffer = buf;
                    spanWriter.Buf = spanWriter.InitialBuffer;
                    return;
                }

                spanWriter.InitialBuffer = _file._data[^1].AsSpan((int)(_ofs % OneBufSize));
                spanWriter.Buf = spanWriter.InitialBuffer;
            }

            public void Sync(ref SpanWriter spanWriter)
            {
                _ofs += (ulong)(spanWriter.InitialBuffer.Length - spanWriter.Buf.Length);
            }

            public bool Flush(ref SpanWriter spanWriter)
            {
                if (spanWriter.Buf.Length != 0) return false;
                _ofs += (ulong)spanWriter.InitialBuffer.Length;
                Init(ref spanWriter);
                return true;
            }

            public long GetCurrentPosition(in SpanWriter spanWriter)
            {
                return (long)_ofs + (spanWriter.InitialBuffer.Length - spanWriter.Buf.Length);
            }

            public long GetCurrentPositionWithoutWriter()
            {
                return (long)_ofs;
            }

            public void WriteBlock(ref SpanWriter spanWriter, ref byte buffer, uint length)
            {
                while (length > 0)
                {
                    if (spanWriter.Buf.Length == 0)
                    {
                        Flush(ref spanWriter);
                    }

                    var l = Math.Min((uint)spanWriter.Buf.Length, length);
                    Unsafe.CopyBlockUnaligned(ref PackUnpack.UnsafeGetAndAdvance(ref spanWriter.Buf, (int)l),
                        ref buffer, l);
                    buffer = ref Unsafe.AddByteOffset(ref buffer, (IntPtr)l);
                    length -= l;
                }
            }

            public void WriteBlockWithoutWriter(ref byte buffer, uint length)
            {
                var writer = new SpanWriter(this);
                writer.WriteBlock(ref buffer, length);
                writer.Sync();
            }

            public void SetCurrentPosition(ref SpanWriter spanWriter, long position)
            {
                throw new NotSupportedException();
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
            _flushedSize = _writer.GetCurrentPositionWithoutWriter();
            Interlocked.MemoryBarrier();
        }

        public void HardFlush()
        {
            Flush();
        }

        public void SetSize(long size)
        {
            if ((ulong)size != GetSize())
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
