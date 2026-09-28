using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.Replication;

// Streaming reads let a KVI, typically a fifth to a third of the database, load while it downloads (cold) or while
// its whole-file checksum runs beside the load (warm). The load is accepted only after that verification succeeds.
public sealed partial class ReplicationFileSet
{
    /// Download progress that a streaming reader waits on. Bytes below Written are on the local target in order.
    internal sealed class DownloadProgress
    {
        readonly object _lock = new();
        IFileCollectionFile? _target;
        long _written;
        bool _done;
        Exception? _failure;

        public void Start(IFileCollectionFile target)
        {
            lock (_lock)
            {
                _target = target;
                Monitor.PulseAll(_lock);
            }
        }

        public void Report(long written)
        {
            lock (_lock)
            {
                _written = written;
                Monitor.PulseAll(_lock);
            }
        }

        public void Finish(IFileCollectionFile file, long length)
        {
            lock (_lock)
            {
                _target = file;
                _written = length;
                _done = true;
                Monitor.PulseAll(_lock);
            }
        }

        public void Fail(Exception error)
        {
            lock (_lock)
            {
                _failure = error;
                _done = true;
                Monitor.PulseAll(_lock);
            }
        }

        // Blocks the reading thread until end bytes are written; downloads run on other threads.
        public IFileCollectionFile WaitFor(ulong end, CancellationToken cancellation)
        {
            lock (_lock)
            {
                while (true)
                {
                    if (_failure != null) throw new IOException("The streamed download failed.", _failure);
                    if (_target != null && (ulong)_written >= end) return _target;
                    if (_done) throw new EndOfStreamException();
                    cancellation.ThrowIfCancellationRequested();
                    Monitor.Wait(_lock, 100);
                }
            }
        }
    }

    /// Forward reader over a file length through a positioned read, buffered in 1 MiB.
    sealed unsafe class StreamingReader(ulong size, StreamingReader.ReadAt readAt) : IMemReader
    {
        public delegate void ReadAt(ulong position, Span<byte> destination);

        const int BufferSize = 1024 * 1024;
        readonly byte[] _buffer = GC.AllocateUninitializedArray<byte>(BufferSize, pinned: true);
        ulong _bufferStart;

        public void Init(ref MemReader reader) => Fill(ref reader, _bufferStart);

        void Fill(ref MemReader reader, ulong position)
        {
            _bufferStart = position;
            var count = (int)Math.Min((ulong)BufferSize, size - Math.Min(size, position));
            if (count != 0) readAt(position, _buffer.AsSpan(0, count));
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
            if (position > size)
            {
                Fill(ref memReader, size);
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
            _bufferStart + (ulong)(memReader.Current - memReader.Start) >= size;
    }

    /// Cold: read the download as it is written; the download verifies the checksum before it completes.
    sealed class DownloadingStreamingRead(RemoteInventoryFile file, DownloadProgress progress,
        CancellationToken cancellation) : IStreamingFileRead
    {
        public IMemReader Reader { get; } = new StreamingReader(file.Selected.Length,
            (position, destination) => progress.WaitFor(position + (ulong)destination.Length, cancellation)
                .RandomRead(destination, position, false));

        public ulong Length => file.Selected.Length;

        public async ValueTask<bool> CompleteAsync(CancellationToken completion)
        {
            await file.GetLocalAsync(completion).ConfigureAwait(false);
            return true;
        }

        public void Dispose() { } // The prefetch continues and is shared with later requests.
    }

    /// Warm: read the cached copy while a parallel task hashes it; the page cache serves the second reader.
    sealed class CachedStreamingRead : IStreamingFileRead
    {
        readonly ReplicationFileSet _owner;
        readonly RemoteInventoryFile _file;
        readonly IFileCollectionFile _candidate;
        readonly Task<IFileCollectionFile?> _verification;

        public CachedStreamingRead(ReplicationFileSet owner, RemoteInventoryFile file, IFileCollectionFile candidate,
            Task<IFileCollectionFile?> verification)
        {
            _owner = owner;
            _file = file;
            _candidate = candidate;
            Reader = new StreamingReader(file.Selected.Length,
                (position, destination) => candidate.RandomRead(destination, position, false));
            _verification = verification;
        }

        public IMemReader Reader { get; }
        public ulong Length => _file.Selected.Length;

        public async ValueTask<bool> CompleteAsync(CancellationToken cancellation)
        {
            var verified = await _verification.WaitAsync(cancellation).ConfigureAwait(false);
            ObjectDisposedException.ThrowIf(_owner._disposed, _owner);
            if (verified != null && ReferenceEquals(_file.GetCachedLocal(), _candidate)) return true;
            // Cache population validates and discards under the same gate; never remove a file it is hashing.
            await _owner._downloads.WaitAsync(cancellation).ConfigureAwait(false);
            try
            {
                if (ReferenceEquals(_owner.Local.GetFile(_file.Index), _candidate))
                    _owner.DiscardCachedFile(_candidate, "SHA-256 validation failed", _file.Index);
            }
            finally { _owner._downloads.Release(); }
            return false;
        }

        public void Dispose() { } // The collection owns and drains the shared verification.
    }
}
