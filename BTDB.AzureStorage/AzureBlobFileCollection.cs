using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.AzureStorage;

public sealed class AzureBlobFileCollection : IFileCollection, IAsyncDisposable
{
    static readonly TimeSpan RemoteOperationRetryDelay = TimeSpan.FromSeconds(1);

    readonly IBlobStorageBackend _blobStorageBackend;
    readonly string _localCacheDirectory;
    readonly bool _deleteLocalCacheDirectoryOnDispose;
    readonly ConcurrentDictionary<uint, AzureBlobFile> _files;
    readonly IAzureBlobFileCollectionLogger? _logger;

    readonly Channel<RemoteOperation> _remoteOperations = Channel.CreateUnbounded<RemoteOperation>(
        new() { SingleReader = true, SingleWriter = false });

    readonly PeriodicTimer _transactionLogTimer;
    readonly Task _transactionLogTimerTask;
    readonly Task _remoteWorkerTask;
    readonly CancellationTokenSource _timerCancellation = new();
    int _queuedOperationCount;
    int _maxFileId;
    bool _disposed;

    AzureBlobFileCollection(AzureBlobFileCollectionOptions options, string localCacheDirectory,
        bool deleteLocalCacheDirectoryOnDispose, ConcurrentDictionary<uint, AzureBlobFile> files, int maxFileId)
    {
        _blobStorageBackend = options.CreateBlobStorageBackend();
        _localCacheDirectory = localCacheDirectory;
        _deleteLocalCacheDirectoryOnDispose = deleteLocalCacheDirectoryOnDispose;
        _files = files;
        _logger = options.Logger;
        _maxFileId = maxFileId;
        _transactionLogTimer = new(options.TransactionLogFlushPeriod, options.TimeProvider);
        _remoteWorkerTask = Task.Run(ProcessRemoteOperationsAsync);
        _transactionLogTimerTask = Task.Run(RunTransactionLogTimerAsync);
    }

    public string LocalCacheDirectory => _localCacheDirectory;

    public static async Task<AzureBlobFileCollection> CreateAsync(AzureBlobFileCollectionOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        var blobStorageBackend = options.CreateBlobStorageBackend();

        var localCacheDirectory = options.LocalCacheDirectory;
        var deleteLocalCacheDirectoryOnDispose = options.DeleteLocalCacheDirectoryOnDispose;
        if (string.IsNullOrEmpty(localCacheDirectory))
        {
            localCacheDirectory = Path.Combine(Path.GetTempPath(), "BTDB.AzureStorage", Guid.NewGuid().ToString("N"));
        }

        Directory.CreateDirectory(localCacheDirectory);

        var localFiles = EnumerateLocalFiles(localCacheDirectory);
        var remoteFiles = await EnumerateRemoteFilesAsync(blobStorageBackend, cancellationToken);
        var filesToUpload = new HashSet<uint>();
        var downloadFiles = new List<RemoteFileState>();

        foreach (var fileId in localFiles.Keys.Union(remoteFiles.Keys))
        {
            var hasLocal = localFiles.TryGetValue(fileId, out var localFile);
            var hasRemote = remoteFiles.TryGetValue(fileId, out var remoteFile);
            if (!hasRemote)
            {
                if (hasLocal && localFile.Length > 0)
                {
                    filesToUpload.Add(fileId);
                }

                continue;
            }

            if (!hasLocal || remoteFile.Length > localFile.Length)
            {
                downloadFiles.Add(remoteFile);
                continue;
            }

            if (localFile.Length > remoteFile.Length)
            {
                filesToUpload.Add(fileId);
            }
        }

        var remainingDownloads = downloadFiles.Count;
        await Parallel.ForEachAsync(downloadFiles, new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = options.DownloadParallelism
            },
            async (remoteFile, ct) =>
            {
                var downloadsLeft = Interlocked.Decrement(ref remainingDownloads);
                options.Logger?.InitialDownloadExecuting(remoteFile.BlobName, downloadsLeft, remoteFile.Length);
                await blobStorageBackend.DownloadToAsync(remoteFile.BlobName,
                    Path.Combine(localCacheDirectory, remoteFile.BlobName), ct);
            });

        var collection = new AzureBlobFileCollection(options, localCacheDirectory, deleteLocalCacheDirectoryOnDispose,
            new(), 0);
        var files = collection.CreateFiles(localCacheDirectory, remoteFiles, out var maxId);
        collection._files.Clear();
        foreach (var pair in files)
        {
            collection._files[pair.Key] = pair.Value;
        }

        collection._maxFileId = maxId;
        foreach (var fileId in filesToUpload.OrderBy(id => id))
        {
            if (collection._files.TryGetValue(fileId, out var file))
            {
                collection.EnqueueRemoteOperation(file.IsTransactionLog
                    ? new SynchronizeTransactionLogOperation(file)
                    : new UploadFileOperation(file));
            }
        }

        return collection;
    }

    ConcurrentDictionary<uint, AzureBlobFile> CreateFiles(string localCacheDirectory,
        IReadOnlyDictionary<uint, RemoteFileState> remoteFiles, out int maxId)
    {
        var files = new ConcurrentDictionary<uint, AzureBlobFile>();
        maxId = 0;
        foreach (var filePath in Directory.EnumerateFiles(localCacheDirectory))
        {
            if (!TryParseFileName(Path.GetFileName(filePath), out var index, out var blobName, out var humanHint))
                continue;
            var remoteLength = remoteFiles.TryGetValue(index, out var remoteFile) ? remoteFile.Length : 0;
            var file = CreateAzureBlobFile(index, humanHint, blobName, filePath, remoteLength, writable: false);
            files[index] = file;
            if (index > maxId) maxId = (int)index;
        }

        return files;
    }

    static Dictionary<uint, LocalFileState> EnumerateLocalFiles(string localCacheDirectory)
    {
        var result = new Dictionary<uint, LocalFileState>();
        foreach (var filePath in Directory.EnumerateFiles(localCacheDirectory))
        {
            var fileName = Path.GetFileName(filePath);
            if (!TryParseFileName(fileName, out var index, out _, out var humanHint)) continue;
            result[index] = new LocalFileState(fileName, humanHint, new FileInfo(filePath).Length);
        }

        return result;
    }

    static async Task<Dictionary<uint, RemoteFileState>> EnumerateRemoteFilesAsync(
        IBlobStorageBackend blobStorageBackend,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<uint, RemoteFileState>();
        await foreach (var blob in blobStorageBackend.ListFilesAsync(cancellationToken))
        {
            if (!TryParseFileName(blob.Name, out var index, out _, out var humanHint)) continue;
            result[index] = new RemoteFileState(blob.Name, humanHint, blob.Length);
        }

        return result;
    }

    public IFileCollectionFile AddFile(string humanHint)
    {
        ThrowIfDisposed();
        var index = (uint)Interlocked.Increment(ref _maxFileId);
        var blobName = CreateFileName(index, humanHint);
        var file = CreateAzureBlobFile(index, humanHint, blobName, Path.Combine(_localCacheDirectory, blobName), 0,
            writable: true);
        if (!_files.TryAdd(index, file))
            throw new InvalidOperationException("Cannot add Azure blob-backed BTDB file.");
        return file;
    }

    public uint GetCount()
    {
        return (uint)_files.Count;
    }

    public IFileCollectionFile? GetFile(uint index)
    {
        return _files.TryGetValue(index, out var file) ? file : null;
    }

    public IEnumerable<IFileCollectionFile> Enumerate()
    {
        return _files.Values.ToArray();
    }

    public void ConcurrentTemporaryTruncate(uint index, uint offset)
    {
        ThrowIfDisposed();
        if (_files.TryGetValue(index, out var file) && file.IsTransactionLog)
        {
            EnqueueRemoteOperation(new SynchronizeTransactionLogOperation(file));
        }
        else
        {
            throw new InvalidOperationException(
                "ConcurrentTemporaryTruncate is only supported for transaction log files.");
        }
    }

    public async Task FlushPendingChangesAsync(CancellationToken cancellationToken = default)
    {
        FlushDirtyTransactionLogs();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EnqueueRemoteOperation(new BarrierOperation(completion));
        using var registration =
            cancellationToken.Register(static state => { ((TaskCompletionSource)state!).TrySetCanceled(); },
                completion);
        await completion.Task;
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await FlushPendingChangesAsync();

        _timerCancellation.Cancel();
        _transactionLogTimer.Dispose();
        try
        {
            await _transactionLogTimerTask;
        }
        catch (OperationCanceledException)
        {
        }

        _remoteOperations.Writer.TryComplete();
        try
        {
            await _remoteWorkerTask;
        }
        finally
        {
            foreach (var file in _files.Values)
            {
                file.DisposeLocal();
            }

            _timerCancellation.Dispose();
            if (_deleteLocalCacheDirectoryOnDispose && Directory.Exists(_localCacheDirectory))
            {
                Directory.Delete(_localCacheDirectory, true);
            }
        }
    }

    async Task RunTransactionLogTimerAsync()
    {
        try
        {
            while (await _transactionLogTimer.WaitForNextTickAsync(_timerCancellation.Token))
            {
                FlushDirtyTransactionLogs();
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    void FlushDirtyTransactionLogs()
    {
        foreach (var file in _files.Values)
        {
            if (file.IsTransactionLogDirty)
            {
                EnqueueRemoteOperation(new SynchronizeTransactionLogOperation(file));
            }
        }
    }

    void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    void EnqueueRemoteOperation(RemoteOperation operation)
    {
        if (!_remoteOperations.Writer.TryWrite(operation))
            throw new InvalidOperationException("Cannot queue Azure blob synchronization work.");
        var queueLength = Interlocked.Increment(ref _queuedOperationCount);
        _logger?.OperationQueued(FormatOperation(operation), queueLength);
    }

    async Task ProcessRemoteOperationsAsync()
    {
        await foreach (var operation in _remoteOperations.Reader.ReadAllAsync())
        {
            var queueLength = Interlocked.Decrement(ref _queuedOperationCount);
            var operationDescription = FormatOperation(operation);
            while (true)
            {
                try
                {
                    _logger?.OperationExecuting(operationDescription, queueLength, GetOperationFileLength(operation));
                    switch (operation)
                    {
                        case UploadFileOperation uploadFileOperation:
                            await UploadFileAsync(uploadFileOperation.File);
                            break;
                        case SynchronizeTransactionLogOperation synchronizeTransactionLogOperation:
                            await SynchronizeTransactionLogAsync(synchronizeTransactionLogOperation.File);
                            break;
                        case DeleteFileOperation deleteFileOperation:
                            await DeleteFileAsync(deleteFileOperation.File);
                            break;
                        case BarrierOperation barrierOperation:
                            barrierOperation.Completion.TrySetResult();
                            break;
                    }

                    break;
                }
                catch (Exception exception)
                {
                    _logger?.OperationFailed(operationDescription, queueLength, exception);
                    await Task.Delay(RemoteOperationRetryDelay);
                }
            }
        }
    }

    async Task UploadFileAsync(AzureBlobFile file)
    {
        await _blobStorageBackend.UploadBlockBlobAsync(file.BlobName, file.LocalPath, CancellationToken.None);
        file.RemoteLength = file.GetLocalLength();
    }

    async Task SynchronizeTransactionLogAsync(AzureBlobFile file)
    {
        var targetLength = file.GetLocalLength();
        if (targetLength == file.RemoteLength) return;
        await _blobStorageBackend.AppendBlockBlobAsync(file.BlobName, targetLength,
            (offset, destination) => file.RandomRead(destination, (ulong)offset, false), CancellationToken.None);

        file.RemoteLength = targetLength;
        file.ClearDirtyUpTo(targetLength);
    }

    async Task DeleteFileAsync(AzureBlobFile file)
    {
        await _blobStorageBackend.DeleteIfExistsAsync(file.BlobName, CancellationToken.None);
        file.DeleteLocalFile();
    }

    void RemoveFile(AzureBlobFile file)
    {
        ThrowIfDisposed();
        EnqueueRemoteOperation(new DeleteFileOperation(file));
    }

    void ScheduleFileFlush(AzureBlobFile file)
    {
        ThrowIfDisposed();
        if (!file.IsTransactionLog)
            throw new InvalidOperationException(
                "HardFlush is supported only for transaction log files in AzureBlobFileCollection.");
        file.MarkTransactionLogDirty();
        EnqueueRemoteOperation(new SynchronizeTransactionLogOperation(file));
    }

    void ScheduleFileFinalize(AzureBlobFile file)
    {
        ThrowIfDisposed();
        EnqueueRemoteOperation(file.IsTransactionLog
            ? new SynchronizeTransactionLogOperation(file)
            : new UploadFileOperation(file));
    }

    static string CreateFileName(uint index, string humanHint)
    {
        return index.ToString("D8") + "." + humanHint;
    }

    AzureBlobFile CreateAzureBlobFile(uint index, string humanHint, string blobName, string filePath,
        long remoteLength, bool writable)
    {
        return humanHint switch
        {
            "trl" => new TransactionLogBlobFile(this, index, blobName, filePath, remoteLength, openWritable: writable),
            "pvl" => new PureValueBlobFile(this, index, blobName, filePath, remoteLength, writable),
            "kvi" => new KeyIndexBlobFile(this, index, blobName, filePath, remoteLength, writable),
            _ => throw new NotSupportedException(
                $"AzureBlobFileCollection supports only BTDB KVDB file types 'trl', 'pvl', and 'kvi'. Unsupported file type: '{humanHint}'.")
        };
    }

    static string FormatOperation(RemoteOperation operation)
    {
        return operation switch
        {
            UploadFileOperation uploadFileOperation => "UploadFile " + uploadFileOperation.File.BlobName,
            SynchronizeTransactionLogOperation synchronizeTransactionLogOperation =>
                "SynchronizeTransactionLog " + synchronizeTransactionLogOperation.File.BlobName,
            DeleteFileOperation deleteFileOperation => "DeleteFile " + deleteFileOperation.File.BlobName,
            BarrierOperation => "Barrier",
            _ => operation.GetType().Name
        };
    }

    static long? GetOperationFileLength(RemoteOperation operation)
    {
        return operation switch
        {
            UploadFileOperation uploadFileOperation => uploadFileOperation.File.GetLocalLength(),
            SynchronizeTransactionLogOperation synchronizeTransactionLogOperation =>
                synchronizeTransactionLogOperation.File.GetLocalLength(),
            _ => null
        };
    }

    static bool TryParseFileName(string fileName, out uint index, out string blobName, out string humanHint)
    {
        blobName = fileName;
        humanHint = "";
        index = 0;
        var extensionSeparator = fileName.IndexOf('.');
        if (extensionSeparator <= 0) return false;
        if (!uint.TryParse(fileName.AsSpan(0, extensionSeparator), out index) || index == 0) return false;
        humanHint = extensionSeparator == fileName.Length - 1 ? "" : fileName[(extensionSeparator + 1)..];
        return fileName.IndexOfAny(Path.GetInvalidFileNameChars()) == -1 &&
               !fileName.Contains(Path.DirectorySeparatorChar) &&
               !fileName.Contains(Path.AltDirectorySeparatorChar);
    }

    abstract class AzureBlobFile : IFileCollectionFile
    {
        readonly AzureBlobFileCollection _owner;
        readonly uint _index;
        readonly string _blobName;
        readonly string _fileName;

        protected AzureBlobFile(AzureBlobFileCollection owner, uint index, string blobName, string fileName)
        {
            _owner = owner;
            _index = index;
            _blobName = blobName;
            _fileName = fileName;
        }

        public uint Index => _index;
        public string LocalPath => _fileName;
        public string BlobName => _blobName;

        public abstract bool IsTransactionLog { get; }
        public abstract bool IsTransactionLogDirty { get; }
        public abstract long RemoteLength { get; set; }
        public abstract long GetLocalLength();
        public virtual void MarkTransactionLogDirty() => throw new InvalidOperationException();
        public virtual void ClearDirtyUpTo(long uploadedLength) => RemoteLength = uploadedLength;
        public abstract IMemReader GetExclusiveReader();
        public abstract void AdvisePrefetch();
        public abstract void RandomRead(Span<byte> data, ulong position, bool doNotCache);
        public abstract IMemWriter GetAppenderWriter();
        public abstract IMemWriter GetExclusiveAppenderWriter();
        public abstract void HardFlush();
        public abstract void HardFlushTruncateSwitchToReadOnlyMode();
        public abstract void HardFlushTruncateSwitchToDisposedMode();
        public ulong GetSize() => (ulong)GetLocalLength();

        public void Remove()
        {
            _owner.RemoveFile(this);
        }

        public void DeleteLocalFile()
        {
            DisposeLocal();
            if (File.Exists(LocalPath))
            {
                File.Delete(LocalPath);
            }
        }

        public abstract void DisposeLocal();

        protected void NotifyHardFlush()
        {
            _owner.ScheduleFileFlush(this);
        }

        protected void NotifyFinalize()
        {
            _owner.ScheduleFileFinalize(this);
        }
    }

    sealed class TransactionLogBlobFile : AzureBlobFile
    {
        readonly object _lock = new();
        MemoryMappedFile? _memoryMappedFile;
        MemoryMappedViewAccessor? _accessor;
        FileStream? _stream;
        unsafe byte* _pointer;
        Writer? _writer;
        long _cachedLength;
        long _localLength;
        long _remoteLength;
        long _dirtyUntilLength;
        bool _disposed;
        bool _readOnly;
        const long ResizeChunkSize = 4 * 1024 * 1024;

        public TransactionLogBlobFile(AzureBlobFileCollection owner, uint index, string blobName, string fileName,
            long remoteLength, bool openWritable) : base(owner, index, blobName, fileName)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(fileName)!);
            _localLength = File.Exists(fileName) ? new FileInfo(fileName).Length : 0;
            _cachedLength = _localLength;
            _remoteLength = remoteLength;
            _dirtyUntilLength = _localLength > _remoteLength ? _localLength : 0;
            if (openWritable)
            {
                OpenWritableCore();
            }
        }

        public override bool IsTransactionLog => true;

        public override bool IsTransactionLogDirty
        {
            get
            {
                lock (_lock)
                {
                    return _dirtyUntilLength > _remoteLength;
                }
            }
        }

        public override long RemoteLength
        {
            get
            {
                lock (_lock)
                {
                    return _remoteLength;
                }
            }
            set
            {
                lock (_lock)
                {
                    _remoteLength = value;
                    if (_dirtyUntilLength <= _remoteLength)
                    {
                        _dirtyUntilLength = 0;
                    }
                }
            }
        }

        public override long GetLocalLength()
        {
            lock (_lock)
            {
                return _localLength;
            }
        }

        public override void MarkTransactionLogDirty()
        {
            lock (_lock)
            {
                _dirtyUntilLength = Math.Max(_dirtyUntilLength, _localLength);
            }
        }

        public override void ClearDirtyUpTo(long uploadedLength)
        {
            lock (_lock)
            {
                _remoteLength = uploadedLength;
                if (_dirtyUntilLength <= uploadedLength)
                {
                    _dirtyUntilLength = 0;
                }
            }
        }

        public override IMemReader GetExclusiveReader()
        {
            return new Reader(this);
        }

        public override void AdvisePrefetch()
        {
        }

        public override unsafe void RandomRead(Span<byte> data, ulong position, bool doNotCache)
        {
            lock (_lock)
            {
                EnsureMapped();
                new Span<byte>(_pointer + position, data.Length).CopyTo(data);
            }
        }

        public override IMemWriter GetAppenderWriter()
        {
            lock (_lock)
            {
                if (_readOnly)
                    throw new InvalidOperationException("Transaction log file is read-only.");
                OpenWritableCore();
                return _writer!;
            }
        }

        public override IMemWriter GetExclusiveAppenderWriter()
        {
            return GetAppenderWriter();
        }

        public override void HardFlush()
        {
            lock (_lock)
            {
                if (_stream is null) return;
                UnmapContent();
                _stream.SetLength(_localLength);
                _cachedLength = _localLength;
            }

            NotifyHardFlush();
        }

        public override void HardFlushTruncateSwitchToReadOnlyMode()
        {
            lock (_lock)
            {
                UnmapContent();
                if (_stream is not null)
                {
                    _stream.SetLength(_localLength);
                    _cachedLength = _localLength;
                    _stream.Dispose();
                    _stream = null;
                }

                _writer = null;
                _readOnly = true;
                EnsureMapped();
            }

            NotifyFinalize();
        }

        public override void HardFlushTruncateSwitchToDisposedMode()
        {
            lock (_lock)
            {
                UnmapContent();
                if (_stream is not null)
                {
                    _stream.SetLength(_localLength);
                    _cachedLength = _localLength;
                    _stream.Dispose();
                    _stream = null;
                }

                _writer = null;
                _readOnly = true;
            }

            NotifyFinalize();
        }

        public override void DisposeLocal()
        {
            lock (_lock)
            {
                if (_disposed) return;
                _disposed = true;
                UnmapContent();
                if (_stream is not null)
                {
                    _stream.SetLength(_localLength);
                    _stream.Dispose();
                    _stream = null;
                }

                _writer = null;
            }
        }

        void OpenWritableCore()
        {
            if (_stream is not null) return;
            UnmapContent();
            _stream = new FileStream(LocalPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 1,
                FileOptions.None);
            _localLength = _stream.Length;
            _cachedLength = _localLength;
            _writer = new Writer(this);
        }

        unsafe void EnsureMapped()
        {
            if (_accessor != null) return;
            if (_stream is null)
            {
                if (_localLength == 0) return;
                _memoryMappedFile = MemoryMappedFile.CreateFromFile(LocalPath, FileMode.Open, null, 0,
                    MemoryMappedFileAccess.Read);
                _accessor = _memoryMappedFile.CreateViewAccessor(0, _localLength, MemoryMappedFileAccess.Read);
            }
            else
            {
                var capacity = Math.Max(1, _cachedLength);
                _memoryMappedFile = MemoryMappedFile.CreateFromFile(_stream!, null, capacity,
                    MemoryMappedFileAccess.ReadWrite,
                    HandleInheritability.None, true);
                _accessor = _memoryMappedFile.CreateViewAccessor();
            }

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
            readonly TransactionLogBlobFile _owner;
            readonly ulong _valueSize;

            public Reader(TransactionLogBlobFile owner)
            {
                _owner = owner;
                _valueSize = (ulong)_owner.GetLocalLength();
                if (_valueSize != 0)
                {
                    lock (_owner._lock)
                    {
                        _owner.EnsureMapped();
                    }
                }
            }

            public void Init(ref MemReader reader)
            {
                unsafe
                {
                    reader.Start = (nint)_owner._pointer;
                    reader.Current = reader.Start;
                    reader.End = reader.Start + (nint)_valueSize;
                }
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
            readonly TransactionLogBlobFile _file;
            internal ulong Ofs;

            public Writer(TransactionLogBlobFile file)
            {
                _file = file;
                Ofs = (ulong)_file._localLength;
            }

            void ExpandIfNeeded(long size)
            {
                lock (_file._lock)
                {
                    if (_file._cachedLength < size)
                    {
                        _file.UnmapContent();
                        var newSize = ((size - 1) / ResizeChunkSize + 1) * ResizeChunkSize;
                        _file._stream!.SetLength(newSize);
                        _file._cachedLength = newSize;
                    }

                    _file.EnsureMapped();
                }
            }

            public void Init(ref MemWriter memWriter)
            {
                lock (_file._lock)
                {
                    _file.EnsureMapped();
                    unsafe
                    {
                        memWriter.Start = (nint)_file._pointer;
                        memWriter.Current = memWriter.Start + (nint)Ofs;
                        memWriter.End = memWriter.Start + (nint)_file._cachedLength;
                    }
                }
            }

            public void Flush(ref MemWriter memWriter, uint spaceNeeded)
            {
                Ofs = (ulong)(memWriter.Current - memWriter.Start);
                lock (_file._lock)
                {
                    _file._localLength = (long)Ofs;
                }

                if (spaceNeeded == 0) return;
                ExpandIfNeeded((long)Ofs + ResizeChunkSize);
                Init(ref memWriter);
            }

            public long GetCurrentPosition(in MemWriter memWriter)
            {
                return memWriter.Current - memWriter.Start;
            }

            public unsafe void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
            {
                Ofs = (ulong)(memWriter.Current - memWriter.Start);
                ExpandIfNeeded((long)Ofs + (long)length);
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(_file._pointer + Ofs), ref buffer, (uint)length);
                Ofs += length;
                lock (_file._lock)
                {
                    _file._localLength = (long)Ofs;
                }

                Init(ref memWriter);
            }

            public void SetCurrentPosition(ref MemWriter memWriter, long position)
            {
                throw new NotSupportedException();
            }
        }
    }

    abstract class BufferedBlobFile : AzureBlobFile
    {
        readonly object _stateLock = new();
        MemoryMappedFile? _memoryMappedFile;
        MemoryMappedViewAccessor? _accessor;
        FileStream? _stream;
        unsafe byte* _pointer;
        BufferedWriter? _writer;
        long _localLength;
        long _remoteLength;
        bool _disposed;
        const int WriterBufferSize = 128 * 1024;

        protected BufferedBlobFile(AzureBlobFileCollection owner, uint index, string blobName, string fileName,
            long remoteLength, bool writable) :
            base(owner, index, blobName, fileName)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(fileName)!);
            _remoteLength = remoteLength;
            if (writable)
            {
                _stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 1,
                    FileOptions.None);
                _localLength = _stream.Length;
                _writer = new BufferedWriter(this, _localLength);
            }
            else
            {
                _localLength = File.Exists(fileName) ? new FileInfo(fileName).Length : 0;
            }
        }

        public override bool IsTransactionLog => false;
        public override bool IsTransactionLogDirty => false;

        public override long RemoteLength
        {
            get
            {
                lock (_stateLock)
                {
                    return _remoteLength;
                }
            }
            set
            {
                lock (_stateLock)
                {
                    _remoteLength = value;
                }
            }
        }

        public override long GetLocalLength()
        {
            lock (_stateLock)
            {
                return _localLength;
            }
        }

        public override IMemReader GetExclusiveReader()
        {
            EnsureReadableMapping();
            return new ReadOnlyMappedReader(this, (ulong)GetLocalLength());
        }

        public override void AdvisePrefetch()
        {
        }

        public override unsafe void RandomRead(Span<byte> data, ulong position, bool doNotCache)
        {
            EnsureReadableMapping();
            new Span<byte>(_pointer + position, data.Length).CopyTo(data);
        }

        public override IMemWriter GetAppenderWriter()
        {
            lock (_stateLock)
            {
                return _writer ?? throw new InvalidOperationException("File is not writable.");
            }
        }

        public override IMemWriter GetExclusiveAppenderWriter()
        {
            return GetAppenderWriter();
        }

        public override void HardFlush()
        {
            throw new InvalidOperationException(
                "HardFlush is supported only for transaction log files in AzureBlobFileCollection.");
        }

        public override void HardFlushTruncateSwitchToReadOnlyMode()
        {
            lock (_stateLock)
            {
                FinalizeWritableCore();
            }

            EnsureReadableMapping();
            NotifyFinalize();
        }

        public override void HardFlushTruncateSwitchToDisposedMode()
        {
            lock (_stateLock)
            {
                FinalizeWritableCore();
                CloseReadableMapping();
            }

            NotifyFinalize();
        }

        public override void DisposeLocal()
        {
            lock (_stateLock)
            {
                if (_disposed) return;
                _disposed = true;
                FinalizeWritableCore();
                CloseReadableMapping();
            }
        }

        protected unsafe byte* Pointer => _pointer;

        void EnsureReadableMapping()
        {
            lock (_stateLock)
            {
                if (_accessor != null || _localLength == 0) return;
                if (_stream is not null)
                    throw new InvalidOperationException(
                        "Reading from a writable AzureBlobFileCollection file is not supported.");

                _memoryMappedFile = MemoryMappedFile.CreateFromFile(LocalPath, FileMode.Open, null, 0,
                    MemoryMappedFileAccess.Read);
                _accessor = _memoryMappedFile.CreateViewAccessor(0, _localLength, MemoryMappedFileAccess.Read);
                unsafe
                {
                    _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
                }
            }
        }

        void CloseReadableMapping()
        {
            if (_accessor == null) return;
            unsafe
            {
                _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }

            _accessor.Dispose();
            _accessor = null;
            _memoryMappedFile!.Dispose();
            _memoryMappedFile = null;
        }

        void FinalizeWritableCore()
        {
            if (_stream is null) return;
            CloseReadableMapping();
            _stream.SetLength(_localLength);
            _stream.Dispose();
            _stream = null;
            _writer = null;
        }

        sealed class ReadOnlyMappedReader : IMemReader
        {
            readonly BufferedBlobFile _owner;
            readonly ulong _valueSize;

            public ReadOnlyMappedReader(BufferedBlobFile owner, ulong valueSize)
            {
                _owner = owner;
                _valueSize = valueSize;
            }

            public void Init(ref MemReader reader)
            {
                unsafe
                {
                    reader.Start = (nint)_owner.Pointer;
                    reader.Current = reader.Start;
                    reader.End = reader.Start + (nint)_valueSize;
                }
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

        sealed class BufferedWriter : IMemWriter
        {
            readonly BufferedBlobFile _file;
            readonly byte[] _buffer;
            internal ulong Ofs;

            public BufferedWriter(BufferedBlobFile file, long initialLength)
            {
                _file = file;
                _buffer = GC.AllocateUninitializedArray<byte>(WriterBufferSize, pinned: true);
                Ofs = (ulong)initialLength;
            }

            void FlushBuffer(int position)
            {
                if (position == 0) return;
                var stream = _file._stream ?? throw new InvalidOperationException("File is not writable.");
                RandomAccess.Write(stream.SafeFileHandle, _buffer.AsSpan(0, position), (long)Ofs);
                Ofs += (ulong)position;
                _file._localLength = (long)Ofs;
            }

            public unsafe void Init(ref MemWriter memWriter)
            {
                memWriter.Start = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_buffer));
                memWriter.Current = memWriter.Start;
                memWriter.End = memWriter.Start + WriterBufferSize;
            }

            public void Flush(ref MemWriter memWriter, uint spaceNeeded)
            {
                FlushBuffer((int)(memWriter.Current - memWriter.Start));
                Init(ref memWriter);
            }

            public long GetCurrentPosition(in MemWriter memWriter)
            {
                return (long)Ofs + (memWriter.Current - memWriter.Start);
            }

            public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
            {
                Flush(ref memWriter, 0);
                var stream = _file._stream ?? throw new InvalidOperationException("File is not writable.");
                RandomAccess.Write(stream.SafeFileHandle, MemoryMarshal.CreateReadOnlySpan(ref buffer, (int)length),
                    (long)Ofs);
                Ofs += length;
                _file._localLength = (long)Ofs;
            }

            public void SetCurrentPosition(ref MemWriter memWriter, long position)
            {
                throw new NotSupportedException();
            }
        }
    }

    sealed class PureValueBlobFile : BufferedBlobFile
    {
        public PureValueBlobFile(AzureBlobFileCollection owner, uint index, string blobName, string fileName,
            long remoteLength, bool writable) :
            base(owner, index, blobName, fileName, remoteLength, writable)
        {
        }
    }

    sealed class KeyIndexBlobFile : BufferedBlobFile
    {
        public KeyIndexBlobFile(AzureBlobFileCollection owner, uint index, string blobName, string fileName,
            long remoteLength, bool writable) :
            base(owner, index, blobName, fileName, remoteLength, writable)
        {
        }
    }

    abstract record RemoteOperation;

    sealed record UploadFileOperation(AzureBlobFile File) : RemoteOperation;

    sealed record SynchronizeTransactionLogOperation(AzureBlobFile File) : RemoteOperation;

    sealed record DeleteFileOperation(AzureBlobFile File) : RemoteOperation;

    sealed record BarrierOperation(TaskCompletionSource Completion) : RemoteOperation;

    sealed record LocalFileState(string FileName, string HumanHint, long Length);

    sealed record RemoteFileState(string BlobName, string HumanHint, long Length);
}
