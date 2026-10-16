using System;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;
using BTDB.LSM.Memory;
using BTDB.LSM.Storage;

namespace BTDB.LSM;

/// Log-structured merge tree implementation of IKeyValueDB stored directly in object storage
/// (BTDB.LSM/ImplementationPlan.md). Every instance opens as a reader of the newest durable state; starting a writing
/// transaction acquires the writer lease, fences the previous writer and catches up. The writer logs every commit to
/// a WAL with group commit, seals full memtables and flushes them and runs leveled compaction in the background.
public sealed partial class LsmKeyValueDB : ILsmKeyValueDB
{
    static long _lastOwner;

    readonly LsmKeyValueDBOptions _options;
    readonly ILsmStorage _storage;
    readonly LsmBlockCache _cache;
    readonly ulong _cacheScope;
    readonly TableBuildOptions _tableOptions;
    readonly LsmCompactionOptions _compactionOptions;
    readonly byte[]?[] _compactionPointers;
    readonly Lock _writeLock = new();
    readonly Lock _stateLock = new();
    readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new();
    readonly ConcurrentDictionary<LsmKeyValueDBTransaction, bool> _transactions = new();
    /// Objects waiting for deletion: the state sequence and time they became obsolete and the first manifest that no
    /// longer references them.
    readonly List<(string Key, ulong FileNumber, long ObsoleteAt, DateTimeOffset Since, long Manifest)>
        _obsoleteObjects = [];
    readonly List<(NativeArena Arena, long ObsoleteAt)> _retiredArenas = [];
    volatile LsmState _state;
    LsmKeyValueDBTransaction? _writingTransaction;
    LsmManifest _lastManifest;
    long _manifestSequence;
    ulong _nextFileNumber;
    volatile bool _disposed;

    LsmKeyValueDB(LsmKeyValueDBOptions options, LsmBlockCache cache, LoadedDatabase loaded, string readerId)
    {
        _readerId = readerId;
        _options = options;
        _storage = options.Storage;
        _cache = cache;
        _cacheScope = cache.Scope(options.Storage, options.Prefix);
        _tableOptions = new()
        {
            BlockSize = options.BlockSize, RestartInterval = options.RestartInterval,
            BloomBitsPerKey = options.BloomBitsPerKey, Compress = options.CompressBlocks,
            IndexPartitionSize = options.IndexPartitionSize
        };
        _compactionOptions = new()
        {
            Level0Trigger = options.Level0CompactionTrigger, LevelBaseBytes = options.LevelBaseBytes,
            LevelMultiplier = options.LevelMultiplier, TargetFileSize = options.TargetFileSize,
            ValueGcThreshold = options.ValueGcThreshold
        };
        _compactionPointers = new byte[]?[loaded.Version.LevelCount];
        _state = new(loaded.Memtable, loaded.MemtableGeneration, loaded.Immutables, loaded.Version,
            loaded.Metadata.CommitUlong, loaded.Metadata.Ulongs, loaded.Metadata.TransactionNumber, 0, 0,
            loaded.Manifest.Sequence);
        _replay = loaded.Replay;
        _lastManifest = loaded.Manifest;
        _manifestSequence = loaded.Manifest.Sequence;
        _nextFileNumber = loaded.Manifest.NextFileNumber;
        _durableTransaction = loaded.Metadata.TransactionNumber;
        MaxTrLogFileSize = options.MemtableSize;
        Logger = options.Logger;
        StartReaderHeartbeat();
    }

    /// Opens the database as a reader of its newest durable state; with AcquireWriterLeaseOnOpen it also becomes the
    /// writer before returning.
    public static async ValueTask<LsmKeyValueDB> OpenAsync(LsmKeyValueDBOptions options,
        CancellationToken cancellation = default)
    {
        var cache = options.BlockCache ?? new LsmBlockCache(options.BlockCacheSize);
        var readerId = Guid.NewGuid().ToString("N");
        if (options.RegisterReader) await WriteRegistryEntryAsync(options, readerId, 0, cancellation);
        var db = new LsmKeyValueDB(options, cache, await LoadAsync(options, cache, null, cancellation), readerId);
        if (options.AcquireWriterLeaseOnOpen) await db.AcquireWriterLeaseAsync(cancellation);
        return db;
    }

    static string TableKey(string prefix, ulong fileNumber) => $"{prefix}tables/{fileNumber:D16}.sst";

    static IMemtable NewMemtable(LsmKeyValueDBOptions options) =>
        options.NativeMemtable ? NativeMemtable.CreateEmpty(options.MemtableSize) : Memtable.CreateEmpty();

    string ValueKey(ulong fileNumber) => $"{_options.Prefix}values/{fileNumber:D16}.vlf";

    string ManifestKey(long sequence) => $"{_options.Prefix}manifest/{sequence:D16}";

    internal LsmKeyValueDBOptions Options => _options;

    internal static long NewOwner() => Interlocked.Increment(ref _lastOwner);

    /// When true, Commit returns only after the commit is durable in the WAL.
    public bool DurableTransactions { get; set; }

    public IKeyValueDBTransaction StartTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Register(false, false);
    }

    public IKeyValueDBTransaction StartReadOnlyTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Register(false, true);
    }

    /// Waits for the writer lease if this instance is not the writer yet, and while flushes, level 0 compaction or WAL
    /// uploads are too far behind. inBatch is ignored: every commit is published (ImplementationPlan.md, D4).
    public async ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isWriter) await AcquireWriterLeaseAsync();
        await WaitWhileStalledAsync();
        ObjectDisposedException.ThrowIf(_disposed, this);
        Task<IKeyValueDBTransaction> waiting;
        lock (_writeLock)
        {
            if (_writingTransaction == null) return NewWritingTransactionUnsafe();
            var waiter = new TaskCompletionSource<IKeyValueDBTransaction>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _writeWaitingQueue.Enqueue(waiter);
            waiting = waiter.Task;
        }

        return await waiting;
    }

    enum StallReason
    {
        None,
        SealedMemtables,
        Level0,
        WalBacklog
    }

    // Stopwatch ticks writers waited, by StallReason.
    readonly long[] _stallTicks = new long[4];

    StallReason Stall()
    {
        if (_worker == null || !_isWriter) return StallReason.None;
        var state = _state;
        if (state.Immutables.Length >= _options.MaxImmutableMemtables) return StallReason.SealedMemtables;
        if (state.Version.Level0.Length >= _options.Level0StopWritesTrigger) return StallReason.Level0;
        return WalBacklogStalls() ? StallReason.WalBacklog : StallReason.None;
    }

    bool IsStalled() => Stall() != StallReason.None;

    void CountStall(StallReason reason, long start) =>
        Interlocked.Add(ref _stallTicks[(int)reason], Stopwatch.GetTimestamp() - start);

    /// Waits while flushes, level 0 compaction or WAL uploads are too far behind; synchronously for an upgrade from
    /// a read transaction, which cannot await (the returned ValueTask is then already complete).
    async ValueTask WaitWhileStalledAsync(bool synchronous = false)
    {
        while (Stall() is var reason && reason != StallReason.None && !_disposed)
        {
            var start = Stopwatch.GetTimestamp();
            var progress = _progress.Task;
            if (IsStalled())
            {
                if (synchronous) progress.Wait(TimeSpan.FromMilliseconds(100));
                else await Task.WhenAny(progress, Task.Delay(100));
            }

            CountStall(reason, start);
        }
    }

    /// Captures the current state and registers the transaction atomically with respect to the decision which
    /// obsolete objects no snapshot can read, so a snapshot never loses its tables, value files or arenas.
    LsmKeyValueDBTransaction Register(bool writing, bool readOnly)
    {
        lock (_obsoleteObjects)
        {
            var transaction = new LsmKeyValueDBTransaction(this, _state, writing, readOnly) { WriterEpoch = _epoch };
            _transactions.TryAdd(transaction, false);
            return transaction;
        }
    }

    LsmKeyValueDBTransaction NewWritingTransactionUnsafe()
    {
        var transaction = Register(true, false);
        _writingTransaction = transaction;
        return transaction;
    }

    internal void MakeWritable(LsmKeyValueDBTransaction transaction, LsmState state)
    {
        WaitWhileStalledAsync(synchronous: true).GetAwaiter().GetResult();

        lock (_writeLock)
        {
            if (!_isWriter)
                throw new BTDBTransactionRetryException("This instance does not hold the writer lease");
            if (_writingTransaction != null)
                throw new BTDBTransactionRetryException("Another writing transaction already running");
            if (_state.CommitSequence != state.CommitSequence)
                throw new BTDBTransactionRetryException("Another writing transaction already finished");
            transaction.WriterEpoch = _epoch;
            _writingTransaction = transaction;
        }
    }

    /// Publishes a commit and queues its WAL record. The memtable's points were computed against the state below it
    /// in the writer's snapshot; flushes and compactions since then changed only the layout of that state.
    internal void CommitWritingTransaction(LsmKeyValueDBTransaction transaction, IMemtable memtable, byte[] record,
        WalCommitMetadata metadata, bool persist)
    {
        try
        {
            if (!_isWriter || transaction.WriterEpoch != _epoch)
                throw new LsmWriterFencedException("The writer lease was lost; the transaction was not committed.");
            var rotate = memtable.ApproximateBytes >= MaxTrLogFileSize;
            long generation;
            lock (_stateLock)
            {
                var current = _state;
                generation = current.MemtableGeneration;
                _state = rotate
                    ? new(NewMemtable(_options), generation + 1,
                        [
                            new(memtable, generation, metadata.CommitUlong, metadata.Ulongs,
                                metadata.TransactionNumber),
                            .. current.Immutables
                        ], current.Version, metadata.CommitUlong, metadata.Ulongs, metadata.TransactionNumber,
                        current.CommitSequence + 1, current.Sequence + 1, current.ManifestSequence)
                    : new(memtable, generation, current.Immutables, current.Version, metadata.CommitUlong,
                        metadata.Ulongs, metadata.TransactionNumber, current.CommitSequence + 1,
                        current.Sequence + 1, current.ManifestSequence);
            }

            AppendWal(generation, metadata.TransactionNumber, record);
            if (rotate) RequestWork();
        }
        finally
        {
            ReleaseWriter();
        }

        // The commit is published and its record queued in order, so the next writer can start while this one waits;
        // durable commits of several writers then share WAL uploads.
        if (persist || DurableTransactions)
            WaitForDurabilityAsync(metadata.TransactionNumber).AsTask().GetAwaiter().GetResult();
    }

    internal void RevertWritingTransaction() => ReleaseWriter();

    void ReleaseWriter()
    {
        lock (_writeLock)
        {
            _writingTransaction = null;
            if (_writeWaitingQueue.Count == 0) return;
            var waiter = _writeWaitingQueue.Dequeue();
            waiter.SetResult(NewWritingTransactionUnsafe());
        }
    }

    /// Releases arenas the snapshot held at once. Storage deletion is left to the background worker, which runs it at
    /// least every second, so disposing a transaction does no storage I/O; without a worker it runs here.
    internal void TransactionDisposed(LsmKeyValueDBTransaction transaction)
    {
        _transactions.TryRemove(transaction, out _);
        if (_worker == null) DeleteObsoleteObjects();
        else ReleaseRetiredArenas();
    }

    public string CalcStats()
    {
        var state = _state;
        var builder = new StringBuilder();
        builder.Append("Writer:").Append(_isWriter).Append(" epoch ").Append(_epoch).AppendLine();
        builder.Append("KeyValueCount:").Append(state.CreateView(state.Memtable).Count).AppendLine();
        builder.Append("MemtableEntries:").Append(state.Memtable.EntryCount).AppendLine();
        builder.Append("MemtableBytes:").Append(state.Memtable.ApproximateBytes).AppendLine();
        builder.Append("MemtableGeneration:").Append(state.MemtableGeneration).AppendLine();
        builder.Append("SealedMemtables:").Append(state.Immutables.Length).AppendLine();
        builder.Append("DurableTransaction:").Append(_durableTransaction).AppendLine();
        builder.Append("PendingWalBytes:").Append(Interlocked.Read(ref _walPendingBytes)).AppendLine();
        for (var level = 0; level < state.Version.LevelCount; level++)
        {
            var tables = level == 0 ? state.Version.Level0 : state.Version.Levels[level];
            if (tables.Length == 0) continue;
            builder.Append("Level").Append(level).Append(':').Append(tables.Length).Append(" tables, ")
                .Append(state.Version.LevelBytes(level)).Append(" bytes").AppendLine();
        }

        builder.Append("Flushes:").Append(_flushCount).Append(", ").Append(_flushBytes).Append(" bytes, ")
            .Append(Stopwatch.GetElapsedTime(0, _flushTicks).TotalMilliseconds.ToString("F0")).Append(" ms").AppendLine();
        builder.Append("Compactions:").Append(_compactionCount).Append(", ").Append(_compactionBytes)
            .Append(" bytes written, ").Append(Stopwatch.GetElapsedTime(0, _compactionTicks).TotalMilliseconds.ToString("F0"))
            .Append(" ms").AppendLine();
        builder.Append("WriteStallMs:");
        for (var reason = StallReason.SealedMemtables; reason <= StallReason.WalBacklog; reason++)
            builder.Append(' ').Append(reason).Append(' ')
                .Append(Stopwatch.GetElapsedTime(0, Interlocked.Read(ref _stallTicks[(int)reason])).TotalMilliseconds
                    .ToString("F0"));
        builder.AppendLine();
        builder.Append("BlockCacheBytes:").Append(_cache.SizeBytes).AppendLine();
        builder.Append("BlockCacheHits:").Append(_cache.Hits).AppendLine();
        builder.Append("BlockCacheMisses:").Append(_cache.Misses).AppendLine();
        if (_workerError != null) builder.Append("BackgroundError:").Append(_workerError.Message).AppendLine();
        return builder.ToString();
    }

    /// Native memory currently held: cached blocks and values plus memtable arenas, including retired ones.
    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats()
    {
        var state = _state;
        var bytes = _cache.NativeBytes + (state.Memtable.Arena?.AllocatedBytes ?? 0) +
                    state.Immutables.Sum(m => m.Memtable.Arena?.AllocatedBytes ?? 0);
        lock (_obsoleteObjects)
            foreach (var (arena, _) in _retiredArenas)
                bytes += arena.AllocatedBytes;
        return ((ulong)bytes, 0, 0, 0);
    }

    /// Flushes the memtable and merges every table into the bottom level. Returns false: no work is left after it.
    public async ValueTask<bool> Compact(CancellationToken cancellation)
    {
        await CheckpointAsync(cancellation);
        var request = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_fullCompactionRequests) _fullCompactionRequests.Add(request);
        RequestWork();
        await request.Task.WaitAsync(cancellation);
        return false;
    }

    /// Seals the memtable and waits until every sealed memtable is in tables and the manifest, so opening needs no
    /// WAL replay.
    public void CreateKvi(CancellationToken cancellation) =>
        CheckpointAsync(cancellation).AsTask().GetAwaiter().GetResult();

    async ValueTask CheckpointAsync(CancellationToken cancellation)
    {
        var transaction = (LsmKeyValueDBTransaction)await StartWritingTransaction();
        try
        {
            RotateHoldingWriter();
        }
        finally
        {
            transaction.Dispose();
        }

        await WaitForFlushesAsync();
    }

    public ulong? PreserveHistoryUpToCommitUlong
    {
        get => null;
        set
        {
            if (value != null)
                throw new NotSupportedException("BTDB.LSM does not support PreserveHistoryUpToCommitUlong.");
        }
    }

    public IKeyValueDBLogger? Logger { get; set; }
    public uint CompactorRamLimitInMb { get; set; }
    public long MaxTrLogFileSize { get; set; }

    public IEnumerable<IKeyValueDBTransaction> Transactions() => _transactions.Keys;

    public ulong CompactorReadBytesPerSecondLimit { get; set; }
    public ulong CompactorWriteBytesPerSecondLimit { get; set; }

    /// As the writer, uploads pending WAL records, flushes sealed memtables and releases the lease; as a reader, only
    /// releases memory.
    public void Dispose()
    {
        if (_disposed) return;
        lock (_writeLock)
        {
            if (_writingTransaction != null)
                throw new BTDBException("Cannot dispose KeyValueDB when writing transaction still running");
            while (_writeWaitingQueue.Count > 0) _writeWaitingQueue.Dequeue().TrySetCanceled();
            if (!_transactions.IsEmpty)
                throw new BTDBException("Cannot dispose KeyValueDB when transactions still running: " +
                                        string.Join(", ", _transactions.Keys.Select(t =>
                                            $"{(t.IsWriting() ? "writing" : "reading")} from {t.CreatedTime:O}")));
            _disposed = true;
        }

        _modeLock.Wait();
        try
        {
            if (_isWriter)
            {
                StopWriterServicesAsync().GetAwaiter().GetResult();
                // The background services are stopped: finish WAL uploads, flushes and due compactions here.
                while (UploadWalAsync().AsTask().GetAwaiter().GetResult())
                {
                }

                RotateForShutdown();
                RunAllWork();
                DeleteObsoleteObjects();
                _storage.ReleaseLeaseAsync(LockKey, _leaseId, CancellationToken.None).AsTask().GetAwaiter()
                    .GetResult();
                _isWriter = false;
            }
        }
        finally
        {
            _modeLock.Release();
        }

        lock (_fullCompactionRequests)
        {
            foreach (var request in _fullCompactionRequests) request.TrySetCanceled();
            _fullCompactionRequests.Clear();
        }

        StopReaderHeartbeat();
        DeleteObsoleteObjects();
        var state = _state;
        state.Memtable.Arena?.Dispose();
        foreach (var sealedMemtable in state.Immutables) sealedMemtable.Memtable.Arena?.Dispose();
        if (_options.BlockCache == null) _cache.Dispose();
    }

    /// Simulates a crash for tests: background work stops, nothing pending is uploaded or flushed and the lease is
    /// kept until it expires.
    internal void SimulateCrash()
    {
        _disposed = true;
        _isWriter = false;
        StopWriterServicesAsync().GetAwaiter().GetResult();
        CompleteDurabilityWaiters(long.MaxValue, new LsmWriterFencedException("The writer crashed."));
    }
}
