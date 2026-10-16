using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
/// (BTDB.LSM/ImplementationPlan.md). A full memtable is sealed at commit; a single background worker flushes sealed
/// memtables to level 0 and runs leveled compaction, and writers stall while too much work is pending. Without the WAL
/// (phase L3) only flushed data survives reopening.
public sealed class LsmKeyValueDB : ILsmKeyValueDB
{
    static long _lastOwner;

    readonly LsmKeyValueDBOptions _options;
    readonly ILsmStorage _storage;
    readonly LsmBlockCache _cache;
    readonly TableBuildOptions _tableOptions;
    readonly LsmCompactionOptions _compactionOptions;
    readonly byte[]?[] _compactionPointers;
    readonly Lock _writeLock = new();
    readonly Lock _stateLock = new();
    readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new();
    readonly ConcurrentDictionary<LsmKeyValueDBTransaction, bool> _transactions = new();
    readonly List<(string Key, ulong FileNumber, long ObsoleteAt)> _obsoleteObjects = [];
    readonly List<(NativeArena Arena, long ObsoleteAt)> _retiredArenas = [];
    readonly List<TaskCompletionSource> _fullCompactionRequests = [];
    readonly SemaphoreSlim _workSignal = new(0);
    readonly Task? _worker;
    volatile LsmState _state;
    volatile TaskCompletionSource _progress = NewProgress();
    volatile bool _stopping;
    LsmKeyValueDBTransaction? _writingTransaction;
    LsmManifest _lastManifest;
    long _manifestSequence;
    ulong _nextFileNumber;
    long _throttleBytes;
    long _throttleStart = Stopwatch.GetTimestamp();
    bool _disposed;

    LsmKeyValueDB(LsmKeyValueDBOptions options, LsmBlockCache cache, LsmState state, LsmManifest manifest)
    {
        _options = options;
        _storage = options.Storage;
        _cache = cache;
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
        _compactionPointers = new byte[]?[state.Version.LevelCount];
        _state = state;
        _lastManifest = manifest;
        _manifestSequence = manifest.Sequence;
        _nextFileNumber = manifest.NextFileNumber;
        MaxTrLogFileSize = options.MemtableSize;
        Logger = options.Logger;
        if (options.BackgroundCompaction) _worker = Task.Factory.StartNew(WorkerLoop, CancellationToken.None,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
    }

    public static async ValueTask<LsmKeyValueDB> OpenAsync(LsmKeyValueDBOptions options,
        CancellationToken cancellation = default)
    {
        var cache = options.BlockCache ?? new LsmBlockCache(options.BlockCacheSize);
        string? manifestKey = null;
        await foreach (var info in options.Storage.ListAsync(options.Prefix + "manifest/", cancellation))
            manifestKey = info.Key;
        var manifest = new LsmManifest { NextFileNumber = 1 };
        if (manifestKey != null)
        {
            var content = await options.Storage.TryReadAsync(manifestKey, cancellation) ??
                          throw new BTDBException($"Manifest {manifestKey} disappeared while opening.");
            manifest = LsmManifest.Decode(content, manifestKey);
        }

        var levelCount = manifestKey == null ? options.LevelCount : manifest.LevelCount;
        var levels = Enumerable.Range(0, levelCount).Select(_ => new List<TableReader>()).ToArray();
        foreach (var (level, fileNumber, length) in manifest.Tables)
            levels[level].Add(TableReader.Open(options.Storage, cache, fileNumber, TableKey(options.Prefix, fileNumber),
                length, options.TableTailReadSize));
        var version = new LsmVersion([.. levels[0]], levels.Select(l => l.ToArray()).ToArray(),
            manifest.ValueFiles.ToDictionary(v => v.FileNumber, v => v.Length));
        var state = new LsmState(NewMemtable(options), [], version, manifest.CommitUlong, manifest.Ulongs,
            manifest.TransactionNumber, 0, 0);
        return new(options, cache, state, manifest);
    }

    static string TableKey(string prefix, ulong fileNumber) => $"{prefix}tables/{fileNumber:D16}.sst";

    static IMemtable NewMemtable(LsmKeyValueDBOptions options) =>
        options.NativeMemtable ? NativeMemtable.CreateEmpty(options.MemtableSize) : Memtable.CreateEmpty();

    static TaskCompletionSource NewProgress() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    string ValueKey(ulong fileNumber) => $"{_options.Prefix}values/{fileNumber:D16}.vlf";

    string ManifestKey(long sequence) => $"{_options.Prefix}manifest/{sequence:D16}";

    internal LsmKeyValueDBOptions Options => _options;

    internal static long NewOwner() => Interlocked.Increment(ref _lastOwner);

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

    /// inBatch is ignored: every commit is published (ImplementationPlan.md, decision D4). Waits while flushes or level
    /// 0 compaction are too far behind.
    public async ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
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

    bool IsStalled()
    {
        if (_worker == null) return false;
        var state = _state;
        return state.Immutables.Length >= _options.MaxImmutableMemtables ||
               state.Version.Level0.Length >= _options.Level0StopWritesTrigger;
    }

    async ValueTask WaitWhileStalledAsync()
    {
        while (IsStalled() && !_stopping)
        {
            var progress = _progress.Task;
            if (!IsStalled()) break;
            await progress;
        }
    }

    /// Captures the current state and registers the transaction atomically with respect to the decision which
    /// obsolete objects no snapshot can read, so a snapshot never loses its tables, value files or arenas.
    LsmKeyValueDBTransaction Register(bool writing, bool readOnly)
    {
        lock (_obsoleteObjects)
        {
            var transaction = new LsmKeyValueDBTransaction(this, _state, writing, readOnly);
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
        lock (_writeLock)
        {
            if (_writingTransaction != null)
                throw new BTDBTransactionRetryException("Another writing transaction already running");
            if (_state.CommitSequence != state.CommitSequence)
                throw new BTDBTransactionRetryException("Another writing transaction already finished");
            _writingTransaction = transaction;
        }
    }

    /// Publishes a commit. The memtable's points were computed against the state below it in the writer's snapshot;
    /// flushes and compactions since then changed only the physical layout of that state, never its content.
    internal void CommitWritingTransaction(IMemtable memtable, ulong commitUlong, ulong[] ulongs,
        long transactionNumber, bool persist)
    {
        try
        {
            var rotate = persist || memtable.ApproximateBytes >= MaxTrLogFileSize;
            lock (_stateLock)
            {
                var current = _state;
                _state = rotate
                    ? new(NewMemtable(_options),
                        [new(memtable, commitUlong, ulongs, transactionNumber), .. current.Immutables],
                        current.Version, commitUlong, ulongs, transactionNumber, current.CommitSequence + 1,
                        current.Sequence + 1)
                    : new(memtable, current.Immutables, current.Version, commitUlong, ulongs, transactionNumber,
                        current.CommitSequence + 1, current.Sequence + 1);
            }

            if (rotate) RequestWork();
            if (persist) WaitForFlushes();
        }
        finally
        {
            ReleaseWriter();
        }
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

    /// Seals the current memtable if it holds data or metadata not yet in a table or the manifest. Requires the
    /// writer slot.
    void RotateHoldingWriter()
    {
        lock (_stateLock)
        {
            var current = _state;
            var metadataPersisted = current.Immutables.Length == 0 &&
                                    current.CommitUlong == _lastManifest.CommitUlong &&
                                    current.Ulongs.AsSpan().SequenceEqual(_lastManifest.Ulongs) &&
                                    current.TransactionNumber == _lastManifest.TransactionNumber;
            if (current.Memtable.IsEmpty && metadataPersisted) return;
            _state = new(NewMemtable(_options),
                [new(current.Memtable, current.CommitUlong, current.Ulongs, current.TransactionNumber), ..
                    current.Immutables], current.Version, current.CommitUlong, current.Ulongs,
                current.TransactionNumber, current.CommitSequence, current.Sequence + 1);
        }

        RequestWork();
    }

    void RequestWork()
    {
        if (_worker == null) RunAllWork();
        else _workSignal.Release();
    }

    /// Blocks until every sealed memtable is flushed.
    void WaitForFlushes()
    {
        while (_state.Immutables.Length > 0)
        {
            var progress = _progress.Task;
            if (_state.Immutables.Length == 0) break;
            ThrowIfWorkerFailed();
            progress.Wait(TimeSpan.FromMilliseconds(100));
        }
    }

    async Task WaitForFlushesAsync()
    {
        while (_state.Immutables.Length > 0)
        {
            var progress = _progress.Task;
            if (_state.Immutables.Length == 0) break;
            ThrowIfWorkerFailed();
            await Task.WhenAny(progress, Task.Delay(100));
        }
    }

    Exception? _workerError;

    void ThrowIfWorkerFailed()
    {
        if (_workerError is { } error && _stopping)
            throw new BTDBException("BTDB.LSM background work failed: " + error.Message, error);
    }

    async Task WorkerLoop()
    {
        var backoff = TimeSpan.FromMilliseconds(50);
        while (!_stopping)
        {
            await _workSignal.WaitAsync(TimeSpan.FromSeconds(1));
            while (!_stopping)
            {
                bool worked;
                try
                {
                    worked = RunOneUnit();
                    _workerError = null;
                    backoff = TimeSpan.FromMilliseconds(50);
                }
                catch (Exception ex)
                {
                    _workerError = ex;
                    if (Logger?.ReportCompactorException(ex) == false) _stopping = true;
                    NotifyProgress();
                    await Task.Delay(backoff);
                    backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(5).Ticks));
                    continue;
                }

                NotifyProgress();
                if (!worked) break;
            }
        }
    }

    void NotifyProgress() => Interlocked.Exchange(ref _progress, NewProgress()).TrySetResult();

    void RunAllWork()
    {
        while (RunOneUnit()) NotifyProgress();
        NotifyProgress();
    }

    /// One unit of background work: flush the oldest sealed memtable, run a requested full compaction, or run one
    /// picked compaction. Only one thread runs units at a time, so only it changes the table set.
    bool RunOneUnit()
    {
        var state = _state;
        if (state.Immutables.Length > 0)
        {
            FlushSealed(state.Immutables[^1]);
            return true;
        }

        TaskCompletionSource[] requests;
        lock (_fullCompactionRequests)
        {
            requests = [.. _fullCompactionRequests];
            _fullCompactionRequests.Clear();
        }

        if (requests.Length > 0)
        {
            try
            {
                CompactAllNow();
                foreach (var request in requests) request.TrySetResult();
            }
            catch (Exception ex)
            {
                foreach (var request in requests) request.TrySetException(ex);
                throw;
            }

            return true;
        }

        if (LsmCompaction.Pick(state.Version, _compactionOptions, _compactionPointers) is not { } plan) return false;
        Logger?.CompactionStart(0);
        var created = new List<(ulong FileNumber, long Bytes)>();
        var (version, obsolete) = LsmCompaction.Execute(state.Version, plan, _compactionOptions, _tableOptions,
            WriteTable, NewValueSeparation(state.Version, created));
        PublishVersion(version, obsolete, created, null);
        return true;
    }

    void FlushSealed(SealedMemtable sealedMemtable)
    {
        var version = _state.Version;
        var created = new List<(ulong FileNumber, long Bytes)>();
        var memtable = sealedMemtable.Memtable;
        if (!memtable.IsEmpty)
        {
            var values = NewValueSeparation(version, created);
            var table = WriteTable(TableBuilder.Build(memtable.Enumerate().Select(e => values.Apply(e)),
                memtable.Tombstones, _tableOptions));
            values.Finish();
            version = version.WithLevel0([table, .. version.Level0]);
            Logger?.TransactionLogCreated((uint)table.FileNumber);
        }

        PublishVersion(version, [], created, sealedMemtable);
    }

    void CompactAllNow()
    {
        var state = _state;
        if (state.Version.AllTables().Count() <= 1 && state.Version.Level0.Length == 0) return;
        Logger?.CompactionStart(0);
        var created = new List<(ulong FileNumber, long Bytes)>();
        var (version, obsolete) = LsmCompaction.CompactAll(state.Version, _compactionOptions, _tableOptions,
            WriteTable, NewValueSeparation(state.Version, created));
        PublishVersion(version, obsolete, created, null);
    }

    ValueSeparation NewValueSeparation(LsmVersion version, List<(ulong FileNumber, long Bytes)> created) =>
        new(_options.ValueSeparationThreshold, new ValueFileWriter(() => _nextFileNumber++, _options.TargetFileSize,
                (fileNumber, content) =>
                {
                    CreateObject(ValueKey(fileNumber), content);
                    created.Add((fileNumber, content.Length));
                }),
            fileNumber => version.LiveValueRatio(fileNumber) < _options.ValueGcThreshold, ReadValue);

    /// Publishes a new table set: writes the manifest, then replaces the version (and the flushed sealed memtable)
    /// in the current state. Value files no table references and replaced tables are deleted, and a flushed
    /// memtable's arena released, once no snapshot can read them.
    void PublishVersion(LsmVersion version, IReadOnlyCollection<TableReader> obsolete,
        IEnumerable<(ulong FileNumber, long Bytes)> createdValueFiles, SealedMemtable? flushed)
    {
        version = version.WithValueFiles(createdValueFiles, out var droppedValueFiles);
        var persisted = flushed != null
            ? (flushed.CommitUlong, flushed.Ulongs, flushed.TransactionNumber)
            : (_lastManifest.CommitUlong, _lastManifest.Ulongs, _lastManifest.TransactionNumber);
        WriteManifest(version, persisted.CommitUlong, persisted.Ulongs, persisted.TransactionNumber);
        long sequence;
        lock (_stateLock)
        {
            var current = _state;
            var immutables = flushed == null ? current.Immutables : current.Immutables.Where(m => m != flushed).ToArray();
            sequence = current.Sequence + 1;
            _state = new(current.Memtable, immutables, version, current.CommitUlong, current.Ulongs,
                current.TransactionNumber, current.CommitSequence, sequence);
        }

        lock (_obsoleteObjects)
        {
            if (flushed?.Memtable.Arena is { } arena) _retiredArenas.Add((arena, sequence));
            foreach (var table in obsolete) _obsoleteObjects.Add((table.ObjectKey, table.FileNumber, sequence));
            foreach (var fileNumber in droppedValueFiles)
                _obsoleteObjects.Add((ValueKey(fileNumber), fileNumber, sequence));
        }

        DeleteObsoleteObjects();
    }

    byte[] ReadValue(ValueHandle handle)
    {
        using var pin = _cache.Pin(handle.FileNumber, handle.Offset, () =>
        {
            var key = ValueKey(handle.FileNumber);
            var record = new byte[handle.StoredLength];
            _storage.ReadRange(key, handle.Offset, record);
            return ValueFileWriter.VerifyRecord(record, key);
        });
        return pin.Span.ToArray();
    }

    /// The bytes of a stored value, reading a separated value from its value file.
    internal ReadOnlyMemory<byte> Resolve(LsmValue value) =>
        value.IsSeparated ? ReadValue(ValueHandle.Decode(value.Data.Span)) : value.Data;

    TableReader WriteTable(byte[] content)
    {
        var fileNumber = _nextFileNumber++;
        var key = TableKey(_options.Prefix, fileNumber);
        CreateObject(key, content);
        return TableReader.Open(_storage, _cache, fileNumber, key, content.Length, _options.TableTailReadSize);
    }

    void WriteManifest(LsmVersion version, ulong commitUlong, ulong[] ulongs, long transactionNumber)
    {
        var manifest = new LsmManifest
        {
            Sequence = ++_manifestSequence, NextFileNumber = _nextFileNumber, TransactionNumber = transactionNumber,
            CommitUlong = commitUlong, Ulongs = ulongs, LevelCount = version.LevelCount,
            Tables = version.AllTables().Select(t => (t.Level, t.Table.FileNumber, t.Table.Length)).ToArray(),
            ValueFiles = version.ValueFiles.Select(v => (v.Key, v.Value)).OrderBy(v => v.Key).ToArray()
        };
        CreateObject(ManifestKey(manifest.Sequence), manifest.Encode());
        var previous = _lastManifest;
        _lastManifest = manifest;
        if (previous.Sequence > 0)
            _storage.DeleteAsync(ManifestKey(previous.Sequence), CancellationToken.None).AsTask().GetAwaiter()
                .GetResult();
        Logger?.KeyValueIndexCreated((uint)manifest.Sequence, version.Components.Sum(c => c.EntryCount), 0,
            TimeSpan.Zero, 0);
    }

    void CreateObject(string key, byte[] content)
    {
        Throttle(content.Length);
        var outcome = _storage.CreateAsync(key, content, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        if (outcome != LsmWriteOutcome.Applied)
            throw new BTDBException($"Creating {key} returned {outcome}; another writer may own the database.");
    }

    /// Paces object creation to CompactorWriteBytesPerSecondLimit.
    void Throttle(long bytes)
    {
        var limit = CompactorWriteBytesPerSecondLimit;
        if (limit == 0) return;
        _throttleBytes += bytes;
        var due = TimeSpan.FromSeconds((double)_throttleBytes / limit);
        var elapsed = Stopwatch.GetElapsedTime(_throttleStart);
        if (elapsed > TimeSpan.FromSeconds(10))
        {
            _throttleStart = Stopwatch.GetTimestamp();
            _throttleBytes = 0;
        }
        else if (due > elapsed) Thread.Sleep(due - elapsed);
    }

    internal void TransactionDisposed(LsmKeyValueDBTransaction transaction)
    {
        _transactions.TryRemove(transaction, out _);
        DeleteObsoleteObjects();
    }

    void DeleteObsoleteObjects()
    {
        List<(string Key, ulong FileNumber, long ObsoleteAt)> deletable;
        lock (_obsoleteObjects)
        {
            if (_obsoleteObjects.Count == 0 && _retiredArenas.Count == 0) return;
            var oldestLive = long.MaxValue;
            foreach (var transaction in _transactions.Keys)
                oldestLive = Math.Min(oldestLive, transaction.StateSequence);
            deletable = _obsoleteObjects.Where(o => o.ObsoleteAt <= oldestLive).ToList();
            _obsoleteObjects.RemoveAll(o => o.ObsoleteAt <= oldestLive);
            // A memtable's native memory is released once no snapshot can reach any version of it.
            foreach (var (arena, _) in _retiredArenas.Where(a => a.ObsoleteAt <= oldestLive)) arena.Dispose();
            _retiredArenas.RemoveAll(a => a.ObsoleteAt <= oldestLive);
        }

        foreach (var (key, fileNumber, _) in deletable)
        {
            _cache.Evict(fileNumber);
            _storage.DeleteAsync(key, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            Logger?.FileMarkedForDelete((uint)fileNumber);
        }
    }

    /// Seals the memtable and waits until every sealed memtable is written to a table and the manifest.
    public async ValueTask FlushAsync(CancellationToken cancellation = default)
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

    public string CalcStats()
    {
        var state = _state;
        var builder = new StringBuilder();
        builder.Append("KeyValueCount:").Append(state.CreateView(state.Memtable).Count).AppendLine();
        builder.Append("MemtableEntries:").Append(state.Memtable.EntryCount).AppendLine();
        builder.Append("MemtableBytes:").Append(state.Memtable.ApproximateBytes).AppendLine();
        builder.Append("SealedMemtables:").Append(state.Immutables.Length).AppendLine();
        for (var level = 0; level < state.Version.LevelCount; level++)
        {
            var tables = level == 0 ? state.Version.Level0 : state.Version.Levels[level];
            if (tables.Length == 0) continue;
            builder.Append("Level").Append(level).Append(':').Append(tables.Length).Append(" tables, ")
                .Append(state.Version.LevelBytes(level)).Append(" bytes").AppendLine();
        }

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
        await FlushAsync(cancellation);
        var request = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_fullCompactionRequests) _fullCompactionRequests.Add(request);
        RequestWork();
        await request.Task;
        return false;
    }

    /// Flushes the memtable and writes a manifest checkpoint.
    public void CreateKvi(CancellationToken cancellation) => FlushAsync(cancellation).AsTask().GetAwaiter().GetResult();

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

    public void Dispose()
    {
        if (_disposed) return;
        lock (_writeLock)
        {
            if (_writingTransaction != null)
                throw new BTDBException("Cannot dispose KeyValueDB when writing transaction still running");
            while (_writeWaitingQueue.Count > 0) _writeWaitingQueue.Dequeue().TrySetCanceled();
            if (!_transactions.IsEmpty)
                throw new BTDBException("Cannot dispose KeyValueDB when transactions still running");
            _disposed = true;
        }

        _stopping = true;
        _workSignal.Release();
        _worker?.GetAwaiter().GetResult();
        // The worker is stopped: finish flushes and due compactions on this thread.
        lock (_stateLock)
        {
            var current = _state;
            var metadataPersisted = current.Immutables.Length == 0 &&
                                    current.CommitUlong == _lastManifest.CommitUlong &&
                                    current.Ulongs.AsSpan().SequenceEqual(_lastManifest.Ulongs) &&
                                    current.TransactionNumber == _lastManifest.TransactionNumber;
            if (!current.Memtable.IsEmpty || !metadataPersisted)
                _state = new(NewMemtable(_options),
                    [new(current.Memtable, current.CommitUlong, current.Ulongs, current.TransactionNumber), ..
                        current.Immutables], current.Version, current.CommitUlong, current.Ulongs,
                    current.TransactionNumber, current.CommitSequence, current.Sequence + 1);
        }

        RunAllWork();
        lock (_fullCompactionRequests)
        {
            foreach (var request in _fullCompactionRequests) request.TrySetCanceled();
            _fullCompactionRequests.Clear();
        }

        DeleteObsoleteObjects();
        _state.Memtable.Arena?.Dispose();
        if (_options.BlockCache == null) _cache.Dispose();
        _workSignal.Dispose();
    }
}
