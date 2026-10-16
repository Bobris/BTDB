using System;
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
/// (BTDB.LSM/ImplementationPlan.md). Phase L1: memtable, tables, manifest, flush and a simple merge of all tables; no
/// WAL yet, so only flushed data survives reopening. Flush and leveled compaction run synchronously while the writer
/// slot is held.
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
    readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new();
    readonly ConcurrentDictionary<LsmKeyValueDBTransaction, bool> _transactions = new();
    readonly List<(string Key, ulong FileNumber, long ObsoleteAt)> _obsoleteObjects = [];
    readonly List<(NativeArena Arena, long ObsoleteAt)> _retiredArenas = [];
    volatile LsmState _state;
    LsmKeyValueDBTransaction? _writingTransaction;
    LsmManifest _lastManifest;
    ulong _nextFileNumber;
    bool _disposed;

    LsmKeyValueDB(LsmKeyValueDBOptions options, LsmBlockCache cache, LsmState state, LsmManifest manifest)
    {
        _options = options;
        _storage = options.Storage;
        _cache = cache;
        _tableOptions = new()
        {
            BlockSize = options.BlockSize, RestartInterval = options.RestartInterval,
            BloomBitsPerKey = options.BloomBitsPerKey, Compress = options.CompressBlocks
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
        _nextFileNumber = manifest.NextFileNumber;
        MaxTrLogFileSize = options.MemtableSize;
        Logger = options.Logger;
    }

    public static async ValueTask<LsmKeyValueDB> OpenAsync(LsmKeyValueDBOptions options,
        CancellationToken cancellation = default)
    {
        var cache = options.BlockCache ?? new LsmBlockCache(256L * 1024 * 1024);
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
        var state = new LsmState(NewMemtable(options), version, manifest.CommitUlong, manifest.Ulongs,
            manifest.TransactionNumber, manifest.Sequence);
        return new(options, cache, state, manifest);
    }

    static string TableKey(string prefix, ulong fileNumber) => $"{prefix}tables/{fileNumber:D16}.sst";

    static IMemtable NewMemtable(LsmKeyValueDBOptions options) =>
        options.NativeMemtable ? NativeMemtable.CreateEmpty(options.MemtableSize) : Memtable.CreateEmpty();

    string ValueKey(ulong fileNumber) => $"{_options.Prefix}values/{fileNumber:D16}.vlf";

    string ManifestKey(long sequence) => $"{_options.Prefix}manifest/{sequence:D16}";

    internal LsmKeyValueDBOptions Options => _options;

    internal static long NewOwner() => Interlocked.Increment(ref _lastOwner);

    public bool DurableTransactions { get; set; }

    public IKeyValueDBTransaction StartTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Register(new(this, _state, false, false));
    }

    public IKeyValueDBTransaction StartReadOnlyTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Register(new(this, _state, false, true));
    }

    /// inBatch is ignored: every commit is published (ImplementationPlan.md, decision D4).
    public ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_writeLock)
        {
            if (_writingTransaction == null) return new(NewWritingTransactionUnsafe());
            var waiter = new TaskCompletionSource<IKeyValueDBTransaction>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _writeWaitingQueue.Enqueue(waiter);
            return new(waiter.Task);
        }
    }

    LsmKeyValueDBTransaction Register(LsmKeyValueDBTransaction transaction)
    {
        _transactions.TryAdd(transaction, false);
        return transaction;
    }

    LsmKeyValueDBTransaction NewWritingTransactionUnsafe()
    {
        var transaction = new LsmKeyValueDBTransaction(this, _state, true, false);
        _writingTransaction = transaction;
        return Register(transaction);
    }

    internal void MakeWritable(LsmKeyValueDBTransaction transaction, LsmState state)
    {
        lock (_writeLock)
        {
            if (_writingTransaction != null)
                throw new BTDBTransactionRetryException("Another writing transaction already running");
            if (_state != state)
                throw new BTDBTransactionRetryException("Another writing transaction already finished");
            _writingTransaction = transaction;
        }
    }

    internal void CommitWritingTransaction(IMemtable memtable, ulong commitUlong, ulong[] ulongs,
        long transactionNumber, bool persist)
    {
        var current = _state;
        _state = new(memtable, current.Version, commitUlong, ulongs, transactionNumber, current.Sequence + 1);
        try
        {
            if (persist || memtable.ApproximateBytes >= MaxTrLogFileSize) FlushHoldingWriter();
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

    /// Writes the memtable to a new level 0 table, runs the compactions it makes due and writes a manifest
    /// checkpoint. Must run while the writer slot is held or no writer can start.
    void FlushHoldingWriter()
    {
        var state = _state;
        var version = state.Version;
        var createdValueFiles = new List<(ulong FileNumber, long Bytes)>();
        if (!state.Memtable.IsEmpty)
        {
            var values = NewValueSeparation(version, createdValueFiles);
            var table = WriteTable(TableBuilder.Build(state.Memtable.Enumerate().Select(e => values.Apply(e)),
                state.Memtable.Tombstones, _tableOptions));
            values.Finish();
            version = version.WithLevel0([table, .. version.Level0]);
            Logger?.TransactionLogCreated((uint)table.FileNumber);
        }
        else if (state.CommitUlong == _lastManifest.CommitUlong && state.Ulongs.AsSpan().SequenceEqual(
                     _lastManifest.Ulongs) && state.TransactionNumber == _lastManifest.TransactionNumber)
        {
            return;
        }

        var obsolete = new List<TableReader>();
        while (LsmCompaction.Pick(version, _compactionOptions, _compactionPointers) is { } plan)
        {
            Logger?.CompactionStart(0);
            (version, var replaced) = LsmCompaction.Execute(version, plan, _compactionOptions, _tableOptions,
                WriteTable, NewValueSeparation(version, createdValueFiles));
            obsolete.AddRange(replaced);
        }

        Publish(new(NewMemtable(_options), version, state.CommitUlong, state.Ulongs, state.TransactionNumber,
            state.Sequence + 1), obsolete, createdValueFiles);
    }

    ValueSeparation NewValueSeparation(LsmVersion version, List<(ulong FileNumber, long Bytes)> created) =>
        new(_options.ValueSeparationThreshold, new ValueFileWriter(() => _nextFileNumber++, _options.TargetFileSize,
                (fileNumber, content) =>
                {
                    CreateObject(ValueKey(fileNumber), content);
                    created.Add((fileNumber, content.Length));
                }),
            fileNumber => version.LiveValueRatio(fileNumber) < _options.ValueGcThreshold, ReadValue);

    /// Publishes a state whose tables no longer use obsolete, adding created value files and dropping value files no
    /// table references; dropped objects are deleted once no snapshot can read them.
    void Publish(LsmState state, IReadOnlyCollection<TableReader> obsolete,
        IEnumerable<(ulong FileNumber, long Bytes)> createdValueFiles)
    {
        var version = state.Version.WithValueFiles(createdValueFiles, out var droppedValueFiles);
        var published = new LsmState(state.Memtable, version, state.CommitUlong, state.Ulongs,
            state.TransactionNumber, state.Sequence);
        WriteManifest(published);
        var retiredArena = _state.Memtable.Arena;
        _state = published;
        if (retiredArena != null && retiredArena != published.Memtable.Arena)
            lock (_obsoleteObjects)
                _retiredArenas.Add((retiredArena, published.Sequence));
        lock (_obsoleteObjects)
        {
            foreach (var table in obsolete)
                _obsoleteObjects.Add((table.ObjectKey, table.FileNumber, published.Sequence));
            foreach (var fileNumber in droppedValueFiles)
                _obsoleteObjects.Add((ValueKey(fileNumber), fileNumber, published.Sequence));
        }

        DeleteObsoleteObjects();
    }

    byte[] ReadValue(ValueHandle handle) => _cache.GetOrLoad(handle.FileNumber, handle.Offset, () =>
    {
        var key = ValueKey(handle.FileNumber);
        var record = new byte[handle.StoredLength];
        _storage.ReadRange(key, handle.Offset, record);
        return new CachedValue(ValueFileWriter.VerifyRecord(record, key));
    }).Data;

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

    void WriteManifest(LsmState state)
    {
        var manifest = new LsmManifest
        {
            Sequence = state.Sequence, NextFileNumber = _nextFileNumber, TransactionNumber = state.TransactionNumber,
            CommitUlong = state.CommitUlong, Ulongs = state.Ulongs, LevelCount = state.Version.LevelCount,
            Tables = state.Version.AllTables().Select(t => (t.Level, t.Table.FileNumber, t.Table.Length)).ToArray(),
            ValueFiles = state.Version.ValueFiles.Select(v => (v.Key, v.Value)).OrderBy(v => v.Key).ToArray()
        };
        CreateObject(ManifestKey(state.Sequence), manifest.Encode());
        var previous = _lastManifest;
        _lastManifest = manifest;
        if (previous.Sequence > 0 || previous.Tables.Length > 0)
            _storage.DeleteAsync(ManifestKey(previous.Sequence), CancellationToken.None).AsTask().GetAwaiter()
                .GetResult();
        Logger?.KeyValueIndexCreated((uint)state.Sequence, state.Memtable.EntryCount, 0, TimeSpan.Zero, 0);
    }

    void CreateObject(string key, byte[] content)
    {
        var outcome = _storage.CreateAsync(key, content, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        if (outcome != LsmWriteOutcome.Applied)
            throw new BTDBException($"Creating {key} returned {outcome}; another writer may own the database.");
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

    public async ValueTask FlushAsync(CancellationToken cancellation = default)
    {
        var transaction = (LsmKeyValueDBTransaction)await StartWritingTransaction();
        try
        {
            FlushHoldingWriter();
        }
        finally
        {
            transaction.Dispose();
        }
    }

    public string CalcStats()
    {
        var state = _state;
        var builder = new StringBuilder();
        builder.Append("KeyValueCount:").Append(state.CreateView(state.Memtable).Count).AppendLine();
        builder.Append("MemtableEntries:").Append(state.Memtable.EntryCount).AppendLine();
        builder.Append("MemtableBytes:").Append(state.Memtable.ApproximateBytes).AppendLine();
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
        return builder.ToString();
    }

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats() =>
        (0, 0, 0, 0);

    /// Flushes the memtable and merges every table into the bottom level. Returns false: no work is left after it.
    public async ValueTask<bool> Compact(CancellationToken cancellation)
    {
        var transaction = (LsmKeyValueDBTransaction)await StartWritingTransaction();
        try
        {
            FlushHoldingWriter();
            var state = _state;
            if (state.Version.AllTables().Count() <= 1 && state.Version.Level0.Length == 0) return false;
            Logger?.CompactionStart(0);
            var created = new List<(ulong FileNumber, long Bytes)>();
            var (version, obsolete) = LsmCompaction.CompactAll(state.Version, _compactionOptions, _tableOptions,
                WriteTable, NewValueSeparation(state.Version, created));
            Publish(new(state.Memtable, version, state.CommitUlong, state.Ulongs, state.TransactionNumber,
                state.Sequence + 1), obsolete, created);
            return false;
        }
        finally
        {
            transaction.Dispose();
        }
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
            FlushHoldingWriter();
            _disposed = true;
        }

        DeleteObsoleteObjects();
        _state.Memtable.Arena?.Dispose();
    }
}
