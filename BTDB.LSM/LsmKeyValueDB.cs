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
using BTDB.LSM.Storage;

namespace BTDB.LSM;

/// Log-structured merge tree implementation of IKeyValueDB stored directly in object storage
/// (BTDB.LSM/ImplementationPlan.md). Phase L1: memtable, tables, manifest, flush and a simple merge of all tables; no
/// WAL yet, so only flushed data survives reopening. Flush and merge run synchronously while the writer slot is held.
/// Compaction is a size-tiered placeholder until leveled compaction in phase L2.
public sealed class LsmKeyValueDB : ILsmKeyValueDB
{
    static long _lastOwner;

    readonly LsmKeyValueDBOptions _options;
    readonly ILsmStorage _storage;
    readonly LsmBlockCache _cache;
    readonly TableBuildOptions _tableOptions;
    readonly Lock _writeLock = new();
    readonly Queue<TaskCompletionSource<IKeyValueDBTransaction>> _writeWaitingQueue = new();
    readonly ConcurrentDictionary<LsmKeyValueDBTransaction, bool> _transactions = new();
    readonly List<(TableReader Table, long ObsoleteAt)> _obsoleteTables = [];
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

        var tables = manifest.Tables.Select(t => TableReader.Open(options.Storage, cache, t.FileNumber,
            TableKey(options.Prefix, t.FileNumber), t.Length, options.TableTailReadSize)).ToArray();
        var state = new LsmState(Memtable.CreateEmpty(), tables, manifest.CommitUlong, manifest.Ulongs,
            manifest.TransactionNumber, manifest.Sequence);
        return new(options, cache, state, manifest);
    }

    static string TableKey(string prefix, ulong fileNumber) => $"{prefix}tables/{fileNumber:D16}.sst";

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

    internal void CommitWritingTransaction(Memtable memtable, ulong commitUlong, ulong[] ulongs,
        long transactionNumber, bool persist)
    {
        var current = _state;
        _state = new(memtable, current.Tables, commitUlong, ulongs, transactionNumber, current.Sequence + 1);
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

    /// Writes the memtable to a new table and a manifest checkpoint. Must run while the writer slot is held or no
    /// writer can start.
    void FlushHoldingWriter()
    {
        var state = _state;
        var tables = state.Tables;
        if (!state.Memtable.IsEmpty)
        {
            var fileNumber = _nextFileNumber++;
            var key = TableKey(_options.Prefix, fileNumber);
            var content = TableBuilder.Build(state.Memtable.Enumerate(), state.Memtable.Tombstones, _tableOptions);
            CreateObject(key, content);
            tables = [TableReader.Open(_storage, _cache, fileNumber, key, content.Length, _options.TableTailReadSize),
                ..tables];
            Logger?.TransactionLogCreated((uint)fileNumber);
        }
        else if (state.CommitUlong == _lastManifest.CommitUlong && state.Ulongs.AsSpan().SequenceEqual(
                     _lastManifest.Ulongs) && state.TransactionNumber == _lastManifest.TransactionNumber)
        {
            return;
        }

        var obsolete = Array.Empty<TableReader>();
        if (tables.Length > _options.MaxTables) (tables, obsolete) = MergeNewestRun(tables);
        var published = new LsmState(Memtable.CreateEmpty(), tables, state.CommitUlong, state.Ulongs,
            state.TransactionNumber, state.Sequence + 1);
        WriteManifest(published);
        _state = published;
        if (obsolete.Length > 0)
        {
            lock (_obsoleteTables)
                foreach (var table in obsolete)
                    _obsoleteTables.Add((table, published.Sequence));
            DeleteObsoleteTables();
        }
    }

    /// Size-tiered placeholder for leveled compaction (phase L2): merges the newest run of tables, extended while
    /// the next older table is at most twice the run's size, so total merge work stays O(n log n).
    (TableReader[] Tables, TableReader[] Obsolete) MergeNewestRun(TableReader[] tables)
    {
        var last = 1;
        var runSize = tables[0].Length + tables[1].Length;
        while (last + 1 < tables.Length && tables[last + 1].Length <= 2 * runSize)
        {
            last++;
            runSize += tables[last].Length;
        }

        return MergeRun(tables, last);
    }

    (TableReader[] Tables, TableReader[] Obsolete) MergeRun(TableReader[] tables, int last)
    {
        Logger?.CompactionStart(0);
        var run = tables[..(last + 1)];
        var below = tables[(last + 1)..];
        var (entries, tombstones) = LsmMerger.Merge(run, new LsmView(below), 0);
        if (entries.Count == 0 && tombstones.Count == 0) return (below, run);
        var fileNumber = _nextFileNumber++;
        var key = TableKey(_options.Prefix, fileNumber);
        var content = TableBuilder.Build(entries, tombstones, _tableOptions);
        CreateObject(key, content);
        var merged = TableReader.Open(_storage, _cache, fileNumber, key, content.Length, _options.TableTailReadSize);
        return ([merged, ..below], run);
    }

    void WriteManifest(LsmState state)
    {
        var manifest = new LsmManifest
        {
            Sequence = state.Sequence, NextFileNumber = _nextFileNumber, TransactionNumber = state.TransactionNumber,
            CommitUlong = state.CommitUlong, Ulongs = state.Ulongs,
            Tables = state.Tables.Select(t => (t.FileNumber, t.Length)).ToArray()
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
        DeleteObsoleteTables();
    }

    void DeleteObsoleteTables()
    {
        List<TableReader> deletable;
        lock (_obsoleteTables)
        {
            if (_obsoleteTables.Count == 0) return;
            var oldestLive = long.MaxValue;
            foreach (var transaction in _transactions.Keys)
                oldestLive = Math.Min(oldestLive, transaction.StateSequence);
            deletable = _obsoleteTables.Where(o => o.ObsoleteAt <= oldestLive).Select(o => o.Table).ToList();
            _obsoleteTables.RemoveAll(o => o.ObsoleteAt <= oldestLive);
        }

        foreach (var table in deletable)
        {
            table.EvictFromCache();
            _storage.DeleteAsync(table.ObjectKey, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            Logger?.FileMarkedForDelete((uint)table.FileNumber);
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
        builder.Append("Tables:").Append(state.Tables.Length).AppendLine();
        builder.Append("TableBytes:").Append(state.Tables.Sum(t => t.Length)).AppendLine();
        builder.Append("BlockCacheBytes:").Append(_cache.SizeBytes).AppendLine();
        builder.Append("BlockCacheHits:").Append(_cache.Hits).AppendLine();
        builder.Append("BlockCacheMisses:").Append(_cache.Misses).AppendLine();
        return builder.ToString();
    }

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats() =>
        (0, 0, 0, 0);

    /// Merges all tables into one. Returns false: there is never more work left after a full merge.
    public async ValueTask<bool> Compact(CancellationToken cancellation)
    {
        var transaction = (LsmKeyValueDBTransaction)await StartWritingTransaction();
        try
        {
            FlushHoldingWriter();
            var state = _state;
            if (state.Tables.Length <= 1) return false;
            var (tables, obsolete) = MergeRun(state.Tables, state.Tables.Length - 1);
            var published = new LsmState(state.Memtable, tables, state.CommitUlong, state.Ulongs,
                state.TransactionNumber, state.Sequence + 1);
            WriteManifest(published);
            _state = published;
            lock (_obsoleteTables)
                foreach (var table in obsolete)
                    _obsoleteTables.Add((table, published.Sequence));
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

        DeleteObsoleteTables();
    }
}
