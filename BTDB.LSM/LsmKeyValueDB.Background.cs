using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;
using BTDB.LSM.Storage;

namespace BTDB.LSM;

public sealed partial class LsmKeyValueDB
{
    readonly List<TaskCompletionSource> _fullCompactionRequests = [];
    readonly SemaphoreSlim _workSignal = new(0);
    volatile TaskCompletionSource _progress = NewProgress();
    Task? _worker;
    Exception? _workerError;
    long _throttleBytes;

    // Background work counters reported by CalcStats; written only by the unit runner.
    long _flushCount;
    long _flushTicks;
    long _flushBytes;
    long _compactionCount;
    long _compactionTicks;
    long _compactionBytes;
    long _tableBytesWritten;
    long _throttleStart = Stopwatch.GetTimestamp();

    static TaskCompletionSource NewProgress() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    void NotifyProgress() => Interlocked.Exchange(ref _progress, NewProgress()).TrySetResult();

    void RequestWork()
    {
        if (_worker == null) RunAllWork();
        else _workSignal.Release();
    }

    void RunAllWork()
    {
        while (RunOneUnit()) NotifyProgress();
        NotifyProgress();
    }

    async Task WorkerLoop(CancellationToken stop)
    {
        var backoff = TimeSpan.FromMilliseconds(50);
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await _workSignal.WaitAsync(TimeSpan.FromSeconds(1), stop);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            DeleteObsoleteObjects();
            while (!stop.IsCancellationRequested)
            {
                bool worked;
                try
                {
                    worked = RunOneUnit();
                    _workerError = null;
                    backoff = TimeSpan.FromMilliseconds(50);
                }
                catch (LsmWriterFencedException ex)
                {
                    _ = LoseWriterAsync(ex);
                    return;
                }
                catch (Exception ex)
                {
                    _workerError = ex;
                    Logger?.ReportCompactorException(ex);
                    NotifyProgress();
                    try
                    {
                        await Task.Delay(backoff, stop);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }

                    backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(5).Ticks));
                    continue;
                }

                NotifyProgress();
                if (!worked) break;
            }
        }
    }

    /// Seals the current memtable if it holds data or metadata not yet in a table or the manifest. Requires the
    /// writer slot.
    void RotateHoldingWriter()
    {
        if (!RotateIfNeeded()) return;
        RequestWork();
    }

    void RotateForShutdown() => RotateIfNeeded();

    bool RotateIfNeeded()
    {
        lock (_stateLock)
        {
            var current = _state;
            var metadataPersisted = current.Immutables.Length == 0 &&
                                    current.CommitUlong == _lastManifest.CommitUlong &&
                                    current.Ulongs.AsSpan().SequenceEqual(_lastManifest.Ulongs) &&
                                    current.TransactionNumber == _lastManifest.TransactionNumber;
            if (current.Memtable.IsEmpty && metadataPersisted) return false;
            _state = new(NewMemtable(_options), current.MemtableGeneration + 1,
                [
                    new(current.Memtable, current.MemtableGeneration, current.CommitUlong, current.Ulongs,
                        current.TransactionNumber),
                    .. current.Immutables
                ], current.Version, current.CommitUlong, current.Ulongs, current.TransactionNumber,
                current.CommitSequence, current.Sequence + 1, current.ManifestSequence);
            return true;
        }
    }

    async Task WaitForFlushesAsync()
    {
        while (_state.Immutables.Length > 0)
        {
            var progress = _progress.Task;
            if (_state.Immutables.Length == 0) break;
            if (!_isWriter) throw new LsmWriterFencedException("The writer lease was lost before the flush finished.");
            await Task.WhenAny(progress, Task.Delay(100));
        }
    }

    /// One unit of background work: flush the oldest sealed memtable, run a requested full compaction, or run one
    /// picked compaction. Only one thread runs units at a time, so only it changes the table set.
    bool RunOneUnit()
    {
        var state = _state;
        if (state.Immutables.Length > 0)
        {
            var flushStart = Stopwatch.GetTimestamp();
            var flushBytesBefore = _tableBytesWritten;
            FlushSealed(state.Immutables[^1]);
            _flushCount++;
            _flushTicks += Stopwatch.GetTimestamp() - flushStart;
            _flushBytes += _tableBytesWritten - flushBytesBefore;
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
        var start = Stopwatch.GetTimestamp();
        var bytesBefore = _tableBytesWritten;
        var created = new List<(ulong FileNumber, long Bytes)>();
        var (version, obsolete) = LsmCompaction.Execute(state.Version, plan, _compactionOptions, _tableOptions,
            WriteTable, NewValueSeparation(state.Version, created));
        PublishVersion(version, obsolete, created, null);
        _compactionCount++;
        _compactionTicks += Stopwatch.GetTimestamp() - start;
        _compactionBytes += _tableBytesWritten - bytesBefore;
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

    /// Publishes a new table set: writes the manifest, then replaces the version (and drops the flushed sealed
    /// memtable) in the current state. Replaced tables, value files no table references and WAL chunks now covered by
    /// tables are deleted, and a flushed memtable's arena released, once no snapshot can need them.
    void PublishVersion(LsmVersion version, IReadOnlyCollection<TableReader> obsolete,
        IEnumerable<(ulong FileNumber, long Bytes)> createdValueFiles, SealedMemtable? flushed)
    {
        version = version.WithValueFiles(createdValueFiles, out var droppedValueFiles);
        var previousWalStart = _lastManifest.WalStartChunk;
        if (flushed != null)
        {
            long walStart;
            lock (_walLock)
                walStart = _generationFirstChunk.TryGetValue(flushed.Generation + 1, out var first)
                    ? first
                    : _walNextChunk;
            WriteManifest(version, flushed.CommitUlong, flushed.Ulongs, flushed.TransactionNumber,
                Math.Max(_lastManifest.FlushedGeneration, flushed.Generation),
                Math.Max(previousWalStart, walStart));
        }
        else
        {
            WriteManifest(version, _lastManifest.CommitUlong, _lastManifest.Ulongs, _lastManifest.TransactionNumber,
                _lastManifest.FlushedGeneration, previousWalStart);
        }

        long sequence;
        lock (_stateLock)
        {
            var current = _state;
            var immutables = flushed == null
                ? current.Immutables
                : current.Immutables.Where(m => m != flushed).ToArray();
            sequence = current.Sequence + 1;
            _state = new(current.Memtable, current.MemtableGeneration, immutables, version, current.CommitUlong,
                current.Ulongs, current.TransactionNumber, current.CommitSequence, sequence,
                _lastManifest.Sequence);
        }

        var now = _options.TimeProvider.GetUtcNow();
        var manifestSequence = _lastManifest.Sequence;
        lock (_obsoleteObjects)
        {
            if (flushed?.Memtable.Arena is { } arena) _retiredArenas.Add((arena, sequence));
            foreach (var table in obsolete)
                _obsoleteObjects.Add((table.ObjectKey, table.FileNumber, sequence, now, manifestSequence));
            foreach (var fileNumber in droppedValueFiles)
                _obsoleteObjects.Add((ValueKey(fileNumber), fileNumber, sequence, now, manifestSequence));
            // Snapshots never read the WAL and a reader finding a covered chunk gone reads the newer manifest
            // instead, so covered chunks can go at once.
            for (var chunk = previousWalStart; chunk < _lastManifest.WalStartChunk; chunk++)
                _obsoleteObjects.Add((WalKey(_options.Prefix, chunk), 0, 0, DateTimeOffset.MinValue, 0));
        }

        DeleteObsoleteObjects();
    }

    byte[] ReadValue(ValueHandle handle)
    {
        using var pin = _cache.Pin(handle.FileNumber, handle.Offset, () =>
        {
            var key = ValueKey(handle.FileNumber);
            var record = new byte[handle.StoredLength];
            LsmObjects.ReadRange(_storage, key, handle.Offset, record, "Value file");
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
        _tableBytesWritten += content.Length;
        return TableReader.Open(_storage, _cache, fileNumber, key, content.Length, _options.TableTailReadSize);
    }

    void WriteManifest(LsmVersion version, ulong commitUlong, ulong[] ulongs, long transactionNumber,
        long flushedGeneration, long walStartChunk)
    {
        var manifest = new LsmManifest
        {
            Sequence = ++_manifestSequence, NextFileNumber = _nextFileNumber, TransactionNumber = transactionNumber,
            CommitUlong = commitUlong, Ulongs = ulongs, LevelCount = version.LevelCount, WriterEpoch = _epoch,
            FlushedGeneration = flushedGeneration, WalStartChunk = walStartChunk,
            Tables = version.AllTables().Select(t => (t.Level, t.Table.FileNumber, t.Table.Length)).ToArray(),
            ValueFiles = version.ValueFiles.Select(v => (v.Key, v.Value)).OrderBy(v => v.Key).ToArray()
        };
        CreateObject(ManifestKey(manifest.Sequence), manifest.Encode(), true);
        var previous = _lastManifest;
        _lastManifest = manifest;
        // Readers probe manifest names in order, so older manifests stay for the grace period.
        if (previous.Sequence > 0)
            lock (_obsoleteObjects)
                _obsoleteObjects.Add((ManifestKey(previous.Sequence), 0, 0, _options.TimeProvider.GetUtcNow(), 0));
        Logger?.KeyValueIndexCreated((uint)manifest.Sequence, version.Components.Sum(c => c.EntryCount), 0,
            TimeSpan.Zero, 0);
    }

    /// Creates an immutable object as the writer. For a manifest, a different object under the name means another
    /// writer took over; a lost response is reconciled by reading the object back.
    void CreateObject(string key, byte[] content, bool fence = false)
    {
        CheckLeaseDeadline();
        Throttle(content.Length);
        while (true)
        {
            var outcome = _storage.CreateAsync(key, content, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            if (outcome == LsmWriteOutcome.Applied) return;
            var existing = _storage.TryReadAsync(key, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            if (existing != null && existing.AsSpan().SequenceEqual(content)) return;
            if (existing != null || outcome == LsmWriteOutcome.Rejected)
            {
                if (fence) throw new LsmWriterFencedException($"{key} was written by another writer.");
                throw new BTDBException($"Creating {key} returned {outcome}; another writer may own the database.");
            }
        }
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

    void DeleteObsoleteObjects()
    {
        List<(string Key, ulong FileNumber, long ObsoleteAt, DateTimeOffset Since, long Manifest)> deletable;
        var now = _options.TimeProvider.GetUtcNow();
        var grace = _options.DeletionGracePeriod;
        bool HasLapsed((string, ulong, long, DateTimeOffset Since, long) o) => now - o.Since >= grace;
        DateTimeOffset? newestLapsed = null;
        lock (_obsoleteObjects)
        {
            if (_obsoleteObjects.Count == 0 && _retiredArenas.Count == 0) return;
            if (!_isWriter && _retiredArenas.Count == 0) return;
            if (_isWriter)
                foreach (var o in _obsoleteObjects)
                    if (HasLapsed(o) && (newestLapsed == null || o.Since > newestLapsed))
                        newestLapsed = o.Since;
        }

        // Readers in other processes are protected by the grace period and, when registered, by their entries,
        // read after the objects became obsolete.
        var oldestRegistered = newestLapsed is { } since ? OldestRegisteredManifest(since) : long.MaxValue;
        lock (_obsoleteObjects)
        {
            var oldestLive = long.MaxValue;
            foreach (var transaction in _transactions.Keys)
                oldestLive = Math.Min(oldestLive, transaction.StateSequence);
            bool Deletable((string, ulong, long ObsoleteAt, DateTimeOffset Since, long Manifest) o) =>
                _isWriter && o.ObsoleteAt <= oldestLive && HasLapsed(o) && o.Manifest <= oldestRegistered;
            deletable = _obsoleteObjects.Where(Deletable).ToList();
            _obsoleteObjects.RemoveAll(Deletable);
            // A memtable's native memory is released once no snapshot can reach any version of it.
            foreach (var (arena, _) in _retiredArenas.Where(a => a.ObsoleteAt <= oldestLive)) arena.Dispose();
            _retiredArenas.RemoveAll(a => a.ObsoleteAt <= oldestLive);
        }

        foreach (var (key, fileNumber, _, _, _) in deletable)
        {
            if (fileNumber != 0) _cache.Evict(fileNumber);
            _storage.DeleteAsync(key, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            if (fileNumber != 0) Logger?.FileMarkedForDelete((uint)fileNumber);
        }
    }
}
