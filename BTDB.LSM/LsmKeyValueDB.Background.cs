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
    DateTimeOffset _workerFailingSince;
    readonly BytePacer _writePacer = new();
    readonly BytePacer _readPacer = new();

    // Background work counters reported by CalcStats; written by whichever thread runs the units (the worker, or the
    // committing or disposing thread without one), never by two at once.
    long _flushCount;
    long _flushTicks;
    long _flushBytes;
    long _compactionCount;
    long _compactionTicks;
    long _compactionBytes;
    long _tableBytesWritten;

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
                    _workerFailingSince = DateTimeOffset.MaxValue;
                    backoff = TimeSpan.FromMilliseconds(50);
                }
                catch (LsmWriterFencedException ex)
                {
                    _ = LoseWriterAsync(ex);
                    return;
                }
                catch (Exception ex)
                {
                    if (_workerError == null) _workerFailingSince = _options.TimeProvider.GetUtcNow();
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

    bool RotateIfNeeded()
    {
        lock (_stateLock)
        {
            var current = _state;
            // An empty memtable whose metadata the newest sealed memtable or the manifest already carries holds
            // nothing to persist. Sealing it would create a generation without WAL chunks.
            var persisted = current.Immutables.Length > 0
                ? (current.Immutables[0].CommitUlong, current.Immutables[0].Ulongs,
                    current.Immutables[0].TransactionNumber)
                : (_lastManifest.CommitUlong, _lastManifest.Ulongs, _lastManifest.TransactionNumber);
            var metadataPersisted = current.CommitUlong == persisted.Item1 &&
                                    current.Ulongs.AsSpan().SequenceEqual(persisted.Item2) &&
                                    current.TransactionNumber == persisted.Item3;
            if (current.Memtable.IsEmpty && metadataPersisted) return false;
            _state = new(NewMemtable(_options), current.MemtableGeneration + 1,
                [
                    new(current.Memtable, current.MemtableGeneration, current.CommitUlong, current.Ulongs,
                        current.TransactionNumber),
                    .. current.Immutables
                ], current.Version, current.CommitUlong, current.Ulongs, current.TransactionNumber,
                // A new commit sequence makes a transaction that read the sealed memtable fail to upgrade to writing.
                current.CommitSequence + 1, current.Sequence + 1, current.ManifestSequence);
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
            ThrowIfWorkerKeepsFailing();
            await Task.WhenAny(progress, Task.Delay(100));
        }
    }

    /// Waiting callers give up when background work has failed for longer than WorkerFailureTimeout; transient
    /// storage errors are retried meanwhile.
    void ThrowIfWorkerKeepsFailing()
    {
        if (_workerError is { } error &&
            _options.TimeProvider.GetUtcNow() - _workerFailingSince > _options.WorkerFailureTimeout)
            throw new BTDBException("BTDB.LSM background work keeps failing: " + error.Message, error);
    }

    /// One unit of background work: flush the oldest sealed memtable, run a requested full compaction, or run one
    /// picked compaction. Only one thread runs units at a time, so only it changes the table set.
    bool RunOneUnit()
    {
        if (_orphanScanDue)
        {
            ScanForOrphans();
            _orphanScanDue = false;
            return true;
        }

        var state = _state;
        // A spilled transaction is published only after every sealed memtable is flushed below its tables.
        if (state.Immutables.Length == 0 && RunIngest()) return true;
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
            version = version.WithLevel0Runs([[table], .. version.Level0Runs]);
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

    ValueSeparation NewValueSeparation(LsmVersion version, List<(ulong FileNumber, long Bytes)> created,
        bool pace = true) =>
        new(_options.ValueSeparationThreshold, new ValueFileWriter(AllocateFileNumber, _options.TargetFileSize,
                (fileNumber, content) =>
                {
                    CreateObject(ValueKey(fileNumber), content, pace: pace);
                    created.Add((fileNumber, content.Length));
                }),
            fileNumber => version.LiveValueRatio(fileNumber) < _options.ValueGcThreshold, ReadValue);

    /// Publishes a new table set: writes the manifest, then replaces the version (and drops the flushed sealed
    /// memtable) in the current state. Replaced tables, value files no table references and WAL chunks now covered by
    /// tables are deleted, and a flushed memtable's arena released, once no snapshot can need them.
    /// With committed, flushed is the empty mutable memtable standing in for a spilled transaction: its generation is
    /// the transaction's, the manifest carries the transaction's commit metadata, and the next generation starts.
    void PublishVersion(LsmVersion version, IReadOnlyCollection<TableReader> obsolete,
        IEnumerable<(ulong FileNumber, long Bytes)> createdValueFiles, SealedMemtable? flushed, bool committed = false)
    {
        version = version.WithValueFiles(createdValueFiles, out var droppedValueFiles);
        var now = _options.TimeProvider.GetUtcNow();
        LsmPendingDeletion[] dropped =
        [
            .. obsolete.Select(t => new LsmPendingDeletion(LsmObjectKind.Table, t.FileNumber, now)),
            .. droppedValueFiles.Select(f => new LsmPendingDeletion(LsmObjectKind.ValueFile, f, now))
        ];
        var previousWalStart = _lastManifest.WalStartChunk;
        if (flushed != null)
        {
            // Recovery must start at the first chunk of any later generation; only when no later generation has a
            // chunk yet is the next chunk number the start. Generations up to the flushed one are no longer needed.
            // The start records the epoch of its writer, because replay starting there must still ignore chunks of
            // earlier writers.
            (long Chunk, long Epoch) walStart;
            lock (_walLock)
            {
                walStart = (_walNextChunk, _epoch);
                foreach (var (generation, first) in _generationFirstChunk)
                    if (generation > flushed.Generation && first.Chunk < walStart.Chunk)
                        walStart = first;
                foreach (var generation in _generationFirstChunk.Keys.Where(g => g <= flushed.Generation).ToArray())
                    _generationFirstChunk.Remove(generation);
            }
            if (previousWalStart >= walStart.Chunk) walStart = (previousWalStart, _lastManifest.WalStartEpoch);
            WriteManifest(version, flushed.CommitUlong, flushed.Ulongs, flushed.TransactionNumber,
                Math.Max(_lastManifest.FlushedGeneration, flushed.Generation), walStart.Chunk, walStart.Epoch,
                dropped, now);
        }
        else
        {
            WriteManifest(version, _lastManifest.CommitUlong, _lastManifest.Ulongs, _lastManifest.TransactionNumber,
                _lastManifest.FlushedGeneration, previousWalStart, _lastManifest.WalStartEpoch, dropped, now);
        }

        long sequence;
        lock (_stateLock)
        {
            var current = _state;
            var immutables = flushed == null
                ? current.Immutables
                : current.Immutables.Where(m => m != flushed).ToArray();
            sequence = current.Sequence + 1;
            _state = committed
                ? new(NewMemtable(_options), current.MemtableGeneration + 1, immutables, version, flushed!.CommitUlong,
                    flushed.Ulongs, flushed.TransactionNumber, current.CommitSequence + 1, sequence,
                    _lastManifest.Sequence)
                : new(current.Memtable, current.MemtableGeneration, immutables, version, current.CommitUlong,
                    current.Ulongs, current.TransactionNumber, current.CommitSequence, sequence,
                    _lastManifest.Sequence);
        }

        lock (_obsoleteObjects)
        {
            if (flushed?.Memtable.Arena is { } arena) _retiredArenas.Add((arena, sequence));
            foreach (var deletion in dropped) _obsoleteObjects.Add(new(deletion.Kind, deletion.Number, sequence, now));
            // Snapshots never read the WAL and a reader finding a covered chunk gone reads the newer manifest
            // instead, so covered chunks can go at once.
            for (var chunk = previousWalStart; chunk < _lastManifest.WalStartChunk; chunk++)
                _obsoleteObjects.Add(new(LsmObjectKind.WalChunk, (ulong)chunk, 0, DateTimeOffset.MinValue));
        }

        DeleteObsoleteObjects();
    }

    byte[] ReadValue(ValueHandle handle)
    {
        using var pin = _cache.Pin(new(_cacheScope, handle.FileNumber), handle.Offset, () =>
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

    /// Background work and a spilling writing transaction both create files, so numbers are taken atomically.
    ulong AllocateFileNumber() => Interlocked.Increment(ref _nextFileNumber) - 1;

    TableReader WriteTable(byte[] content) => WriteTable(content, true);

    TableReader WriteTable(byte[] content, bool pace)
    {
        var fileNumber = AllocateFileNumber();
        var key = TableKey(_options.Prefix, fileNumber);
        CreateObject(key, content, pace: pace);
        Interlocked.Add(ref _tableBytesWritten, content.Length);
        return TableReader.Open(_storage, _cache, _cacheScope, fileNumber, key, content.Length,
            _options.TableTailReadSize, content);
    }

    /// Writes the next manifest. It lists every pending deletion: those already waiting, the objects this edit drops
    /// and the manifest it replaces; the caller adds the dropped objects to the waiting ones once it published them.
    void WriteManifest(LsmVersion version, ulong commitUlong, ulong[] ulongs, long transactionNumber,
        long flushedGeneration, long walStartChunk, long walStartEpoch,
        IReadOnlyCollection<LsmPendingDeletion>? dropped = null,
        DateTimeOffset? now = null)
    {
        var previous = _lastManifest;
        var replacedAt = now ?? _options.TimeProvider.GetUtcNow();
        LsmPendingDeletion[] pending;
        lock (_obsoleteObjects)
            pending =
            [
                .. _obsoleteObjects.Where(o => o.Kind != LsmObjectKind.WalChunk)
                    .Select(o => new LsmPendingDeletion(o.Kind, o.Number, o.Since)),
                .. dropped ?? [],
                .. previous.Sequence > 0
                    ? [new LsmPendingDeletion(LsmObjectKind.Manifest, (ulong)previous.Sequence, replacedAt)]
                    : Array.Empty<LsmPendingDeletion>()
            ];
        // Rounding the times up to a 64th of the grace period keeps the manifest small: the manifests replaced
        // within one period form one run. A successor deletes those objects at most that much later.
        var period = Math.Max(1, _options.DeletionGracePeriod.Ticks / 64);
        for (var i = 0; i < pending.Length; i++)
        {
            var ticks = pending[i].Since.UtcTicks;
            var rounded = ticks % period == 0 ? ticks : Math.Min(ticks - ticks % period + period, DateTimeOffset.MaxValue.UtcTicks);
            pending[i] = pending[i] with { Since = new(rounded, TimeSpan.Zero) };
        }
        var manifest = new LsmManifest
        {
            Sequence = ++_manifestSequence, NextFileNumber = _nextFileNumber, TransactionNumber = transactionNumber,
            CommitUlong = commitUlong, Ulongs = ulongs, LevelCount = version.LevelCount, WriterEpoch = _epoch,
            WriterSession = _writerSession, FlushedGeneration = flushedGeneration, WalStartChunk = walStartChunk,
            WalStartEpoch = walStartEpoch,
            Tables = version.AllTables().Select(t => (t.Level, t.Table.FileNumber, t.Table.Length)).ToArray(),
            Level0RunLengths = version.Level0Runs.Select(r => r.Length).ToArray(),
            ValueFiles = version.ValueFiles.Select(v => (v.Key, v.Value)).OrderBy(v => v.Key).ToArray(),
            PendingDeletions = pending
        };
        CreateObject(ManifestKey(manifest.Sequence), manifest.Encode(), true);
        _lastManifest = manifest;
        // Readers probe manifest names in order, so replaced manifests stay for a while (ManifestRetention).
        if (previous.Sequence > 0)
            lock (_obsoleteObjects)
                _obsoleteObjects.Add(new(LsmObjectKind.Manifest, (ulong)previous.Sequence, 0, replacedAt));
        Logger?.KeyValueIndexCreated((uint)manifest.Sequence, version.Components.Sum(c => c.EntryCount), 0,
            TimeSpan.Zero, 0);
    }

    /// Creates an immutable object as the writer. For a manifest, a different object under the name means another
    /// writer took over; a lost response is reconciled by reading the object back.
    void CreateObject(string key, byte[] content, bool fence = false, bool pace = true)
    {
        // The pacer belongs to background work; a spilling transaction is foreground work and is not paced.
        if (pace) _writePacer.Pace(content.Length, CompactorWriteBytesPerSecondLimit);
        var backoff = TimeSpan.FromMilliseconds(20);
        for (var attempt = 0;; attempt++)
        {
            // Every attempt is a write, so every attempt needs the lease; a missing object after an ambiguous create
            // is retried with a growing pause.
            CheckLeaseDeadline();
            if (attempt > 0)
            {
                Thread.Sleep(backoff);
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(2).Ticks));
            }

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

    /// Sleeps so that the reported bytes do not exceed limit bytes per second; 0 means no limit. Used by one thread at
    /// a time: whichever runs the background units.
    sealed class BytePacer
    {
        long _bytes;
        long _start = Stopwatch.GetTimestamp();

        public void Pace(long bytes, ulong limit)
        {
            if (limit == 0) return;
            // Time when the paced work was idle or caught up is not credit for reading faster later.
            if (Stopwatch.GetElapsedTime(_start) > TimeSpan.FromSeconds((double)_bytes / limit))
            {
                _start = Stopwatch.GetTimestamp();
                _bytes = 0;
            }

            _bytes += bytes;
            var due = TimeSpan.FromSeconds((double)_bytes / limit);
            var elapsed = Stopwatch.GetElapsedTime(_start);
            if (due > elapsed) Thread.Sleep(due - elapsed);
        }
    }

    /// Oldest state sequence any live snapshot of this instance uses. Enumerating the dictionary takes no locks.
    long OldestLiveSequence()
    {
        var oldest = long.MaxValue;
        foreach (var (transaction, _) in _transactions) oldest = Math.Min(oldest, transaction.StateSequence);
        return oldest;
    }

    /// Releases the native memory of memtables no snapshot can reach any more; no storage access.
    void ReleaseRetiredArenas()
    {
        lock (_obsoleteObjects)
        {
            if (_retiredArenas.Count == 0) return;
            var oldestLive = OldestLiveSequence();
            for (var i = _retiredArenas.Count - 1; i >= 0; i--)
            {
                if (_retiredArenas[i].ObsoleteAt > oldestLive) continue;
                _retiredArenas[i].Arena.Dispose();
                _retiredArenas.RemoveAt(i);
            }
        }
    }

    /// An object waiting for deletion: ObsoleteAt is the first state sequence that no longer references it, so older
    /// snapshots of this instance keep it; Since is when it became obsolete.
    readonly record struct ObsoleteObject(LsmObjectKind Kind, ulong Number, long ObsoleteAt, DateTimeOffset Since)
    {
        public bool IsFile => Kind is LsmObjectKind.Table or LsmObjectKind.ValueFile;
    }

    string ObjectKey(LsmObjectKind kind, ulong number) => kind switch
    {
        LsmObjectKind.Table => TableKey(_options.Prefix, number),
        LsmObjectKind.ValueFile => ValueKey(number),
        LsmObjectKind.Manifest => ManifestKey((long)number),
        _ => WalKey(_options.Prefix, (long)number)
    };

    static readonly TimeSpan ManifestRetention = TimeSpan.FromMinutes(1);

    /// Deletes obsolete objects no snapshot of this instance or current version needs and whose grace period for
    /// readers in other processes has passed (ImplementationPlan.md, section 16). Only the writer deletes; a failed
    /// delete is retried on a later pass.
    void DeleteObsoleteObjects()
    {
        ReleaseRetiredArenas();
        if (!_isWriter) return;
        var now = _options.TimeProvider.GetUtcNow();
        var grace = _options.DeletionGracePeriod;
        // Snapshots never read manifests, and a reader whose next manifest is gone lists the newest one, so replaced
        // manifests stay only briefly; few of them keep listings at takeover cheap. They stay that long even with a
        // shorter grace period: deleting one frees its name, and a delayed create landing there would take it again
        // with an epoch already in use (BTDB.LSM/Proofs, Fence.lean).
        HashSet<ulong>? referenced = null;
        var deletable = new List<ObsoleteObject>();
        lock (_obsoleteObjects)
        {
            var oldestLive = OldestLiveSequence();
            for (var i = _obsoleteObjects.Count - 1; i >= 0; i--)
            {
                var o = _obsoleteObjects[i];
                if (o.ObsoleteAt > oldestLive ||
                    now - o.Since < (o.Kind == LsmObjectKind.Manifest ? ManifestRetention : grace)) continue;
                _obsoleteObjects.RemoveAt(i);
                // Never delete what the current version references, whatever led to the entry.
                var version = _state.Version;
                referenced ??= version.AllTables().Select(t => t.Table.FileNumber).Concat(version.ValueFiles.Keys)
                    .ToHashSet();
                if (o.IsFile && referenced.Contains(o.Number))
                {
                    Logger?.LogWarning("BTDB.LSM kept " + ObjectKey(o.Kind, o.Number) +
                                       ": the current version references it.");
                    continue;
                }

                deletable.Add(o);
            }
        }

        foreach (var o in deletable)
        {
            var key = ObjectKey(o.Kind, o.Number);
            try
            {
                if (o.IsFile) _cache.Evict(new(_cacheScope, o.Number));
                _storage.DeleteAsync(key, CancellationToken.None).AsTask().GetAwaiter().GetResult();
                if (o.IsFile) Logger?.FileMarkedForDelete((uint)o.Number);
            }
            catch (Exception ex)
            {
                Logger?.LogWarning("BTDB.LSM could not delete " + key + ", will retry: " + ex.Message);
                lock (_obsoleteObjects) _obsoleteObjects.Add(o);
            }
        }
    }
}
