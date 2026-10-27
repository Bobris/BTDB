using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Storage;
using BTDB.Replication.EventLog;

namespace BTDB.LSM;

public sealed partial class LsmKeyValueDB
{
    sealed record PendingRecord(long Generation, long TransactionNumber, byte[] Payload);

    sealed record EncodedChunk(long Number, byte[] Content);

    readonly Lock _walLock = new();
    readonly List<PendingRecord> _walPending = [];
    readonly SortedDictionary<long, List<TaskCompletionSource>> _durabilityWaiters = new();
    readonly Dictionary<long, long> _generationFirstChunk = new();
    readonly CommitLingerEstimator? _walLinger;
    readonly long _walClockOrigin = Stopwatch.GetTimestamp();
    SemaphoreSlim _walSignal = new(0);
    List<EncodedChunk>? _walInFlight;
    int _walInFlightRecords;
    long _walInFlightBytes;
    long _walInFlightTransaction;
    long _walPendingBytes;
    long _walNextChunk;
    long _durableTransaction;
    Task? _walUploader;

    /// Queues the commit record of the writer's commit; called in commit order while the writer slot is held.
    void AppendWal(long generation, long transactionNumber, byte[] record)
    {
        lock (_walLock)
        {
            _walPending.Add(new(generation, transactionNumber, record));
            _walPendingBytes += record.Length;
            _walLinger?.Arrival(WalClock);
        }

        if (_walUploader == null) UploadWalAsync().AsTask().GetAwaiter().GetResult();
        else _walSignal.Release();
    }

    async Task WalLoop(CancellationToken stop)
    {
        var backoff = TimeSpan.FromMilliseconds(20);
        while (!stop.IsCancellationRequested)
        {
            try
            {
                // Upload as soon as a commit arrives; commits arriving during an upload form the next group.
                await _walSignal.WaitAsync(_options.WalFlushInterval, stop);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await LingerWalAsync(stop);
                    if (!await UploadWalAsync()) break;
                    backoff = TimeSpan.FromMilliseconds(20);
                }
                catch (LsmWriterFencedException ex)
                {
                    _ = LoseWriterAsync(ex);
                    return;
                }
                catch (Exception ex)
                {
                    _workerError = ex;
                    Logger?.LogWarning("BTDB.LSM WAL upload failed, retrying: " + ex.Message);
                    try
                    {
                        await Task.Delay(backoff, _options.TimeProvider, stop);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }

                    backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(5).Ticks));
                }
            }
        }
    }

    /// Uploads one batch of pending records; returns false when nothing was pending. A batch holds about one round of
    /// parallel chunk uploads (at least one record), so durability advances round by round. A failed batch keeps its
    /// chunk numbers and contents, so a retry recreates exactly the missing chunks and history stays contiguous.
    async ValueTask<bool> UploadWalAsync()
    {
        if (_walInFlight == null)
        {
            PendingRecord[] records;
            lock (_walLock)
            {
                var limit = WalBatchLimit;
                var count = 0;
                long bytes = 0;
                while (count < _walPending.Count && (count == 0 || bytes + _walPending[count].Payload.Length <= limit))
                    bytes += _walPending[count++].Payload.Length;
                if (count == 0) return false;
                records = _walPending.GetRange(0, count).ToArray();
                _walInFlightBytes = bytes;
            }

            var planned = PlanChunks(records);
            var numbers = new long[planned.Count];
            lock (_walLock)
            {
                // Chunk numbers and the first chunk of each generation change only under the lock, because a flush
                // reads them to decide where recovery starts.
                for (var i = 0; i < planned.Count; i++)
                {
                    numbers[i] = _walNextChunk++;
                    _generationFirstChunk.TryAdd(planned[i].Generation, numbers[i]);
                }
            }

            _walInFlight = planned.Select((chunk, i) => new EncodedChunk(numbers[i],
                WalFormat.Encode(_epoch, chunk.Generation, false, chunk.Parts))).ToList();
            _walInFlightRecords = records.Length;
            _walInFlightTransaction = records[^1].TransactionNumber;
        }

        var chunks = _walInFlight;
        var started = WalClock;
        for (var start = 0; start < chunks.Count; start += _options.WalUploadParallelism)
        {
            var batch = chunks.Skip(start).Take(_options.WalUploadParallelism).ToArray();
            await Task.WhenAll(batch.Select(c => CreateChunkAsync(c.Number, c.Content).AsTask()));
        }

        long durable;
        lock (_walLock)
        {
            _walPending.RemoveRange(0, _walInFlightRecords);
            _walPendingBytes -= _walInFlightBytes;
            _walInFlight = null;
            // A spilled transaction's commit may already have made later transactions durable.
            durable = _durableTransaction = Math.Max(_durableTransaction, _walInFlightTransaction);
            // Before the waiters complete, so the commits answering them count from this upload.
            _walLinger?.Committed(started, WalClock, _walPending.Count > 0);
        }

        CompleteDurabilityWaiters(durable, null);
        NotifyProgress();
        return true;
    }

    TimeSpan WalClock => Stopwatch.GetElapsedTime(_walClockOrigin);

    long WalBatchLimit => (long)_options.WalChunkTargetSize * _options.WalUploadParallelism;

    /// The wait before each upload chosen by AdaptiveWalLingerMax; zero when disabled.
    internal TimeSpan WalLinger
    {
        get
        {
            lock (_walLock) return _walLinger?.Current ?? TimeSpan.Zero;
        }
    }

    /// Before an upload, waits for commits that usually follow the previous one closely, see AdaptiveWalLingerMax.
    /// Ends early once pending commits fill a batch or the uploader stops; a retry of a failed batch does not wait.
    async ValueTask LingerWalAsync(CancellationToken stop)
    {
        TimeSpan linger;
        lock (_walLock)
        {
            if (_walLinger == null || _walInFlight != null || _walPending.Count == 0) return;
            linger = _walLinger.Current;
        }

        if (linger == TimeSpan.Zero) return;
        var start = Stopwatch.GetTimestamp();
        TimeSpan Remaining() => stop.IsCancellationRequested || Interlocked.Read(ref _walPendingBytes) >= WalBatchLimit
            ? TimeSpan.Zero
            : linger - Stopwatch.GetElapsedTime(start);

        if (linger >= TimeSpan.FromMilliseconds(2))
        {
            // Every commit releases the signal, so the batch limit is checked as commits arrive.
            try
            {
                for (var remaining = Remaining(); remaining > TimeSpan.Zero; remaining = Remaining())
                    await _walSignal.WaitAsync(remaining, stop);
            }
            catch (OperationCanceledException)
            {
            }

            return;
        }

        // Timers are coarser than short windows.
        while (Remaining() > TimeSpan.Zero) await Task.Yield();
    }

    sealed record PlannedChunk(long Generation, List<WalPart> Parts);

    /// Splits records into chunks of about WalChunkTargetSize; a chunk holds records of one generation only.
    List<PlannedChunk> PlanChunks(IReadOnlyList<PendingRecord> records)
    {
        var chunks = new List<PlannedChunk>();
        var parts = new List<WalPart>();
        var size = 0;
        var generation = records[0].Generation;

        void Close()
        {
            if (parts.Count == 0) return;
            chunks.Add(new(generation, parts));
            parts = [];
            size = 0;
        }

        foreach (var record in records)
        {
            if (record.Generation != generation)
            {
                Close();
                generation = record.Generation;
            }

            var payload = record.Payload;
            var offset = 0;
            do
            {
                var room = Math.Max(1024, _options.WalChunkTargetSize - size);
                var length = Math.Min(room, payload.Length - offset);
                var first = offset == 0;
                var last = offset + length == payload.Length;
                parts.Add(new(first, last, record.TransactionNumber, payload.AsSpan(offset, length).ToArray()));
                size += length + 16;
                offset += length;
                if (size >= _options.WalChunkTargetSize) Close();
            } while (offset < payload.Length);
        }

        Close();
        return chunks;
    }

    /// Creates a chunk; a lost or ambiguous response is reconciled by reading the chunk back, and a different chunk
    /// under the name means another writer took over.
    async ValueTask CreateChunkAsync(long number, byte[] content)
    {
        var key = WalKey(_options.Prefix, number);
        while (true)
        {
            CheckLeaseDeadline();
            var outcome = await _storage.CreateAsync(key, content, CancellationToken.None);
            if (outcome == LsmWriteOutcome.Applied) return;
            var existing = await _storage.TryReadAsync(key, CancellationToken.None);
            if (existing != null)
            {
                if (existing.AsSpan().SequenceEqual(content)) return;
                throw new LsmWriterFencedException($"WAL chunk {key} was written by another writer.");
            }

            if (outcome == LsmWriteOutcome.Rejected)
                throw new LsmWriterFencedException($"WAL chunk {key} was rejected and is not readable.");
        }
    }

    void CompleteDurabilityWaiters(long durable, Exception? error)
    {
        var completed = new List<TaskCompletionSource>();
        lock (_walLock)
        {
            foreach (var (transaction, waiters) in _durabilityWaiters.ToArray())
            {
                if (error == null && transaction > durable) break;
                completed.AddRange(waiters);
                _durabilityWaiters.Remove(transaction);
            }
        }

        foreach (var waiter in completed)
        {
            if (error == null) waiter.TrySetResult();
            else waiter.TrySetException(error);
        }
    }

    /// Completes when the commit with transactionNumber is durable in the WAL. When omitted or null, waits for the
    /// latest transaction committed at the time of this call; later commits and uncommitted changes are not included.
    public ValueTask WaitForDurabilityAsync(long? transactionNumber = null, CancellationToken cancellation = default)
    {
        var target = transactionNumber ?? _state.TransactionNumber;
        TaskCompletionSource waiter;
        lock (_walLock)
        {
            if (target <= _durableTransaction) return ValueTask.CompletedTask;
            if (!_isWriter)
                throw new BTDBException("This instance is not the writer; the commit was not made durable.");
            waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_durabilityWaiters.TryGetValue(target, out var list))
                _durabilityWaiters.Add(target, list = []);
            list.Add(waiter);
        }

        _walSignal.Release();
        return new(waiter.Task.WaitAsync(cancellation));
    }

    /// Makes every commit so far durable in the WAL.
    public ValueTask FlushAsync(CancellationToken cancellation = default) =>
        WaitForDurabilityAsync(cancellation: cancellation);

    bool WalBacklogStalls() => Interlocked.Read(ref _walPendingBytes) > _options.MaxUnflushedWalBytes;
}

/// The writer session was fenced by another writer or lost its lease; it must not write again.
public sealed class LsmWriterFencedException(string message) : BTDBException(message);
