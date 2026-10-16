using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Storage;

namespace BTDB.LSM;

public sealed partial class LsmKeyValueDB
{
    sealed record PendingRecord(long Generation, long TransactionNumber, byte[] Payload);

    sealed record EncodedChunk(long Number, byte[] Content, bool LastRecordComplete);

    readonly Lock _walLock = new();
    readonly List<PendingRecord> _walPending = [];
    readonly SortedDictionary<long, List<TaskCompletionSource>> _durabilityWaiters = new();
    readonly Dictionary<long, long> _generationFirstChunk = new();
    SemaphoreSlim _walSignal = new(0);
    List<EncodedChunk>? _walInFlight;
    int _walInFlightRecords;
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
                await _walSignal.WaitAsync(_options.WalFlushInterval, stop);
                // Group commit: let concurrent commits pile up for one flush interval.
                await Task.Delay(_options.WalFlushInterval, _options.TimeProvider, stop);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            while (!stop.IsCancellationRequested)
            {
                try
                {
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

    /// Uploads one batch of pending records; returns false when nothing was pending. A failed batch keeps its chunk
    /// numbers and contents, so a retry recreates exactly the missing chunks and history stays contiguous.
    async ValueTask<bool> UploadWalAsync()
    {
        if (_walInFlight == null)
        {
            PendingRecord[] records;
            lock (_walLock) records = [.. _walPending];
            if (records.Length == 0) return false;
            _walInFlight = EncodeChunks(records);
            _walInFlightRecords = records.Length;
            _walInFlightTransaction = records[^1].TransactionNumber;
        }

        var chunks = _walInFlight;
        for (var start = 0; start < chunks.Count; start += _options.WalUploadParallelism)
        {
            var batch = chunks.Skip(start).Take(_options.WalUploadParallelism).ToArray();
            await Task.WhenAll(batch.Select(c => CreateChunkAsync(c.Number, c.Content).AsTask()));
        }

        long durable;
        lock (_walLock)
        {
            _walPending.RemoveRange(0, _walInFlightRecords);
            _walPendingBytes = _walPending.Sum(r => (long)r.Payload.Length);
            _walInFlight = null;
            durable = _durableTransaction = _walInFlightTransaction;
        }

        CompleteDurabilityWaiters(durable, null);
        NotifyProgress();
        return true;
    }

    /// Splits records into chunks of about WalChunkTargetSize; a chunk holds records of one generation only.
    List<EncodedChunk> EncodeChunks(IReadOnlyList<PendingRecord> records)
    {
        var chunks = new List<EncodedChunk>();
        var parts = new List<WalPart>();
        var size = 0;
        var generation = records[0].Generation;

        void Close(bool lastComplete)
        {
            if (parts.Count == 0) return;
            var number = _walNextChunk++;
            _generationFirstChunk.TryAdd(generation, number);
            chunks.Add(new(number, WalFormat.Encode(_epoch, generation, false, parts), lastComplete));
            parts = [];
            size = 0;
        }

        foreach (var record in records)
        {
            if (record.Generation != generation)
            {
                Close(true);
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
                if (size >= _options.WalChunkTargetSize) Close(last);
            } while (offset < payload.Length);
        }

        Close(true);
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

    /// Completes when the commit with transactionNumber is durable in the WAL.
    public ValueTask WaitForDurabilityAsync(long transactionNumber, CancellationToken cancellation = default)
    {
        TaskCompletionSource waiter;
        lock (_walLock)
        {
            if (transactionNumber <= _durableTransaction) return ValueTask.CompletedTask;
            if (!_isWriter)
                throw new BTDBException("This instance is not the writer; the commit was not made durable.");
            waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_durabilityWaiters.TryGetValue(transactionNumber, out var list))
                _durabilityWaiters.Add(transactionNumber, list = []);
            list.Add(waiter);
        }

        _walSignal.Release();
        return new(waiter.Task.WaitAsync(cancellation));
    }

    /// Makes every commit so far durable in the WAL.
    public ValueTask FlushAsync(CancellationToken cancellation = default) =>
        WaitForDurabilityAsync(_state.TransactionNumber, cancellation);

    bool WalBacklogStalls() => Interlocked.Read(ref _walPendingBytes) > _options.MaxUnflushedWalBytes;
}

/// The writer session was fenced by another writer or lost its lease; it must not write again.
public sealed class LsmWriterFencedException(string message) : BTDBException(message);
