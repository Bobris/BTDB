using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;

namespace BTDB.LSM;

public sealed partial class LsmKeyValueDB
{
    readonly SemaphoreSlim _modeLock = new(1, 1);
    readonly string _leaseId = Guid.NewGuid().ToString();
    CancellationTokenSource? _writerStop;
    Task? _leaseRenewer;
    volatile bool _isWriter;

    // Set from acquiring the lease, through the takeover, until the writer stops; every object this instance creates
    // requires it, so writes racing a takeover or a lease loss are fenced locally.
    volatile bool _holdsLease;
    long _epoch;
    ulong _writerSession;
    DateTimeOffset _leaseDeadline;

    string LockKey => _options.Prefix + "LOCK";

    /// True while this instance is the writer (ImplementationPlan.md, section 14).
    public bool HoldsWriterLease => _isWriter;

    /// Waits until this instance holds the writer lease, then fences any previous writer, catches up to the newest
    /// durable state and starts accepting writes. Returns at once if it already is the writer.
    public async ValueTask AcquireWriterLeaseAsync(CancellationToken cancellation = default)
    {
        await _modeLock.WaitAsync(cancellation);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_isWriter) return;
            // A refresh must not publish reader state while the instance turns into the writer.
            await _refreshLock.WaitAsync(cancellation);
            try
            {
                while (true)
                {
                    // Acquiring again with the same lease id also extends a lease this instance still holds. A lease
                    // released by its holder is taken over soon: retries start fast and slow down to
                    // LeaseRetryInterval.
                    var retry = TimeSpan.FromTicks(Math.Min(TimeSpan.FromMilliseconds(25).Ticks,
                        _options.LeaseRetryInterval.Ticks));
                    while (true)
                    {
                        var requested = _options.TimeProvider.GetUtcNow();
                        if (await _storage.TryAcquireLeaseAsync(LockKey, _leaseId, _options.LeaseDuration,
                                cancellation))
                        {
                            _leaseDeadline = requested + _options.LeaseDuration - _options.LeaseSafetyMargin;
                            _holdsLease = true;
                            break;
                        }

                        await Task.Delay(retry, _options.TimeProvider, cancellation);
                        retry = TimeSpan.FromTicks(Math.Min(retry.Ticks * 2, _options.LeaseRetryInterval.Ticks));
                    }

                    try
                    {
                        await BecomeWriterAsync(cancellation);
                        return;
                    }
                    catch (LsmWriterFencedException ex)
                    {
                        // A predecessor wrote at the same name concurrently, or the lease ran out during the
                        // takeover; reload its legitimate history under a fresh lease.
                        _holdsLease = false;
                        Logger?.LogWarning("BTDB.LSM writer takeover did not complete, retrying: " + ex.Message);
                    }
                }
            }
            finally
            {
                _refreshLock.Release();
            }
        }
        catch
        {
            _holdsLease = false;
            throw;
        }
        finally
        {
            _modeLock.Release();
        }
    }

    /// Takes over as the writer while holding the lease: catches up from this instance's reader state (listing
    /// manifests and WAL chunks, so nothing newer is missed), then fences the manifest and the WAL. Nothing proportional
    /// to the database size happens here; orphans of a predecessor are searched for in the background afterwards.
    async ValueTask BecomeWriterAsync(CancellationToken cancellation)
    {
        var (nextChunk, missing) = await CatchUpAsync(true, cancellation);
        var manifest = _lastManifest;
        var replay = _replay;
        var state = _state;
        _epoch = Math.Max(replay.MaxEpoch, manifest.WriterEpoch) + 1;
        // Another takeover from the same state computes the same epoch; its fencing manifest must differ from ours.
        _writerSession = _options.WriterSession ?? (ulong)Random.Shared.NextInt64(long.MinValue, long.MaxValue);
        _manifestSequence = manifest.Sequence;
        // Every writer session numbers new files from its own range, so they never collide with files a predecessor
        // created but did not publish.
        _nextFileNumber = Math.Max(manifest.NextFileNumber, FirstFileNumber(_epoch));
        // The predecessor's pending deletions keep their time; snapshots this instance took before keep them alive.
        lock (_obsoleteObjects)
        {
            _obsoleteObjects.Clear();
            foreach (var deletion in manifest.PendingDeletions)
                _obsoleteObjects.Add(new(deletion.Kind, deletion.Number, state.Sequence, deletion.Since));
        }

        // Fence the manifest: the next manifest name now carries this writer's epoch.
        WriteManifest(state.Version, manifest.CommitUlong, manifest.Ulongs, manifest.TransactionNumber,
            manifest.FlushedGeneration, manifest.WalStartChunk, manifest.WalStartEpoch);
        // Fence the WAL: an epoch marker at the first free name and at every other name the listing missed below the
        // next chunk, so a predecessor's request still in flight cannot fill such a hole later and replay always
        // reaches this writer's chunks. Later stale chunks have a lower epoch; the markers name the first own chunk,
        // so replay stops this writer's history where a stale chunk took one of its names (BTDB.LSM/Proofs).
        var firstFreeChunk = replay.NextChunk;
        var firstOwnChunk = Math.Max(nextChunk, firstFreeChunk + 1);
        var marker = WalFormat.Encode(_epoch, state.MemtableGeneration, true, firstOwnChunk, []);
        await Task.WhenAll(missing.Prepend(firstFreeChunk).Distinct()
            .Select(number => CreateChunkAsync(number, marker).AsTask()));
        lock (_walLock)
        {
            _walPending.Clear();
            _walPendingBytes = 0;
            _walInFlight = null;
            _walNextChunk = firstOwnChunk;
            _durableTransaction = state.TransactionNumber;
            _generationFirstChunk.Clear();
            foreach (var (generation, chunk) in replay.GenerationFirstChunk) _generationFirstChunk[generation] = chunk;
        }

        // Only now is the manifest final: a failed attempt may have seen objects a predecessor then published.
        _orphanScanDue = true;
        _isWriter = true;
        StartWriterServices();
        RequestWork();
    }

    static ulong FirstFileNumber(long epoch) => (ulong)epoch << 32;

    bool _orphanScanDue;

    /// After a takeover, queues for deletion what no manifest tracks: tables and value files a predecessor created
    /// but never published, and manifests and WAL chunks whose deletion was lost. The manifest lists every published
    /// object still waiting for deletion, so the others are deleted at once. Runs as a unit of background work, on the thread that moves files from the version
    /// to the pending deletions, so a published file is never mistaken for an orphan.
    void ScanForOrphans()
    {
        var listed = new List<(LsmObjectKind Kind, ulong Number)>();
        foreach (var (kind, prefix) in new[]
                 {
                     (LsmObjectKind.Table, "tables/"), (LsmObjectKind.ValueFile, "values/"),
                     (LsmObjectKind.Manifest, "manifest/"), (LsmObjectKind.WalChunk, "wal/")
                 })
        {
            var enumerator = _storage.ListAsync(_options.Prefix + prefix, CancellationToken.None).GetAsyncEnumerator();
            try
            {
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                    if (ParseNumber(enumerator.Current.Key) is var number and not ulong.MaxValue)
                        listed.Add((kind, number));
            }
            finally
            {
                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        var state = _state;
        var referenced = state.Version.AllTables().Select(t => t.Table.FileNumber)
            .Concat(state.Version.ValueFiles.Keys).ToHashSet();
        var ownFiles = FirstFileNumber(_epoch);
        var manifest = _lastManifest;
        var found = 0;
        lock (_obsoleteObjects)
        {
            var pending = _obsoleteObjects.Select(o => (o.Kind, o.Number)).ToHashSet();
            foreach (var (kind, number) in listed)
            {
                if (pending.Contains((kind, number))) continue;
                var orphan = kind switch
                {
                    LsmObjectKind.Table or LsmObjectKind.ValueFile => number < ownFiles && !referenced.Contains(number),
                    LsmObjectKind.Manifest => number < (ulong)manifest.Sequence,
                    _ => number < (ulong)manifest.WalStartChunk
                };
                if (!orphan) continue;
                _obsoleteObjects.Add(new(kind, number, state.Sequence, DateTimeOffset.MinValue));
                found++;
            }
        }

        if (found > 0) Logger?.LogWarning($"BTDB.LSM found {found} objects no manifest tracks; they will be deleted.");
        DeleteObsoleteObjects();
    }

    /// Replaces the in-memory state by state loaded from storage. Existing snapshots keep what they read; objects
    /// they use are never deleted by a reader, and replaced memtable arenas are released once unused.
    void InstallLoaded(LoadedDatabase loaded)
    {
        long sequence;
        LsmState previous;
        lock (_stateLock)
        {
            previous = _state;
            sequence = previous.Sequence + 1;
            _state = new(loaded.Memtable, loaded.MemtableGeneration, loaded.Immutables, loaded.Version,
                loaded.Metadata.CommitUlong, loaded.Metadata.Ulongs, loaded.Metadata.TransactionNumber,
                previous.CommitSequence + 1, sequence, loaded.Manifest.Sequence);
        }

        _replay = loaded.Replay;

        _lastManifest = loaded.Manifest;
        _nextFileNumber = Math.Max(_nextFileNumber, loaded.Manifest.NextFileNumber);
        lock (_obsoleteObjects)
        {
            foreach (var memtable in previous.Immutables.Select(m => m.Memtable).Append(previous.Memtable))
            {
                if (memtable.Arena is { } arena && !ReferencesArena(loaded, arena))
                    _retiredArenas.Add((arena, sequence));
            }
        }
    }

    static bool ReferencesArena(LoadedDatabase loaded, object arena) =>
        loaded.Memtable.Arena == arena || loaded.Immutables.Any(m => m.Memtable.Arena == arena);

    System.Collections.Generic.Dictionary<ulong, Format.TableReader> OpenTables() =>
        _state.Version.AllTables().ToDictionary(t => t.Table.FileNumber, t => t.Table);

    void StartWriterServices()
    {
        _writerStop = new();
        var stop = _writerStop.Token;
        _walSignal = new(0);
        if (_options.BackgroundCompaction)
        {
            _walUploader = Task.Run(() => WalLoop(stop));
            _worker = Task.Run(() => WorkerLoop(stop));
        }

        _leaseRenewer = Task.Run(() => RenewLeaseLoop(stop));
    }

    async Task StopWriterServicesAsync()
    {
        _writerStop?.Cancel();
        _workSignal.Release();
        foreach (var task in new[] { _walUploader, _worker, _leaseRenewer })
        {
            if (task == null) continue;
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _walUploader = null;
        _worker = null;
        _leaseRenewer = null;
    }

    async Task RenewLeaseLoop(CancellationToken stop)
    {
        var interval = _options.LeaseDuration / 3;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, _options.TimeProvider, stop);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            var requested = _options.TimeProvider.GetUtcNow();
            bool renewed;
            try
            {
                renewed = await _storage.RenewLeaseAsync(LockKey, _leaseId, _options.LeaseDuration, stop);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                Logger?.LogWarning("BTDB.LSM lease renewal failed: " + ex.Message);
                renewed = false;
                if (_options.TimeProvider.GetUtcNow() < _leaseDeadline) continue;
            }

            if (renewed)
            {
                _leaseDeadline = requested + _options.LeaseDuration - _options.LeaseSafetyMargin;
                continue;
            }

            _ = LoseWriterAsync(new LsmWriterFencedException("The writer lease was lost."));
            return;
        }
    }

    /// Self-fence: an instance that does not hold the lease, or whose lease may have expired, must not write any more
    /// objects. The renewal loop renews every third of the lease; when it is late and less than a third remains, the
    /// lease is renewed here first, so writing does not depend on the loop being scheduled on time.
    void CheckLeaseDeadline()
    {
        if (!_holdsLease) throw new LsmWriterFencedException("This instance does not hold the writer lease.");
        var now = _options.TimeProvider.GetUtcNow();
        if (now > _leaseDeadline - _options.LeaseDuration / 3)
        {
            bool renewed;
            try
            {
                renewed = _storage.RenewLeaseAsync(LockKey, _leaseId, _options.LeaseDuration, CancellationToken.None)
                    .AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Logger?.LogWarning("BTDB.LSM lease renewal failed: " + ex.Message);
                renewed = false;
            }

            if (renewed) _leaseDeadline = now + _options.LeaseDuration - _options.LeaseSafetyMargin;
            else if (_options.TimeProvider.GetUtcNow() <= _leaseDeadline) return;
            else throw new LsmWriterFencedException("The writer lease could not be renewed.");
        }

        if (_options.TimeProvider.GetUtcNow() > _leaseDeadline)
            throw new LsmWriterFencedException("The writer lease deadline passed.");
    }

    /// Hands the writer role over: waits for a running writing transaction, makes every commit durable, releases the
    /// lease and continues as a reader of the same state, so another instance can take over at once. Flushes and
    /// compactions are left to the next writer; a compaction running now is abandoned.
    public async ValueTask ReleaseWriterLeaseAsync(CancellationToken cancellation = default)
    {
        await _modeLock.WaitAsync(cancellation);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_isWriter) return;
            // Holding the writer slot keeps commits out while the WAL drains.
            var waiting = TakeWriterSlotAsync();
            try
            {
                await waiting.WaitAsync(cancellation);
            }
            catch (OperationCanceledException)
            {
                // The slot may still be granted later; give it back then.
                _ = waiting.ContinueWith(t => t.Result.Dispose(), CancellationToken.None,
                    TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
                throw;
            }

            using var slot = waiting.Result;
            await _refreshLock.WaitAsync(CancellationToken.None);
            try
            {
                await StepDownAsync();
            }
            finally
            {
                _refreshLock.Release();
            }
        }
        finally
        {
            _modeLock.Release();
        }
    }

    /// Stops being the writer without losing acknowledged commits: drains the WAL, then releases the lease before
    /// waiting for background work to stop, so a successor does not wait for a running compaction. Objects that work
    /// still tries to create are fenced locally. Requires the mode lock and no commit in progress. Commits that cannot
    /// be uploaded are lost, as when the lease is lost, and reported by the returned task.
    async Task StepDownAsync()
    {
        _writerStop?.Cancel();
        _walSignal.Release();
        try
        {
            if (_walUploader != null) await _walUploader;
        }
        catch (OperationCanceledException)
        {
        }

        _walUploader = null;
        Exception? uploadError = null;
        try
        {
            while (await UploadWalAsync())
            {
            }
        }
        catch (Exception ex)
        {
            uploadError = ex;
        }

        _holdsLease = false;
        _isWriter = false;
        try
        {
            await _storage.ReleaseLeaseAsync(LockKey, _leaseId, CancellationToken.None);
        }
        catch (Exception ex)
        {
            Logger?.LogWarning("BTDB.LSM could not release the writer lease; it expires instead: " + ex.Message);
        }

        await StopWriterServicesAsync();
        var reason = uploadError == null
            ? new LsmWriterFencedException("The writer lease was released.")
            : new LsmWriterFencedException("The writer lease was released; the WAL upload failed: " +
                                           uploadError.Message);
        CompleteDurabilityWaiters(long.MaxValue, reason);
        TaskCompletionSource[] compactions;
        lock (_fullCompactionRequests)
        {
            compactions = [.. _fullCompactionRequests];
            _fullCompactionRequests.Clear();
        }

        foreach (var compaction in compactions) compaction.TrySetException(reason);
        FailIngestRequests(reason);
        lock (_walLock)
        {
            _walPending.Clear();
            _walPendingBytes = 0;
            _walInFlight = null;
        }

        // The state is the durable state, but the reader replay lags it; the next refresh or takeover reloads.
        _replayStale = true;
        if (uploadError != null) throw new BTDBException("Commits not yet durable were lost: " + uploadError.Message, uploadError);
    }

    /// Stops being the writer: pending commits that were not durable are lost and their durability waiters fail, the
    /// in-memory state is reloaded from storage and the instance continues as a reader.
    async Task LoseWriterAsync(Exception reason)
    {
        await _modeLock.WaitAsync();
        try
        {
            if (!_isWriter) return;
            _holdsLease = false;
            _isWriter = false;
            Logger?.LogWarning("BTDB.LSM stopped being the writer: " + reason.Message);
            // A refresh must not run until the reader state is reloaded: its replay still holds the writer's arenas.
            await _refreshLock.WaitAsync();
            try
            {
                await StopWriterServicesAsync();
                CompleteDurabilityWaiters(long.MaxValue, reason);
                TaskCompletionSource[] compactions;
                lock (_fullCompactionRequests)
                {
                    compactions = [.. _fullCompactionRequests];
                    _fullCompactionRequests.Clear();
                }

                foreach (var compaction in compactions)
                    compaction.TrySetException(
                        new LsmWriterFencedException("The writer lease was lost before the compaction ran."));
                FailIngestRequests(new LsmWriterFencedException("The writer lease was lost before the commit."));
                lock (_walLock)
                {
                    _walPending.Clear();
                    _walPendingBytes = 0;
                    _walInFlight = null;
                }

                _replayStale = true;
                if (_disposed) return;
                try
                {
                    InstallLoaded(await LoadAsync(_options, _cache, OpenTables(), CancellationToken.None));
                    _replayStale = false;
                }
                catch (Exception ex)
                {
                    Logger?.LogWarning("BTDB.LSM could not reload after losing the writer lease: " + ex.Message);
                }
            }
            finally
            {
                _refreshLock.Release();
            }
        }
        finally
        {
            _modeLock.Release();
        }
    }
}
