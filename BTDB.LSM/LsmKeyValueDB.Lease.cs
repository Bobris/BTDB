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
                    // Acquiring again with the same lease id also extends a lease this instance still holds.
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

                        await Task.Delay(_options.LeaseRetryInterval, _options.TimeProvider, cancellation);
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

    async ValueTask BecomeWriterAsync(CancellationToken cancellation)
    {
        var loaded = await LoadAsync(_options, _cache, OpenTables(), cancellation);
        InstallLoaded(loaded);
        var orphans = await FindOrphansAsync(loaded, cancellation);
        _epoch = loaded.MaxEpoch + 1;
        _manifestSequence = loaded.Manifest.Sequence;
        // Fence the manifest: the next manifest name now carries this writer's epoch.
        WriteManifest(loaded.Version, loaded.Manifest.CommitUlong, loaded.Manifest.Ulongs,
            loaded.Manifest.TransactionNumber, loaded.Manifest.FlushedGeneration, loaded.Manifest.WalStartChunk);
        // Fence the WAL: an epoch marker at the first free name; later stale chunks have a lower epoch.
        await CreateChunkAsync(loaded.FirstFreeChunk,
            WalFormat.Encode(_epoch, loaded.MemtableGeneration, true, []));
        // Only now is the loaded manifest final: a failed attempt may have seen objects a predecessor then published.
        var now = _options.TimeProvider.GetUtcNow();
        lock (_obsoleteObjects)
            foreach (var (key, fileNumber) in orphans)
                _obsoleteObjects.Add((key, fileNumber, 0, now, loaded.Manifest.Sequence + 1));
        lock (_walLock)
        {
            _walPending.Clear();
            _walPendingBytes = 0;
            _walInFlight = null;
            _walNextChunk = Math.Max(loaded.NextChunk, loaded.FirstFreeChunk + 1);
            _durableTransaction = loaded.Metadata.TransactionNumber;
            _generationFirstChunk.Clear();
            foreach (var (generation, chunk) in loaded.GenerationFirstChunk) _generationFirstChunk[generation] = chunk;
        }

        _isWriter = true;
        StartWriterServices();
        if (loaded.Immutables.Length > 0) RequestWork();
    }

    /// A predecessor may have created tables and value files it never published. New file numbers start after every
    /// existing object; the unreferenced objects are returned, to be deleted like obsolete ones once fenced.
    async ValueTask<List<(string Key, ulong FileNumber)>> FindOrphansAsync(LoadedDatabase loaded,
        CancellationToken cancellation)
    {
        var orphans = new List<(string, ulong)>();
        var referenced = loaded.Version.AllTables().Select(t => t.Table.FileNumber)
            .Concat(loaded.Version.ValueFiles.Keys).ToHashSet();
        var highest = 0UL;
        foreach (var prefix in new[] { "tables/", "values/" })
        {
            await foreach (var info in _storage.ListAsync(_options.Prefix + prefix, cancellation))
            {
                var name = info.Key[(info.Key.LastIndexOf('/') + 1)..];
                var dot = name.IndexOf('.');
                if (dot < 0 || !ulong.TryParse(name.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture,
                        out var fileNumber)) continue;
                highest = Math.Max(highest, fileNumber);
                if (!referenced.Contains(fileNumber)) orphans.Add((info.Key, fileNumber));
            }
        }

        _nextFileNumber = Math.Max(_nextFileNumber, highest + 1);
        return orphans;
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
                lock (_walLock)
                {
                    _walPending.Clear();
                    _walPendingBytes = 0;
                    _walInFlight = null;
                }

                if (_disposed) return;
                try
                {
                    InstallLoaded(await LoadAsync(_options, _cache, OpenTables(), CancellationToken.None));
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
