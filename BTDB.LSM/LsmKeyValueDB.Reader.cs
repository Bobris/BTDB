using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;

namespace BTDB.LSM;

public sealed partial class LsmKeyValueDB
{
    readonly SemaphoreSlim _refreshLock = new(1, 1);
    WalReplay _replay = null!;

    // Set when this instance stopped being the writer: its replay then lags its state.
    bool _replayStale;
    DateTimeOffset _lastManifestList = DateTimeOffset.MinValue;

    /// Catches up to the newest durable state (ImplementationPlan.md, section 15): reads manifests newer than the
    /// current one, then replays WAL chunks after the last one read into copies of the memtables. Returns the
    /// CommitUlong reached. The writer is always current.
    public async ValueTask<ulong> RefreshAsync(CancellationToken cancellation = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_isWriter) return _state.CommitUlong;
        await _refreshLock.WaitAsync(cancellation);
        try
        {
            if (_isWriter) return _state.CommitUlong;
            await CatchUpAsync(false, cancellation);
            DeleteObsoleteObjects();
            return _state.CommitUlong;
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    /// Continues the reader's replay and publishes the result; requires the refresh lock. A refresh probes the next
    /// manifest and chunk names. A takeover (authoritative) lists them instead, so it surely reaches the newest
    /// manifest and the end of the WAL. Returns one past the highest WAL chunk name seen. After this instance stopped
    /// being the writer, its replay does not match its state, so the state is loaded from storage once.
    async ValueTask<long> CatchUpAsync(bool authoritative, CancellationToken cancellation)
    {
        if (_replayStale)
        {
            var loaded = await LoadAsync(_options, _cache, OpenTables(), cancellation);
            InstallLoaded(loaded);
            _replayStale = false;
            return loaded.NextChunk;
        }

        var replay = _replay;
        replay.BeginRound();
        var previous = _state;
        var version = previous.Version;
        var manifestSequence = previous.ManifestSequence;
        var newer = authoritative
            ? await ReadNewerManifestAsync(manifestSequence, cancellation)
            : await FindNewerManifestAsync(manifestSequence, cancellation);
        if (newer is { } manifest)
        {
            try
            {
                version = BuildVersion(_options, _cache, manifest, OpenTables());
            }
            catch (BTDBException)
            {
                // The probed manifest is already outdated and its tables deleted; take the newest one.
                (manifest, _) = await ReadNewestManifestAsync(_options, cancellation);
                version = BuildVersion(_options, _cache, manifest, OpenTables());
            }

            replay.AdvanceFlushed(manifest);
            manifestSequence = manifest.Sequence;
            _lastManifest = manifest;
        }

        var nextChunk = authoritative ? await ReplayListedWalAsync(_options, replay, cancellation) : 0;
        if (!authoritative)
        {
            // Probe one chunk first, so an idle refresh costs one read; while chunks keep coming, read ahead up to
            // WalReadParallelism names at once.
            for (var window = 1;; window = Math.Min(window * 2, WalReadParallelism))
            {
                var numbers = Enumerable.Range(0, window).Select(i => replay.NextChunk + i).ToArray();
                var complete = true;
                await foreach (var (number, content) in ReadChunksAsync(_options, numbers, cancellation))
                {
                    if (content == null)
                    {
                        complete = false;
                        break;
                    }

                    replay.Apply(number, WalFormat.Decode(content, WalKey(_options.Prefix, number)));
                }

                if (!complete) break;
            }

            nextChunk = replay.NextChunk;
        }

        var (memtable, generation) = replay.Current();
        var metadata = replay.Metadata;
        LsmState published;
        lock (_stateLock)
        {
            published = new(memtable, generation, [.. replay.Sealed], version, metadata.CommitUlong,
                metadata.Ulongs, metadata.TransactionNumber, _state.CommitSequence + 1, _state.Sequence + 1,
                manifestSequence);
            _state = published;
        }

        RetireReplacedArenas(previous, published);
        return nextChunk;
    }

    /// Refreshes until CommitUlong reaches commitUlong, pausing RefreshRetryInterval between refreshes and doubling
    /// the pause up to one second.
    public async ValueTask WaitForCommitUlongAsync(ulong commitUlong, CancellationToken cancellation = default)
    {
        var delay = _options.RefreshRetryInterval;
        while (true)
        {
            if (_state.CommitUlong >= commitUlong) return;
            if (!_isWriter && await RefreshAsync(cancellation) >= commitUlong) return;
            await Task.Delay(delay, _options.TimeProvider, cancellation);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromSeconds(1).Ticks));
        }
    }

    /// The newest manifest after sequence, found by listing every manifest name.
    async ValueTask<LsmManifest?> ReadNewerManifestAsync(long sequence, CancellationToken cancellation)
    {
        string? newestKey = null;
        await foreach (var info in _storage.ListAsync(_options.Prefix + "manifest/", cancellation))
            newestKey = info.Key;
        if (newestKey == null || ParseNumber(newestKey) <= (ulong)sequence) return null;
        var content = await _storage.TryReadAsync(newestKey, cancellation) ??
                      throw new BTDBException($"Manifest {newestKey} disappeared while loading.");
        return LsmManifest.Decode(content, newestKey);
    }

    /// The number in an object name: the digits after the last slash, up to an extension.
    static ulong ParseNumber(string key)
    {
        var name = key.AsSpan(key.LastIndexOf('/') + 1);
        var dot = name.IndexOf('.');
        return ulong.TryParse(dot < 0 ? name : name[..dot], System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture, out var number)
            ? number
            : ulong.MaxValue;
    }

    /// The newest manifest after sequence, found by probing the following names; when a probe misses, manifests are
    /// listed now and then, because an old reader may have missed names already deleted.
    async ValueTask<LsmManifest?> FindNewerManifestAsync(long sequence, CancellationToken cancellation)
    {
        LsmManifest? newest = null;
        while (true)
        {
            var key = ManifestKey(sequence + 1);
            var content = await _storage.TryReadAsync(key, cancellation);
            if (content == null) break;
            newest = LsmManifest.Decode(content, key);
            sequence = newest.Sequence;
        }

        var now = _options.TimeProvider.GetUtcNow();
        if (newest != null || now - _lastManifestList < _options.ManifestListInterval) return newest;
        _lastManifestList = now;
        var (listed, listedKey) = await ReadNewestManifestAsync(_options, cancellation);
        return listedKey != null && listed.Sequence > sequence ? listed : null;
    }

    void RetireReplacedArenas(LsmState previous, LsmState current)
    {
        var kept = current.Immutables.Select(m => m.Memtable.Arena).Append(current.Memtable.Arena).ToHashSet();
        lock (_obsoleteObjects)
        {
            foreach (var memtable in previous.Immutables.Select(m => m.Memtable).Append(previous.Memtable))
            {
                if (memtable.Arena is { } arena && !kept.Contains(arena))
                    _retiredArenas.Add((arena, current.Sequence));
            }
        }
    }
}
