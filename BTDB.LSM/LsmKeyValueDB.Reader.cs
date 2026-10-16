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
    readonly string _readerId;
    WalReplay _replay = null!;
    DateTimeOffset _lastManifestList = DateTimeOffset.MinValue;
    Task? _heartbeat;
    CancellationTokenSource? _heartbeatStop;

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
            var replay = _replay;
            replay.BeginRound();
            var previous = _state;
            var version = previous.Version;
            var manifestSequence = previous.ManifestSequence;
            if (await FindNewerManifestAsync(manifestSequence, cancellation) is { } manifest)
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
            }

            while (true)
            {
                var key = WalKey(_options.Prefix, replay.NextChunk);
                var content = await _storage.TryReadAsync(key, cancellation);
                if (content == null) break;
                replay.Apply(replay.NextChunk, WalFormat.Decode(content, key));
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
            DeleteObsoleteObjects();
            if (_options.RegisterReader) await WriteRegistryEntryAsync(cancellation);
            return metadata.CommitUlong;
        }
        finally
        {
            _refreshLock.Release();
        }
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

    /// Registry entry: the oldest manifest any live snapshot of this reader uses, and when the entry expires.
    async ValueTask WriteRegistryEntryAsync(CancellationToken cancellation)
    {
        var oldest = _state.ManifestSequence;
        foreach (var transaction in _transactions.Keys) oldest = Math.Min(oldest, transaction.ManifestSequence);
        await WriteRegistryEntryAsync(_options, _readerId, oldest, cancellation);
    }

    /// A reader registers with oldest manifest 0 before it loads any manifest, so a writer never misses a reader
    /// that is about to use objects the writer is deleting.
    static async ValueTask WriteRegistryEntryAsync(LsmKeyValueDBOptions options, string readerId, long oldest,
        CancellationToken cancellation)
    {
        var encoder = new LsmEncoder();
        encoder.WriteVUInt64((ulong)oldest);
        encoder.WriteInt64((options.TimeProvider.GetUtcNow() + options.ReaderHeartbeatInterval * 3).UtcTicks);
        await options.Storage.PutAsync(RegistryKey(options, readerId), encoder.WrittenSpan.ToArray(), cancellation);
    }

    static string RegistryKey(LsmKeyValueDBOptions options, string readerId) =>
        $"{options.Prefix}readers/{readerId}";

    string RegistryKey(string readerId) => RegistryKey(_options, readerId);

    void StartReaderHeartbeat()
    {
        if (!_options.RegisterReader) return;
        _heartbeatStop = new();
        var stop = _heartbeatStop.Token;
        _heartbeat = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    if (!_isWriter) await WriteRegistryEntryAsync(stop);
                    await Task.Delay(_options.ReaderHeartbeatInterval, _options.TimeProvider, stop);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    Logger?.LogWarning("BTDB.LSM reader registry heartbeat failed: " + ex.Message);
                }
            }
        });
    }

    void StopReaderHeartbeat()
    {
        _heartbeatStop?.Cancel();
        try
        {
            _heartbeat?.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
        }

        if (_options.RegisterReader)
        {
            try
            {
                _storage.DeleteAsync(RegistryKey(_readerId), CancellationToken.None).AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Logger?.LogWarning("BTDB.LSM could not remove the reader registry entry: " + ex.Message);
            }
        }
    }

    (long OldestManifest, DateTimeOffset ReadAt)? _registryView;

    /// Oldest manifest still used by a registered reader in another process. The registry is read again unless the
    /// last read happened after notBefore (when the objects in question became obsolete) and within one heartbeat.
    long OldestRegisteredManifest(DateTimeOffset notBefore)
    {
        var now = _options.TimeProvider.GetUtcNow();
        if (_registryView is { } view && view.ReadAt >= notBefore &&
            now - view.ReadAt < _options.ReaderHeartbeatInterval) return view.OldestManifest;
        var oldest = long.MaxValue;
        try
        {
            var keys = new List<string>();
            var enumerator = _storage.ListAsync(_options.Prefix + "readers/", CancellationToken.None).GetAsyncEnumerator();
            try
            {
                while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()) keys.Add(enumerator.Current.Key);
            }
            finally
            {
                enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }

            foreach (var key in keys)
            {
                if (key == RegistryKey(_readerId)) continue;
                var content = _storage.TryReadAsync(key, CancellationToken.None).AsTask().GetAwaiter().GetResult();
                if (content == null) continue;
                var decoder = new LsmDecoder(content);
                var manifest = (long)decoder.ReadVUInt64();
                var expires = new DateTimeOffset(decoder.ReadInt64(), TimeSpan.Zero);
                if (expires > now) oldest = Math.Min(oldest, manifest);
            }
        }
        catch (Exception ex)
        {
            Logger?.LogWarning("BTDB.LSM could not read the reader registry: " + ex.Message);
            return _registryView?.OldestManifest ?? 0;
        }

        _registryView = (oldest, now);
        return oldest;
    }
}
