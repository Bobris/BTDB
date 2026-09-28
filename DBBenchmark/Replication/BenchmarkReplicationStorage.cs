using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication;
using BTDB.StreamLayer;

namespace DBBenchmark.Replication;

/// Monotonic benchmark clock. Stopwatch excludes OS suspend on some platforms, which is irrelevant here.
sealed class StopwatchScheduler : IReplicationScheduler
{
    readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    public TimeSpan Elapsed => _stopwatch.Elapsed;

    public IDisposable Schedule(TimeSpan delay, Action callback, string description)
    {
        var timer = new Timer(_ => callback(), null, delay, Timeout.InfiniteTimeSpan);
        return timer;
    }

    public static LeaseAuthority Authority()
    {
        var authority = new LeaseAuthority(new StopwatchScheduler(), 0, TimeSpan.Zero);
        if (!authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromDays(1)))
            throw new InvalidOperationException("Benchmark authority was not accepted.");
        return authority;
    }
}

/// In-memory replication storage for measurements: canonical TRL CAS plus immutable PVL/KVI objects, with an
/// optional fixed latency per request as a crude Blob model. It counts requests and transferred bytes. Checksums are
/// computed once per object, unlike the test fakes, so they do not distort measurements.
sealed class BenchmarkReplicationStorage(TimeSpan latency = default) : IReplicationStorage, IDisposable
{
    // Fixed 1 MiB segments: the stored copy allocates exactly the uploaded bytes, without regrowth garbage.
    sealed class Trl(uint fileId)
    {
        const int SegmentSize = 1024 * 1024;
        public readonly uint FileId = fileId;
        readonly List<byte[]> _segments = new();
        public TrlObjectState? State;

        public void Write(TrlWrite write)
        {
            for (var offset = write.ExpectedLength; offset < write.Length;)
            {
                var segment = (int)(offset / SegmentSize);
                while (_segments.Count <= segment) _segments.Add(GC.AllocateUninitializedArray<byte>(SegmentSize));
                var start = (int)(offset % SegmentSize);
                var count = (int)Math.Min((uint)(SegmentSize - start), write.Length - offset);
                write.ReadAppend(offset - write.ExpectedLength, _segments[segment].AsSpan(start, count));
                offset += (uint)count;
            }
        }

        public void Read(uint offset, Span<byte> destination)
        {
            while (!destination.IsEmpty)
            {
                var start = (int)(offset % SegmentSize);
                var count = Math.Min(SegmentSize - start, destination.Length);
                _segments[(int)(offset / SegmentSize)].AsSpan(start, count).CopyTo(destination);
                destination = destination[count..];
                offset += (uint)count;
            }
        }
    }

    sealed record Immutable(KVFileType Type, IFileCollectionFile File, string Sha, string Version);

    readonly object _lock = new();
    readonly Dictionary<string, Trl> _trls = new(StringComparer.Ordinal);
    readonly Dictionary<uint, Immutable> _immutables = new();
    readonly InMemoryReplicationFileStorage _files = new();
    int _version;

    public long Requests;
    public long TrlWrites;
    public long UploadedBytes;
    public long DownloadedBytes;

    public void ResetCounters()
    {
        Interlocked.Exchange(ref Requests, 0);
        Interlocked.Exchange(ref TrlWrites, 0);
        Interlocked.Exchange(ref UploadedBytes, 0);
        Interlocked.Exchange(ref DownloadedBytes, 0);
    }

    async ValueTask RequestAsync(CancellationToken cancellation)
    {
        Interlocked.Increment(ref Requests);
        cancellation.ThrowIfCancellationRequested();
        if (latency > TimeSpan.Zero) await Task.Delay(latency, cancellation).ConfigureAwait(false);
    }

    string NextVersion() => (++_version).ToString();

    public async ValueTask<TrlObjectState?> ReadAsync(string key, CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        lock (_lock) return _trls.GetValueOrDefault(key)?.State;
    }

    public async ValueTask ReadRangeAsync(string key, string token, uint offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        lock (_lock)
        {
            var trl = _trls[key];
            if (trl.State?.Token != token) throw new IOException("Selected TRL version changed.");
            trl.Read(offset, destination.Span);
        }
        Interlocked.Add(ref DownloadedBytes, destination.Length);
    }

    public async ValueTask<TrlWriteResult> WriteAsync(TrlWrite write, CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        Interlocked.Increment(ref TrlWrites);
        lock (_lock)
        {
            var trl = _trls.GetValueOrDefault(write.Key);
            if (trl?.State?.Token != write.ExpectedToken || (trl?.State?.Length ?? 0) != write.ExpectedLength)
                return new(TrlWriteOutcome.Rejected);
            trl ??= _trls[write.Key] = new(write.FileId);
            trl.Write(write);
            trl.State = new(NextVersion(), write.Length, write.Sha256);
            Interlocked.Add(ref UploadedBytes, write.AppendLength);
            return new(TrlWriteOutcome.Applied, trl.State);
        }
    }

    public async IAsyncEnumerable<TrlHead> EnumerateTrlsAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        List<TrlHead> heads = new();
        lock (_lock)
            foreach (var (key, trl) in _trls)
                if (trl.State != null) heads.Add(new(trl.FileId, key, trl.State));
        foreach (var head in heads) yield return head;
    }

    public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        List<RemoteFile> files = new();
        lock (_lock)
            foreach (var (id, file) in _immutables)
                files.Add(new(id, file.Type, file.File.GetSize(), file.Version, true, file.Sha));
        foreach (var file in files) yield return file;
    }

    public async ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        Immutable immutable;
        lock (_lock)
            if (!_immutables.TryGetValue(file.FileId, out immutable!) || immutable.Version != file.Version)
                throw new IOException("Remote version changed or disappeared.");
        var count = (int)Math.Min((ulong)buffer.Length, file.Length - offset);
        immutable.File.RandomRead(buffer.Span[..count], offset, false);
        Interlocked.Add(ref DownloadedBytes, count);
        return count;
    }

    public async ValueTask EnsurePureValuesAsync(uint remoteFileId, KeyIndexFileSource source, CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        lock (_lock)
        {
            if (_immutables.ContainsKey(remoteFileId)) return;
            var file = _files.ImportFile(remoteFileId, "pvl");
            var buffer = new byte[1024 * 1024];
            var writer = new MemWriter(file.GetAppenderWriter());
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            for (ulong offset = 0; offset < source.Length;)
            {
                var count = (int)Math.Min((ulong)buffer.Length, source.Length - offset);
                source.File.RandomRead(buffer.AsSpan(0, count), offset, false);
                writer.WriteBlock(buffer.AsSpan(0, count));
                hash.AppendData(buffer, 0, count);
                offset += (uint)count;
            }
            writer.Flush();
            _immutables.Add(remoteFileId, new(KVFileType.PureValues, file, Convert.ToHexString(hash.GetHashAndReset()), NextVersion()));
            Interlocked.Add(ref UploadedBytes, (long)source.Length);
        }
    }

    public async ValueTask PublishKeyIndexAsync(uint remoteFileId, KeyIndexSnapshot snapshot,
        IReadOnlyDictionary<uint, uint> pureValueFileIds, CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        lock (_lock)
        {
            if (_immutables.ContainsKey(remoteFileId)) return;
            var file = _files.ImportFile(remoteFileId, "kvi");
            snapshot.WriteTo(file.GetAppenderWriter(), 0, pureValueFileIds, cancellation);
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            _immutables.Add(remoteFileId, new(KVFileType.KeyIndex, file, Convert.ToHexString(SHA256.HashData(bytes)), NextVersion()));
            Interlocked.Add(ref UploadedBytes, bytes.Length);
        }
    }

    public async IAsyncEnumerable<RemoteMaintenanceFile> EnumerateMaintenanceAsync([EnumeratorCancellation] CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        List<RemoteMaintenanceFile> files = new();
        lock (_lock)
        {
            foreach (var (key, trl) in _trls)
                if (trl.State != null) files.Add(new(key, trl.FileId, KVFileType.TransactionLog, trl.State.Token));
            foreach (var (id, file) in _immutables)
                files.Add(new($"{id}.{(file.Type == KVFileType.PureValues ? "pvl" : "kvi")}", id, file.Type, file.Version));
        }
        foreach (var file in files) yield return file;
    }

    public ValueTask DeleteAsync(RemoteMaintenanceFile file, CancellationToken cancellation) =>
        throw new NotSupportedException("Measurements do not run remote cleanup.");

    public async ValueTask<bool> ProtectPureValuesAsync(uint fileId, KeyIndexFileSource source, CancellationToken cancellation)
    {
        await RequestAsync(cancellation).ConfigureAwait(false);
        lock (_lock) return _immutables.ContainsKey(fileId);
    }

    /// The restore inventory: the selected canonical TRLs plus every immutable object, like the Azure adapter.
    public IRemoteFileCollection Bind(CanonicalTrlInventory inventory) => new RestoreView(this, inventory);

    public long TrlBytes
    {
        get
        {
            lock (_lock)
            {
                long total = 0;
                foreach (var trl in _trls.Values) total += trl.State?.Length ?? 0;
                return total;
            }
        }
    }

    public long KviBytes
    {
        get
        {
            lock (_lock)
            {
                long total = 0;
                foreach (var file in _immutables.Values)
                    if (file.Type == KVFileType.KeyIndex) total += (long)file.File.GetSize();
                return total;
            }
        }
    }

    public long ImmutableBytes
    {
        get
        {
            lock (_lock)
            {
                long total = 0;
                foreach (var file in _immutables.Values) total += (long)file.File.GetSize();
                return total;
            }
        }
    }

    public void Dispose() => _files.Dispose();

    sealed class RestoreView(BenchmarkReplicationStorage storage, CanonicalTrlInventory inventory) : IRemoteFileCollection
    {
        public async IAsyncEnumerable<RemoteFile> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellation)
        {
            await foreach (var file in inventory.EnumerateAsync(cancellation).ConfigureAwait(false)) yield return file;
            await foreach (var file in storage.EnumerateAsync(cancellation).ConfigureAwait(false)) yield return file;
        }

        public ValueTask<int> ReadAsync(RemoteFile file, ulong offset, Memory<byte> buffer, CancellationToken cancellation) =>
            file.FileType == KVFileType.TransactionLog
                ? inventory.ReadAsync(file, offset, buffer, cancellation)
                : storage.ReadAsync(file, offset, buffer, cancellation);
    }
}
