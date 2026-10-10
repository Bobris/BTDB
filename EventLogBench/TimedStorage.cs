using BTDB.Replication.EventLog;

namespace EventLogBench;

/// <summary>Records the duration and size of every storage call, so a scenario can attribute latency to Azure.</summary>
public sealed class TimedStorage(IEventLogStorage inner) : IEventLogStorage
{
    readonly object _lock = new();
    List<(double Ms, long Bytes)> _writes = [];
    List<double> _reads = [];
    readonly Dictionary<string, long> _lengths = new();
    long _newBytes;
    HashSet<string> _keys = [];

    public void Reset()
    {
        lock (_lock)
        {
            _writes = [];
            _reads = [];
            _newBytes = 0;
            _keys = [];
        }
    }

    public object Snapshot()
    {
        List<(double Ms, long Bytes)> writes;
        List<double> reads;
        long newBytes;
        string[] keys;
        lock (_lock)
        {
            writes = [.. _writes];
            reads = [.. _reads];
            newBytes = _newBytes;
            keys = [.. _keys];
        }
        var latencies = writes.Select(w => w.Ms).Order().ToList();
        reads.Sort();
        return new
        {
            writes = writes.Count, newBytesPerWrite = writes.Count == 0 ? 0 : newBytes / (double)writes.Count,
            objects = keys.Length, objectKeys = keys.Order().Take(40).ToArray(), writeBytesMean = writes.Count == 0 ? 0 : writes.Average(w => (double)w.Bytes),
            writeP50 = Q(latencies, .5), writeP90 = Q(latencies, .9), writeP99 = Q(latencies, .99),
            writeMax = latencies.Count == 0 ? (double?)null : latencies[^1],
            reads = reads.Count, readP50 = Q(reads, .5), readP99 = Q(reads, .99)
        };
    }

    static double? Q(List<double> sorted, double q) => sorted.Count == 0 ? null
        : Math.Round(sorted[Math.Clamp((int)Math.Ceiling(q * sorted.Count) - 1, 0, sorted.Count - 1)], 3);

    void Write(long start, string key, long bytes)
    {
        var ms = (BenchClock.NowUs - start) / 1000.0;
        lock (_lock)
        {
            _writes.Add((ms, bytes));
            _keys.Add(key);
            _lengths.TryGetValue(key, out var previous);
            _newBytes += Math.Max(0, bytes - previous);
            if (bytes > 0) _lengths[key] = bytes;
        }
    }

    void Read(long start)
    {
        var ms = (BenchClock.NowUs - start) / 1000.0;
        lock (_lock) _reads.Add(ms);
    }

    public async ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation)
    {
        var start = BenchClock.NowUs;
        try { return await inner.ReadAsync(key, cancellation); }
        finally { Read(start); }
    }

    public async ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation)
    {
        var start = BenchClock.NowUs;
        try { return await inner.GetPropertiesAsync(key, cancellation); }
        finally { Read(start); }
    }

    public async ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        var start = BenchClock.NowUs;
        try { await inner.ReadRangeAsync(key, version, offset, destination, cancellation); }
        finally { Read(start); }
    }

    public async ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion,
        ReadOnlyMemory<byte> content, EventLogOwner? owner, string? sha256, CancellationToken cancellation)
    {
        var start = BenchClock.NowUs;
        try { return await inner.WriteAsync(key, expectedVersion, content, owner, sha256, cancellation); }
        finally { Write(start, key, content.Length); }
    }

    public async ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner,
        CancellationToken cancellation)
    {
        var start = BenchClock.NowUs;
        try { return await inner.SetOwnerAsync(key, expectedVersion, owner, cancellation); }
        finally { Write(start, key, 0); }
    }

    public IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken cancellation) =>
        inner.ListAsync(prefix, cancellation);

    public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay,
        CancellationToken cancellation) => inner.ScheduleDeletionAsync(key, version, delay, cancellation);

    public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation) =>
        inner.DeleteAsync(key, version, cancellation);
}
