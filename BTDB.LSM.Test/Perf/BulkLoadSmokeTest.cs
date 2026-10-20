using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BTDB.LSM.Storage;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.LSM.Test.Perf;

/// Adds object-storage-like latency (Measurements.md, section 2, Standard LRS p50) to an in-memory storage: a fixed
/// cost per request plus transfer time, and counts requests and bytes per kind.
sealed class LatencyLsmStorage(ILsmStorage inner) : ILsmStorage
{
    const double WriteBytesPerMs = 80_000; // about 80 MB/s per request
    const double ReadBytesPerMs = 150_000;

    readonly Dictionary<string, (long Count, long Bytes, long Ticks)> _stats = new();

    static TimeSpan Cost(double fixedMs, long bytes, double bytesPerMs) =>
        TimeSpan.FromMilliseconds(fixedMs + bytes / bytesPerMs);

    void Count(string kind, long bytes, long start)
    {
        var elapsed = Stopwatch.GetTimestamp() - start;
        lock (_stats)
        {
            _stats.TryGetValue(kind, out var current);
            _stats[kind] = (current.Count + 1, current.Bytes + bytes, current.Ticks + elapsed);
        }
    }

    public string Report()
    {
        lock (_stats)
        {
            var lines = new List<string>();
            foreach (var (kind, (count, bytes, ticks)) in _stats)
                lines.Add($"{kind}: {count} requests, {bytes / 1048576.0:F1} MiB, " +
                          $"{Stopwatch.GetElapsedTime(0, ticks).TotalSeconds:F1} s summed");
            lines.Sort(StringComparer.Ordinal);
            return string.Join(Environment.NewLine, lines);
        }
    }

    static string Kind(string key) => key.Contains("wal/") ? "wal" : key.Contains("tables/") ? "tables" :
        key.Contains("values/") ? "values" : key.Contains("manifest/") ? "manifest" : "other";

    public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        var start = Stopwatch.GetTimestamp();
        await Task.Delay(Cost(8, content.Length, WriteBytesPerMs), cancellation);
        var outcome = await inner.CreateAsync(key, content, cancellation);
        Count("create " + Kind(key), content.Length, start);
        return outcome;
    }

    public async ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation)
    {
        var start = Stopwatch.GetTimestamp();
        var result = await inner.TryReadAsync(key, cancellation);
        await Task.Delay(Cost(4, result?.Length ?? 0, ReadBytesPerMs), cancellation);
        Count("get " + Kind(key), result?.Length ?? 0, start);
        return result;
    }

    public async ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        var start = Stopwatch.GetTimestamp();
        await Task.Delay(Cost(4, destination.Length, ReadBytesPerMs), cancellation);
        await inner.ReadRangeAsync(key, offset, destination, cancellation);
        Count("async range " + Kind(key), destination.Length, start);
    }

    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        var start = Stopwatch.GetTimestamp();
        Thread.Sleep(Cost(4, destination.Length, ReadBytesPerMs));
        inner.ReadRange(key, offset, destination);
        var thread = Thread.CurrentThread.Name?.StartsWith("BTDB.LSM read") == true ? "readahead" : "cursor";
        Count(thread + " range " + Kind(key), destination.Length, start);
    }

    public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) =>
        inner.ListAsync(prefix, cancellation);

    public async ValueTask DeleteAsync(string key, CancellationToken cancellation)
    {
        await Task.Delay(Cost(5, 0, 1), cancellation);
        await inner.DeleteAsync(key, cancellation);
    }

    public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);

    public async ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration,
        CancellationToken cancellation)
    {
        var start = Stopwatch.GetTimestamp();
        await Task.Delay(Cost(5, 0, 1), cancellation);
        var renewed = await inner.RenewLeaseAsync(key, leaseId, duration, cancellation);
        Count("renew lease", 0, start);
        return renewed;
    }

    public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) =>
        inner.ReleaseLeaseAsync(key, leaseId, cancellation);
}

/// Random-key bulk load like DBBenchmark lsm-azure on in-memory storage; prints throughput and the background work and
/// write stall breakdown for Measurements.md and asserts nothing about speed.
public class BulkLoadSmokeTest(ITestOutputHelper output)
{
    static byte[] Key(long i)
    {
        var z = (ulong)i + 0x9E3779B97F4A7C15;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EB;
        z ^= z >> 31;
        var key = new byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(key, z);
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(8), i);
        return key;
    }

    /// BTDB_LSM_BULK_KEYS and BTDB_LSM_BULK_MEMTABLE_MIB scale the run; Measurements.md used 2,000,000 keys and 32 MiB.
    [Theory]
    [InlineData(false, 16)]
    [InlineData(true, 10)]
    [InlineData(true, 16)]
    [InlineData(true, 20)]
    public async Task RandomKeyBulkLoad(bool latency, int bloomBitsPerKey)
    {
        var keyCount = int.Parse(Environment.GetEnvironmentVariable("BTDB_LSM_BULK_KEYS") ?? "100000");
        var memtableMiB = int.Parse(Environment.GetEnvironmentVariable("BTDB_LSM_BULK_MEMTABLE_MIB") ?? "2");
        var storage = latency ? new LatencyLsmStorage(new InMemoryLsmStorage()) : null;
        using var db = await LsmKeyValueDB.OpenAsync(new()
        {
            Storage = storage ?? (ILsmStorage)new InMemoryLsmStorage(), AcquireWriterLeaseOnOpen = true,
            MemtableSize = memtableMiB << 20, BloomBitsPerKey = bloomBitsPerKey,
            BlockCacheSize = 64 << 20
        });
        var value = new byte[100];
        var stopwatch = Stopwatch.StartNew();
        var inserting = TimeSpan.Zero;
        for (long done = 0; done < keyCount;)
        {
            using var tr = await db.StartWritingTransaction();
            using var cursor = tr.CreateCursor();
            var start = Stopwatch.GetTimestamp();
            for (var i = 0; i < 10_000 && done < keyCount; i++, done++)
            {
                BinaryPrimitives.WriteInt64LittleEndian(value, done);
                cursor.CreateOrUpdateKeyValue(Key(done), value);
            }

            inserting += Stopwatch.GetElapsedTime(start);
            tr.Commit();
        }

        await db.FlushAsync();
        var loaded = stopwatch.Elapsed;
        output.WriteLine($"{keyCount} keys, {memtableMiB} MiB memtable, {(latency ? "simulated latency" : "no latency")}, " +
                         $"{bloomBitsPerKey} bloom bits per key: " +
                         $"{keyCount / loaded.TotalSeconds:F0} inserts/s, " +
                         $"{loaded.TotalSeconds:F1} s, inserting {inserting.TotalSeconds:F1} s");
        output.WriteLine(db.CalcStats());
        if (storage != null) output.WriteLine(storage.Report());
    }
}
