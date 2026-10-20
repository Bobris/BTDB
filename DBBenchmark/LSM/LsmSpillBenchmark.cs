using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using BTDB.KVDBLayer;
using BTDB.LSM;
using BTDB.LSM.Azure;

namespace DBBenchmark.LSM;

/// Spilled writing transactions and writer handoff of BTDB.LSM on Azure Blob Storage (BTDB.LSM/Measurements.md,
/// section 8). Run on same-region Azure compute with a managed identity: BENCH_ACCOUNT (required, or
/// BENCH_CONNECTION_STRING for smoke runs), BENCH_BASE_KEYS (default 2,000,000 keys of 100 bytes loaded first),
/// BENCH_TX_MIB (default 2048: key and value bytes written by the large transaction), BENCH_LARGE_VALUE (default
/// 65,536 bytes; a quarter of the transaction's bytes are such values), BENCH_TX_MEMORY_MIB (default 64,
/// MaxTransactionMemory), BENCH_UPDATE_EVERY (default 2000: every n-th new key also updates a random existing key,
/// which costs one uncached ranged read), BENCH_MEMTABLE_MIB (default 64), BENCH_CACHE_MIB (default 256),
/// BENCH_HANDOFFS (default 50), BENCH_BLOOM_BITS (default 16).
static class LsmSpillBenchmark
{
    static long Env(string name, long fallback) =>
        long.Parse(Environment.GetEnvironmentVariable(name) ?? fallback.ToString());

    static readonly long BaseKeys = Env("BENCH_BASE_KEYS", 2_000_000);
    static readonly long TransactionBytes = Env("BENCH_TX_MIB", 2048) << 20;
    static readonly int LargeValue = (int)Env("BENCH_LARGE_VALUE", 65_536);
    static readonly long TransactionMemory = Env("BENCH_TX_MEMORY_MIB", 64) << 20;
    static readonly long MemtableBytes = Env("BENCH_MEMTABLE_MIB", 64) << 20;
    static readonly long CacheBytes = Env("BENCH_CACHE_MIB", 256) << 20;
    static readonly int Handoffs = (int)Env("BENCH_HANDOFFS", 50);
    static readonly long UpdateEvery = Env("BENCH_UPDATE_EVERY", 2000);
    static readonly int BloomBits = (int)Env("BENCH_BLOOM_BITS", 16);

    static byte[] Key(long i)
    {
        // Pseudo-random order (SplitMix64), as in LsmAzureBenchmark.
        var z = (ulong)i + 0x9E3779B97F4A7C15;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EB;
        z ^= z >> 31;
        var key = new byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(key, z);
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(8), i);
        return key;
    }

    static byte[] Value(long i, int size)
    {
        var value = new byte[size];
        BinaryPrimitives.WriteInt64LittleEndian(value, i);
        // Large values get some entropy, so they do not compress to nothing.
        if (size > 1024) new Random((int)i).NextBytes(value.AsSpan(8, size / 2));
        return value;
    }

    static void Report(string name, object values) =>
        Console.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["kind"] = "case", ["name"] = name, ["values"] = values, ["utc"] = DateTime.UtcNow
        }));

    static object Latencies(List<double> milliseconds)
    {
        milliseconds.Sort();
        double Q(double q) => milliseconds.Count == 0 ? double.NaN :
            milliseconds[Math.Min(milliseconds.Count - 1, (int)Math.Ceiling(q * milliseconds.Count) - 1)];
        return new { count = milliseconds.Count, p50 = Q(0.5), p95 = Q(0.95), p99 = Q(0.99), max = Q(1) };
    }

    /// Level 0 tables and runs; the level 0 triggers count runs.
    static (int Tables, int Runs) Level0(LsmKeyValueDB db) =>
        Regex.Match(db.CalcStats(), @"^Level0:(\d+) tables, \d+ bytes, (\d+) runs", RegexOptions.Multiline) is
            { Success: true } match
            ? (int.Parse(match.Groups[1].Value), int.Parse(match.Groups[2].Value))
            : (0, 0);

    /// Samples native memory (block cache, memtables, retired arenas), managed heap and working set.
    sealed class MemorySampler : IDisposable
    {
        readonly CancellationTokenSource _stop = new();
        readonly Task _task;
        double _nativeMiB, _managedMiB, _workingSetMiB;

        public MemorySampler(IKeyValueDB db)
        {
            _task = Task.Run(async () =>
            {
                while (!_stop.IsCancellationRequested)
                {
                    _nativeMiB = Math.Max(_nativeMiB, db.GetNativeMemoryStats().AllocSize / 1048576.0);
                    _managedMiB = Math.Max(_managedMiB, GC.GetTotalMemory(false) / 1048576.0);
                    _workingSetMiB = Math.Max(_workingSetMiB, Environment.WorkingSet / 1048576.0);
                    try
                    {
                        await Task.Delay(100, _stop.Token);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
            });
        }

        public object Peak => new
        {
            nativeMiB = _nativeMiB, managedMiB = _managedMiB, workingSetMiB = _workingSetMiB,
            processPeakWorkingSetMiB = Process.GetCurrentProcess().PeakWorkingSet64 / 1048576.0
        };

        public void Dispose()
        {
            _stop.Cancel();
            _task.GetAwaiter().GetResult();
        }
    }

    public static async Task RunAsync()
    {
        var options = new BlobClientOptions();
        options.Retry.MaxRetries = 0;
        var connectionString = Environment.GetEnvironmentVariable("BENCH_CONNECTION_STRING");
        var account = Environment.GetEnvironmentVariable("BENCH_ACCOUNT") ?? "connection-string";
        var service = connectionString != null
            ? new BlobServiceClient(connectionString, options)
            : new BlobServiceClient(new Uri($"https://{account}.blob.core.windows.net"),
                new ManagedIdentityCredential(ManagedIdentityId.SystemAssigned), options);
        var container = service.GetBlobContainerClient("lsm-spill-" + DateTime.UtcNow.ToString("yyyyMMddHHmmss"));
        await container.CreateIfNotExistsAsync();
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            kind = "environment", account, container = container.Name, baseKeys = BaseKeys,
            transactionMiB = TransactionBytes >> 20, largeValue = LargeValue,
            transactionMemoryMiB = TransactionMemory >> 20, memtableMiB = MemtableBytes >> 20,
            cacheMiB = CacheBytes >> 20, bloomBits = BloomBits, machine = Environment.MachineName, cpuCount = Environment.ProcessorCount,
            utc = DateTime.UtcNow
        }));
        var storage = new AzureLsmStorage(container);
        LsmKeyValueDBOptions Options(bool writer) => new()
        {
            Storage = storage, Prefix = "db/", AcquireWriterLeaseOnOpen = writer, MemtableSize = MemtableBytes,
            BlockCacheSize = CacheBytes, MaxTransactionMemory = TransactionMemory, BloomBitsPerKey = BloomBits,
            DeletionGracePeriod = TimeSpan.FromMinutes(1), RefreshRetryInterval = TimeSpan.FromMilliseconds(10)
        };

        var first = await LsmKeyValueDB.OpenAsync(Options(true));
        var second = await LsmKeyValueDB.OpenAsync(Options(false));
        try
        {
            // Base data in transactions of 10,000 keys, compacted into the bottom level.
            var stopwatch = Stopwatch.StartNew();
            for (long done = 0; done < BaseKeys;)
            {
                using var tr = await first.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 10_000 && done < BaseKeys; i++, done++)
                    cursor.CreateOrUpdateKeyValue(Key(done), Value(done, 100));
                tr.SetCommitUlong((ulong)done);
                tr.Commit();
            }

            await first.FlushAsync();
            await first.Compact(default);
            Report("base-load", new { keys = BaseKeys, seconds = stopwatch.Elapsed.TotalSeconds });

            // One large transaction: new keys with 100-byte values, some keys also update an existing one, and a
            // quarter of the bytes are large values.
            long keys = 0, bytes = 0, updates = 0, large = 0;
            var next = BaseKeys;
            var random = new Random(1);
            object peak;
            TimeSpan write, commit;
            long committedAt;
            using (var sampler = new MemorySampler(first))
            {
                stopwatch.Restart();
                using (var tr = await first.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    while (bytes < TransactionBytes)
                    {
                        var isLarge = bytes > 0 && large * 4 < bytes;
                        var size = isLarge ? LargeValue : 100;
                        cursor.CreateOrUpdateKeyValue(Key(next), Value(next, size));
                        next++;
                        keys++;
                        bytes += 16 + size;
                        if (isLarge) large += size;
                        if (keys % UpdateEvery == 0)
                        {
                            var existing = random.NextInt64(BaseKeys);
                            cursor.CreateOrUpdateKeyValue(Key(existing), Value(-existing, 100));
                            updates++;
                            bytes += 116;
                        }
                    }

                    tr.SetCommitUlong(1_000_000_000);
                    write = stopwatch.Elapsed;
                    stopwatch.Restart();
                    tr.Commit();
                    commit = stopwatch.Elapsed;
                    committedAt = Stopwatch.GetTimestamp();
                }

                peak = sampler.Peak;
            }

            var (level0TablesAfterCommit, level0RunsAfterCommit) = Level0(first);
            Report("large-transaction", new
            {
                keys, updates, largeValues = large / LargeValue, mebibytes = bytes / 1048576.0,
                writeSeconds = write.TotalSeconds, commitSeconds = commit.TotalSeconds,
                mebibytesPerSecond = bytes / 1048576.0 / (write + commit).TotalSeconds, peak,
                level0TablesAfterCommit, level0RunsAfterCommit, stats = first.CalcStats()
            });

            // Another instance sees the commit after one manifest read.
            stopwatch.Restart();
            await second.WaitForCommitUlongAsync(1_000_000_000);
            Report("reader-sees-large-commit", new
            {
                seconds = stopwatch.Elapsed.TotalSeconds,
                nativeMiB = second.GetNativeMemoryStats().AllocSize / 1048576.0
            });
            using (var tr = second.StartReadOnlyTransaction())
            {
                var expected = BaseKeys + keys;
                if (tr.GetKeyValueCount() != expected)
                    throw new InvalidOperationException($"Reader sees {tr.GetKeyValueCount()} keys, expected {expected}.");
            }

            // The next small writer waits while level 0 holds too many tables.
            stopwatch.Restart();
            using (var tr = await first.StartWritingTransaction())
            {
                var waited = stopwatch.Elapsed;
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key(next), Value(next, 100));
                tr.SetCommitUlong(1_000_000_001);
                tr.Commit();
                Report("next-writer-after-large-commit", new { waitSeconds = waited.TotalSeconds });
            }

            // Level 0 compaction starts at Level0CompactionTrigger (4) runs.
            while (Level0(first).Runs >= 4) await Task.Delay(250);
            Report("level0-compacted", new
            {
                secondsSinceCommit = Stopwatch.GetElapsedTime(committedAt).TotalSeconds, stats = first.CalcStats()
            });
            await first.FlushAsync();

            // Handoff: the writer commits and releases the lease, the other instance takes over and commits.
            var release = new List<double>();
            var takeover = new List<double>();
            var commitUlong = 1_000_000_001UL;
            var (writer, reader) = (first, second);
            for (var i = 0; i < Handoffs; i++)
            {
                using (var tr = await writer.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue(Key(i), Value(i, 100));
                    tr.SetCommitUlong(++commitUlong);
                    tr.Commit();
                }

                var start = Stopwatch.GetTimestamp();
                await writer.ReleaseWriterLeaseAsync();
                release.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
                start = Stopwatch.GetTimestamp();
                using (var tr = await reader.StartWritingTransaction())
                {
                    takeover.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
                    if (tr.GetCommitUlong() != commitUlong)
                        throw new InvalidOperationException($"Takeover saw {tr.GetCommitUlong()}, expected {commitUlong}.");
                }

                (writer, reader) = (reader, writer);
            }

            Report("handoff-release", Latencies(release));
            Report("handoff-takeover", Latencies(takeover));
        }
        finally
        {
            first.Dispose();
            second.Dispose();
            await container.DeleteIfExistsAsync();
        }
    }
}
