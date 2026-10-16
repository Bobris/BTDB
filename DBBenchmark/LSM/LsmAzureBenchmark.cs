using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using BTDB.AzureStorage;
using BTDB.KVDBLayer;
using BTDB.LSM;
using BTDB.LSM.Azure;
using BTDB.LSM.Format;

namespace DBBenchmark.LSM;

/// End-to-end BTDB.LSM qualification on Azure Blob Storage (BTDB.LSM/ImplementationPlan.md, phases L4–L6), compared
/// with BTreeKeyValueDB on AzureBlobFileCollection for the same data. Run on same-region Azure compute with a managed
/// identity: BENCH_ACCOUNT (required), BENCH_KEYS (default 2,000,000), BENCH_VALUE (default 100 bytes),
/// BENCH_SAMPLES (default 1000), BENCH_CACHE_MIB (default 64), BENCH_MEMTABLE_MIB (default 32),
/// BENCH_COMPARE (default 1: also run BTreeKeyValueDB), BENCH_LOCAL (default /mnt/bench, local cache of the comparison).
static class LsmAzureBenchmark
{
    static int Keys => int.Parse(Environment.GetEnvironmentVariable("BENCH_KEYS") ?? "2000000");
    static int ValueSize => int.Parse(Environment.GetEnvironmentVariable("BENCH_VALUE") ?? "100");
    static int Samples => int.Parse(Environment.GetEnvironmentVariable("BENCH_SAMPLES") ?? "1000");
    static long CacheBytes => long.Parse(Environment.GetEnvironmentVariable("BENCH_CACHE_MIB") ?? "64") << 20;
    static long MemtableBytes => long.Parse(Environment.GetEnvironmentVariable("BENCH_MEMTABLE_MIB") ?? "32") << 20;

    static byte[] Key(long i)
    {
        // Pseudo-random order (SplitMix64), so bulk loading exercises flushes and compactions across the key space.
        var z = (ulong)i + 0x9E3779B97F4A7C15;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EB;
        z ^= z >> 31;
        var key = new byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(key, z);
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(8), i);
        return key;
    }

    static byte[] Value(long i)
    {
        var value = new byte[ValueSize];
        BinaryPrimitives.WriteInt64LittleEndian(value, i);
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

    static object Memory(IKeyValueDB db)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        return new
        {
            managedMiB = GC.GetTotalMemory(false) / 1048576.0,
            nativeMiB = db.GetNativeMemoryStats().AllocSize / 1048576.0,
            workingSetMiB = Environment.WorkingSet / 1048576.0
        };
    }

    public static async Task RunAsync()
    {
        var options = new BlobClientOptions();
        options.Retry.MaxRetries = 0;
        // BENCH_CONNECTION_STRING (for example Azurite) is for smoke runs of the harness only.
        var connectionString = Environment.GetEnvironmentVariable("BENCH_CONNECTION_STRING");
        var account = Environment.GetEnvironmentVariable("BENCH_ACCOUNT") ?? "connection-string";
        var service = connectionString != null
            ? new BlobServiceClient(connectionString, options)
            : new BlobServiceClient(new Uri($"https://{account}.blob.core.windows.net"),
                new ManagedIdentityCredential(ManagedIdentityId.SystemAssigned), options);
        var container = service.GetBlobContainerClient("lsm-bench-" + DateTime.UtcNow.ToString("yyyyMMddHHmmss"));
        await container.CreateIfNotExistsAsync();
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            kind = "environment", account, container = container.Name, keys = Keys, valueSize = ValueSize,
            cacheMiB = CacheBytes >> 20, memtableMiB = MemtableBytes >> 20, machine = Environment.MachineName,
            cpuCount = Environment.ProcessorCount, utc = DateTime.UtcNow
        }));
        var storage = new AzureLsmStorage(container);
        LsmKeyValueDBOptions Options(bool writer) => new()
        {
            Storage = storage, Prefix = "db/", AcquireWriterLeaseOnOpen = writer, MemtableSize = MemtableBytes,
            BlockCacheSize = CacheBytes, DeletionGracePeriod = TimeSpan.FromMinutes(1), RegisterReader = !writer
        };

        // Bulk load in transactions of 10,000 keys, then wait until background compaction settles.
        var stopwatch = Stopwatch.StartNew();
        using (var db = await LsmKeyValueDB.OpenAsync(Options(true)))
        {
            for (long done = 0; done < Keys;)
            {
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 10_000 && done < Keys; i++, done++)
                    cursor.CreateOrUpdateKeyValue(Key(done), Value(done));
                tr.SetCommitUlong((ulong)done);
                tr.Commit();
            }

            await db.FlushAsync();
            var loaded = stopwatch.Elapsed;
            db.CreateKvi(default);
            await db.Compact(default);
            Report("lsm-bulk-load", new
            {
                seconds = loaded.TotalSeconds, insertsPerSecond = Keys / loaded.TotalSeconds,
                secondsIncludingFullCompaction = stopwatch.Elapsed.TotalSeconds, memory = Memory(db),
                stats = db.CalcStats()
            });

            // Durable single-key commits: WAL group commit floor.
            var latencies = new List<double>();
            db.DurableTransactions = true;
            for (var s = 0; s < Samples; s++)
            {
                var start = Stopwatch.GetTimestamp();
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(Key(s), Value(-s));
                tr.Commit();
                latencies.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            }

            db.DurableTransactions = false;
            Report("lsm-durable-commit-1-key", Latencies(latencies));

            // Non-durable commits of 100 existing keys, made durable together every 100 commits. Each update of a key
            // that is not cached reads its block, because CreateOrUpdateKeyValue reports whether the key existed.
            stopwatch.Restart();
            var groupSamples = Math.Min(Samples, 200);
            for (var s = 0; s < groupSamples; s++)
            {
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 100; i++) cursor.CreateOrUpdateKeyValue(Key(s * 100 + i), Value(s));
                tr.Commit();
                if (s % 100 == 99) await db.FlushAsync();
            }

            Report("lsm-group-commit-100-keys", new { commitsPerSecond = groupSamples / stopwatch.Elapsed.TotalSeconds });

            // Reader in another instance: delay from the writer's durable commit until the reader sees it.
            using var follower = await LsmKeyValueDB.OpenAsync(Options(false) with
            {
                RefreshRetryInterval = TimeSpan.FromMilliseconds(10)
            });
            latencies.Clear();
            for (var s = 0; s < Math.Min(Samples, 300); s++)
            {
                ulong commitUlong;
                long number;
                using (var tr = await db.StartWritingTransaction())
                {
                    using var cursor = tr.CreateCursor();
                    cursor.CreateOrUpdateKeyValue(Key(s), Value(s));
                    commitUlong = tr.GetCommitUlong() + 1;
                    tr.SetCommitUlong(commitUlong);
                    number = tr.GetTransactionNumber();
                    tr.Commit();
                }

                var start = Stopwatch.GetTimestamp();
                await follower.WaitForCommitUlongAsync(commitUlong);
                latencies.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
                await db.WaitForDurabilityAsync(number);
            }

            Report("lsm-reader-freshness", Latencies(latencies));
        }

        // Cold open, cold and warm point reads, FindKeyIndex, and a full scan in a fresh instance.
        stopwatch.Restart();
        using (var db = await LsmKeyValueDB.OpenAsync(Options(false) with { RegisterReader = false }))
        {
            var openSeconds = stopwatch.Elapsed.TotalSeconds;
            Report("lsm-open", new { seconds = openSeconds, memory = Memory(db) });
            await PointReads(db, "lsm");
            using var tr = db.StartReadOnlyTransaction();
            using var cursor = tr.CreateCursor();
            stopwatch.Restart();
            long count = 0, bytes = 0;
            if (cursor.FindFirstKey([]))
                do
                {
                    bytes += cursor.GetStorageSizeOfCurrentKey().Value;
                    count++;
                } while (cursor.FindNextKey([]));

            Report("lsm-scan-keys", new
            {
                keys = count, seconds = stopwatch.Elapsed.TotalSeconds,
                keysPerSecond = count / stopwatch.Elapsed.TotalSeconds, memory = Memory(db)
            });
        }

        if (Environment.GetEnvironmentVariable("BENCH_COMPARE") != "0") await CompareBTree(service);
        Console.WriteLine($"Objects left in container {container.Name}; delete the account or container afterwards.");
    }

    static async Task PointReads(IKeyValueDB db, string prefix)
    {
        var random = new Random(1);
        foreach (var phase in new[] { "cold", "warm" })
        {
            using var tr = db.StartReadOnlyTransaction();
            using var cursor = tr.CreateCursor();
            var latencies = new List<double>();
            var indexLatencies = new List<double>();
            var count = tr.GetKeyValueCount();
            random = new Random(1);
            for (var s = 0; s < Samples; s++)
            {
                var i = random.NextInt64(Keys);
                var start = Stopwatch.GetTimestamp();
                if (!cursor.FindExactKey(Key(i))) throw new InvalidOperationException("Key not found");
                _ = cursor.GetValueMemory(ref Unsafe.Buffer, true);
                latencies.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
                start = Stopwatch.GetTimestamp();
                if (!cursor.FindKeyIndex(random.NextInt64(count))) throw new InvalidOperationException("Index");
                indexLatencies.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            }

            Report($"{prefix}-point-read-{phase}", new { latency = Latencies(latencies), stats = db.CalcStats() });
            Report($"{prefix}-find-key-index-{phase}", Latencies(indexLatencies));
        }

        await Task.CompletedTask;
    }

    static class Unsafe
    {
        public static Memory<byte> Buffer;
    }

    /// The same data in BTreeKeyValueDB on AzureBlobFileCollection: memory, local cache size and open time.
    static async Task CompareBTree(BlobServiceClient service)
    {
        var options = new BlobClientOptions();
        var container = service.GetBlobContainerClient("btree-bench-" + DateTime.UtcNow.ToString("yyyyMMddHHmmss"));
        await container.CreateIfNotExistsAsync();
        var local = Environment.GetEnvironmentVariable("BENCH_LOCAL") ?? "/mnt/bench";
        var directory = Path.Combine(local, Guid.NewGuid().ToString("N"));
        var stopwatch = Stopwatch.StartNew();
        await using (var files = await AzureBlobFileCollection.CreateAsync(new()
                     {
                         BlobStorageBackend = new AzureBlobStorageBackend(container), LocalCacheDirectory = directory,
                         TransactionLogFlushPeriod = TimeSpan.FromSeconds(5)
                     }))
        {
            using var db = new BTreeKeyValueDB(files);
            for (long done = 0; done < Keys;)
            {
                using var tr = await db.StartWritingTransaction();
                using var cursor = tr.CreateCursor();
                for (var i = 0; i < 10_000 && done < Keys; i++, done++)
                    cursor.CreateOrUpdateKeyValue(Key(done), Value(done));
                tr.Commit();
            }

            db.CreateKvi(default);
            await files.FlushPendingChangesAsync();
            Report("btree-bulk-load", new
            {
                seconds = stopwatch.Elapsed.TotalSeconds, insertsPerSecond = Keys / stopwatch.Elapsed.TotalSeconds,
                memory = Memory(db)
            });
        }

        var localBytes = Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories)
            .Sum(f => new FileInfo(f).Length);
        Directory.Delete(directory, true);
        stopwatch.Restart();
        await using (var files = await AzureBlobFileCollection.CreateAsync(new()
                     {
                         BlobStorageBackend = new AzureBlobStorageBackend(container), LocalCacheDirectory = directory,
                         DeleteLocalCacheDirectoryOnDispose = true
                     }))
        {
            using var db = new BTreeKeyValueDB(files);
            Report("btree-open-from-blob", new
            {
                seconds = stopwatch.Elapsed.TotalSeconds, localCacheMiB = localBytes / 1048576.0, memory = Memory(db)
            });
            await PointReads(db, "btree");
        }

        _ = options;
    }
}
