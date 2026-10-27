using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using BTDB.LSM;
using BTDB.LSM.Azure;
using BTDB.LSM.Storage;

namespace DBBenchmark.LSM;

/// Durable single-key commits of BTDB.LSM on Azure Blob Storage with and without the adaptive WAL upload window
/// (LsmKeyValueDBOptions.AdaptiveWalLingerMax; BTDB.LSM/Measurements.md, section 9). Every commit waits for
/// WaitForDurabilityAsync(its transaction number). Closed loops: n writers each commit and wait in a loop. Open loops:
/// independent commits arrive at a Poisson rate. Mixed: an open loop plus closed-loop writers. Every case runs once per
/// mode on a fresh database, alternating which mode goes first. Run on same-region Azure compute with a managed
/// identity: BENCH_ACCOUNT (required, or BENCH_CONNECTION_STRING for smoke runs), BENCH_SECONDS (default 15 measured
/// seconds per case after BENCH_WARMUP_SECONDS, default 3), BENCH_REPEATS (default 2), BENCH_VALUE (default 100
/// bytes), BENCH_CLOSED (default "1,2,4,8,16,64,256" writers), BENCH_OPEN (default "100,1000" commits/s),
/// BENCH_MIXED (default "1000:8", rate:writers), BENCH_MODES (default "off,adaptive").
static class LsmDurableBenchmark
{
    static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;

    static readonly TimeSpan Measured = TimeSpan.FromSeconds(double.Parse(Env("BENCH_SECONDS", "15")));
    static readonly TimeSpan Warmup = TimeSpan.FromSeconds(double.Parse(Env("BENCH_WARMUP_SECONDS", "3")));
    static readonly int Repeats = int.Parse(Env("BENCH_REPEATS", "2"));
    static readonly int ValueSize = int.Parse(Env("BENCH_VALUE", "100"));

    static int[] List(string name, string fallback) =>
        Env(name, fallback).Split(',', StringSplitOptions.RemoveEmptyEntries).Select(int.Parse).ToArray();

    sealed record Case(string Name, int Writers, double Rate);

    static readonly JsonSerializerOptions JsonOptions = new()
    {
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals
    };

    static void Report(string name, object values) =>
        Console.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["kind"] = "case", ["name"] = name, ["values"] = values, ["utc"] = DateTime.UtcNow
        }, JsonOptions));

    static object Latencies(List<double> milliseconds)
    {
        milliseconds.Sort();
        double Q(double q) => milliseconds.Count == 0 ? double.NaN :
            milliseconds[Math.Min(milliseconds.Count - 1, (int)Math.Ceiling(q * milliseconds.Count) - 1)];
        return new { count = milliseconds.Count, p50 = Q(0.5), p95 = Q(0.95), p99 = Q(0.99), max = Q(1) };
    }

    /// Counts WAL chunk creates and their latency.
    sealed class CountingStorage(ILsmStorage inner) : ILsmStorage
    {
        readonly List<(long Start, double Milliseconds)> _walCreates = [];

        public List<double> WalCreates(long from, long to)
        {
            lock (_walCreates)
                return _walCreates.Where(c => c.Start >= from && c.Start < to).Select(c => c.Milliseconds).ToList();
        }

        public async ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
            CancellationToken cancellation)
        {
            var start = Stopwatch.GetTimestamp();
            var outcome = await inner.CreateAsync(key, content, cancellation);
            if (key.Contains("wal/", StringComparison.Ordinal))
                lock (_walCreates)
                    _walCreates.Add((start, Stopwatch.GetElapsedTime(start).TotalMilliseconds));
            return outcome;
        }

        public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation) =>
            inner.TryReadAsync(key, cancellation);

        public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination,
            CancellationToken cancellation) => inner.ReadRangeAsync(key, offset, destination, cancellation);

        public void ReadRange(string key, long offset, Span<byte> destination) =>
            inner.ReadRange(key, offset, destination);

        public IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix, CancellationToken cancellation) =>
            inner.ListAsync(prefix, cancellation);

        public ValueTask DeleteAsync(string key, CancellationToken cancellation) => inner.DeleteAsync(key, cancellation);

        public ValueTask<bool> TryAcquireLeaseAsync(string key, string leaseId, TimeSpan duration,
            CancellationToken cancellation) => inner.TryAcquireLeaseAsync(key, leaseId, duration, cancellation);

        public ValueTask<bool> RenewLeaseAsync(string key, string leaseId, TimeSpan duration,
            CancellationToken cancellation) => inner.RenewLeaseAsync(key, leaseId, duration, cancellation);

        public ValueTask ReleaseLeaseAsync(string key, string leaseId, CancellationToken cancellation) =>
            inner.ReleaseLeaseAsync(key, leaseId, cancellation);
    }

    /// Latencies of commits started in the measured window, by source.
    sealed class Samples
    {
        readonly List<(long Start, double Milliseconds, bool Open)> _samples = [];

        public void Add(long start, bool open)
        {
            var milliseconds = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            lock (_samples) _samples.Add((start, milliseconds, open));
        }

        public List<double> Get(long from, long to, bool open)
        {
            lock (_samples)
                return _samples.Where(s => s.Start >= from && s.Start < to && s.Open == open)
                    .Select(s => s.Milliseconds).ToList();
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
        var container = service.GetBlobContainerClient("lsm-durable-" + DateTime.UtcNow.ToString("yyyyMMddHHmmss"));
        await container.CreateIfNotExistsAsync();
        var modes = Env("BENCH_MODES", "off,adaptive").Split(',', StringSplitOptions.RemoveEmptyEntries);
        var cases = new List<Case>();
        cases.AddRange(List("BENCH_CLOSED", "1,2,4,8,16,64,256").Select(n => new Case($"closed-{n}", n, 0)));
        cases.AddRange(List("BENCH_OPEN", "100,1000").Select(r => new Case($"open-{r}", 0, r)));
        foreach (var mixed in Env("BENCH_MIXED", "1000:8").Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = mixed.Split(':');
            cases.Add(new($"mixed-{parts[0]}-{parts[1]}", int.Parse(parts[1]), double.Parse(parts[0])));
        }

        Console.WriteLine(JsonSerializer.Serialize(new
        {
            kind = "environment", account, container = container.Name, measuredSeconds = Measured.TotalSeconds,
            warmupSeconds = Warmup.TotalSeconds, repeats = Repeats, valueSize = ValueSize, modes,
            cases = cases.Select(c => c.Name), machine = Environment.MachineName,
            cpuCount = Environment.ProcessorCount, utc = DateTime.UtcNow
        }));

        var storage = new CountingStorage(new AzureLsmStorage(container));
        for (var repeat = 0; repeat < Repeats; repeat++)
        {
            foreach (var benchCase in cases)
            {
                var order = repeat % 2 == 0 ? modes : modes.Reverse().ToArray();
                foreach (var mode in order)
                    await RunCaseAsync(storage, benchCase, mode, repeat);
            }
        }
    }

    static async Task RunCaseAsync(CountingStorage storage, Case benchCase, string mode, int repeat)
    {
        var dbOptions = new LsmKeyValueDBOptions
        {
            Storage = storage, Prefix = $"{benchCase.Name}-{mode}-{repeat}/", AcquireWriterLeaseOnOpen = true,
            DeletionGracePeriod = TimeSpan.FromMinutes(1)
        };
        if (mode == "off") dbOptions = dbOptions with { AdaptiveWalLingerMax = TimeSpan.Zero };
        using var db = await LsmKeyValueDB.OpenAsync(dbOptions);
        var samples = new Samples();
        long keyCounter = 0;
        var started = Stopwatch.GetTimestamp();
        var from = started + (long)(Warmup.TotalSeconds * Stopwatch.Frequency);
        var to = from + (long)(Measured.TotalSeconds * Stopwatch.Frequency);
        using var stop = new CancellationTokenSource(Warmup + Measured);

        async Task CommitAsync(long start, bool open)
        {
            var key = new byte[8];
            var value = new byte[ValueSize];
            BitConverter.TryWriteBytes(key, Interlocked.Increment(ref keyCounter));
            long transactionNumber;
            using (var tr = await db.StartWritingTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.CreateOrUpdateKeyValue(key, value);
                tr.Commit();
                transactionNumber = tr.GetTransactionNumber();
            }

            await db.WaitForDurabilityAsync(transactionNumber);
            samples.Add(start, open);
        }

        var tasks = Enumerable.Range(0, benchCase.Writers).Select(_ => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested) await CommitAsync(Stopwatch.GetTimestamp(), false);
        })).ToList();

        // Poisson arrivals on a dedicated thread: sleeps while the next arrival is far and spins when it is close.
        var openTasks = new List<Task>();
        Thread? launcher = null;
        if (benchCase.Rate > 0)
        {
            launcher = new(() =>
            {
                var random = new Random(repeat);
                var next = Stopwatch.GetTimestamp();
                while (!stop.IsCancellationRequested)
                {
                    next += (long)(-Math.Log(1 - random.NextDouble()) / benchCase.Rate * Stopwatch.Frequency);
                    while (true)
                    {
                        var ahead = (next - Stopwatch.GetTimestamp()) * 1000.0 / Stopwatch.Frequency;
                        if (ahead <= 0) break;
                        if (ahead > 2) Thread.Sleep(1);
                        else Thread.SpinWait(100);
                    }

                    var scheduled = next;
                    var task = Task.Run(() => CommitAsync(scheduled, true));
                    lock (openTasks) openTasks.Add(task);
                }
            }) { IsBackground = true, Name = "open loop" };
            launcher.Start();
        }

        // The window chosen by the uploader, sampled during the measured part.
        var windows = new List<double>();
        var sampler = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                if (Stopwatch.GetTimestamp() >= from) windows.Add(db.WalLinger.TotalMilliseconds);
                try
                {
                    await Task.Delay(50, stop.Token);
                }
                catch (OperationCanceledException)
                {
                }
            }
        });

        await Task.WhenAll(tasks);
        launcher?.Join();
        Task[] pending;
        lock (openTasks) pending = openTasks.ToArray();
        await Task.WhenAll(pending);
        await sampler;

        var closed = samples.Get(from, to, false);
        var open = samples.Get(from, to, true);
        var uploads = storage.WalCreates(from, to);
        windows.Sort();
        Report(benchCase.Name, new
        {
            mode, repeat, writers = benchCase.Writers, rate = benchCase.Rate,
            commitsPerSecond = (closed.Count + open.Count) / Measured.TotalSeconds,
            walCreates = uploads.Count,
            commitsPerWalCreate = uploads.Count == 0 ? double.NaN : (double)(closed.Count + open.Count) / uploads.Count,
            closedMs = Latencies(closed), openMs = Latencies(open), walCreateMs = Latencies(uploads),
            windowMsMedian = windows.Count == 0 ? 0 : windows[windows.Count / 2],
            windowMsMax = windows.Count == 0 ? 0 : windows[^1]
        });
    }
}
