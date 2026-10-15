using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;

namespace DBBenchmark.LSM;

/// Closed-loop Azure Blob storage primitives behind the BTDB.LSM read and WAL design (BTDB.LSM/ImplementationPlan.md,
/// phase L0): ranged GET latency by size, sync versus async reads, parallel and thread-pool behavior, sequential
/// throughput, 404 probes, small GETs, properties, listing, create-if-absent chunk writes and lease renewal.
/// Run it on same-region Azure compute with a managed identity:
/// BENCH_ACCOUNT (required unless BENCH_CONNECTION_STRING is set), BENCH_CONTAINER (default lsm-latency),
/// BENCH_SAMPLES (default 1000), BENCH_FILTER (case-name substring), BENCH_TABLE_MIB (default 256, size of the object
/// read by range cases), BENCH_TOKEN (optional bearer token instead of the IMDS managed identity).
/// BENCH_CONNECTION_STRING (for example Azurite) authenticates with a container SAS instead; it is meant for smoke
/// runs of the harness, not for measurements.
/// No automatic retries; an error terminates the current case and remains visible in its result.
static class LsmStorageScreening
{
    public static async Task RunAsync()
    {
        var container = Environment.GetEnvironmentVariable("BENCH_CONTAINER") ?? "lsm-latency";
        var samples = int.Parse(Environment.GetEnvironmentVariable("BENCH_SAMPLES") ?? "1000");
        var tableMib = int.Parse(Environment.GetEnvironmentVariable("BENCH_TABLE_MIB") ?? "256");
        var filter = Environment.GetEnvironmentVariable("BENCH_FILTER");
        var prefix = DateTime.UtcNow.ToString("yyyyMMddTHHmmss") + "-" + Guid.NewGuid().ToString("N");
        using var handler = new SocketsHttpHandler
            { MaxConnectionsPerServer = 256, PooledConnectionLifetime = TimeSpan.FromMinutes(10) };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(60) };
        http.DefaultRequestHeaders.Add("x-ms-version", "2023-11-03");

        string root;
        var sasQuery = "";
        var connectionString = Environment.GetEnvironmentVariable("BENCH_CONNECTION_STRING");
        if (connectionString != null)
        {
            var containerClient = new BlobServiceClient(connectionString).GetBlobContainerClient(container);
            await containerClient.CreateIfNotExistsAsync();
            var sas = containerClient.GenerateSasUri(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddHours(12));
            root = sas.GetLeftPart(UriPartial.Path);
            sasQuery = sas.Query.TrimStart('?');
        }
        else
        {
            var account = Environment.GetEnvironmentVariable("BENCH_ACCOUNT") ??
                          throw new InvalidOperationException("BENCH_ACCOUNT or BENCH_CONNECTION_STRING required");
            var token = Environment.GetEnvironmentVariable("BENCH_TOKEN");
            if (token == null)
            {
                using var auth = new HttpRequestMessage(HttpMethod.Get,
                    "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F");
                auth.Headers.Add("Metadata", "true");
                using var response = await http.SendAsync(auth);
                response.EnsureSuccessStatusCode();
                token = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement
                    .GetProperty("access_token").GetString();
            }

            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            root = $"https://{account}.blob.core.windows.net/{container}";
            await Request("PUT", "?restype=container", allowStatus: 409);
        }

        var results = new List<object>();
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            kind = "environment", root, prefix, samples, tableMib, machine = Environment.MachineName,
            cpuCount = Environment.ProcessorCount, runtime = Environment.Version.ToString(), utc = DateTime.UtcNow,
            qualification = "closed-loop storage primitives; not application end-to-end SLO"
        }));

        // Source object for all range cases: large enough that random offsets defeat any service-side locality.
        var table = $"/{prefix}/tables/000001.sst";
        var tableLength = (long)tableMib * 1024 * 1024;
        await Run($"table-upload-{tableMib}mib-4mib-blocks-parallel-8", async record =>
        {
            const int blockSize = 4 * 1024 * 1024;
            var block = RandomNumberGenerator.GetBytes(blockSize);
            var ids = Enumerable.Range(0, (int)(tableLength / blockSize)).Select(BlockId).ToArray();
            var start = Stopwatch.GetTimestamp();
            await Parallel.ForEachAsync(ids, new ParallelOptions { MaxDegreeOfParallelism = 8 }, async (id, _) =>
                await Request("PUT", table + "?comp=block&blockid=" + Uri.EscapeDataString(id), block));
            var xml = Encoding.UTF8.GetBytes("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>" +
                                             string.Concat(ids.Select(id => "<Latest>" + id + "</Latest>")) +
                                             "</BlockList>");
            await Request("PUT", table + "?comp=blocklist", xml, new() { ["If-None-Match"] = "*" });
            var elapsed = Stopwatch.GetElapsedTime(start);
            record(new(0, elapsed.TotalMilliseconds, tableLength, tableLength / 1048576.0 / elapsed.TotalSeconds));
        });

        foreach (var size in new[] { 4, 16, 32, 64, 128, 256, 1024, 4096 })
            await Run($"range-get-async-{size}k", async record =>
            {
                var random = new Random(size);
                var buffer = new byte[size * 1024];
                for (var i = -50; i < samples; i++)
                {
                    var offset = RandomOffset(random, buffer.Length);
                    var start = Stopwatch.GetTimestamp();
                    await ReadRange(table, offset, buffer);
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, buffer.Length, 0));
                }
            });

        // The cursor API is synchronous; compare HttpClient.Send with the async path at block-sized reads.
        foreach (var size in new[] { 32, 256 })
            await Run($"range-get-sync-{size}k", record =>
            {
                var random = new Random(size + 1);
                var buffer = new byte[size * 1024];
                for (var i = -50; i < samples; i++)
                {
                    var offset = RandomOffset(random, buffer.Length);
                    var start = Stopwatch.GetTimestamp();
                    ReadRangeSync(table, offset, buffer);
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, buffer.Length, 0));
                }

                return Task.CompletedTask;
            });

        // Parallel component seeks: latency per request and aggregate rate with N requests in flight.
        foreach (var parallelism in new[] { 4, 16, 64 })
            await Run($"range-get-async-32k-parallel-{parallelism}", async record =>
            {
                var perWorker = Math.Max(1, samples / parallelism) + 20;
                var start = Stopwatch.GetTimestamp();
                await Task.WhenAll(Enumerable.Range(0, parallelism).Select(async worker =>
                {
                    var random = new Random(worker * 7919 + parallelism);
                    var buffer = new byte[32 * 1024];
                    for (var i = -20; i < perWorker - 20; i++)
                    {
                        var offset = RandomOffset(random, buffer.Length);
                        var requestStart = Stopwatch.GetTimestamp();
                        await ReadRange(table, offset, buffer);
                        if (i >= 0)
                            lock (record)
                                record(new(i, Stopwatch.GetElapsedTime(requestStart).TotalMilliseconds,
                                    buffer.Length, 0));
                    }
                }));
                var total = (perWorker - 20) * parallelism;
                record(new(-1, 0, 0, total / Stopwatch.GetElapsedTime(start).TotalSeconds));
            });

        // Synchronous reads issued from thread-pool work items, as concurrent cursors would do. The probe measures how
        // long a trivial work item waits for a thread while the pool is blocked in I/O.
        foreach (var parallelism in new[] { 16, 64, 256 })
            await Run($"range-get-sync-32k-threadpool-{parallelism}", async record =>
            {
                var perWorker = Math.Max(1, samples / parallelism) + 10;
                var probeDelays = new List<double>();
                using var stop = new CancellationTokenSource();
                var probe = Task.Run(async () =>
                {
                    while (!stop.IsCancellationRequested)
                    {
                        var queued = Stopwatch.GetTimestamp();
                        await Task.Run(() => probeDelays.Add(Stopwatch.GetElapsedTime(queued).TotalMilliseconds));
                        await Task.Delay(10);
                    }
                });
                var maxThreads = 0;
                var start = Stopwatch.GetTimestamp();
                await Task.WhenAll(Enumerable.Range(0, parallelism).Select(worker => Task.Run(() =>
                {
                    var random = new Random(worker * 104729 + parallelism);
                    var buffer = new byte[32 * 1024];
                    for (var i = -10; i < perWorker - 10; i++)
                    {
                        var offset = RandomOffset(random, buffer.Length);
                        var requestStart = Stopwatch.GetTimestamp();
                        ReadRangeSync(table, offset, buffer);
                        if (i < 0) continue;
                        lock (record)
                        {
                            record(new(i, Stopwatch.GetElapsedTime(requestStart).TotalMilliseconds, buffer.Length, 0));
                            maxThreads = Math.Max(maxThreads, ThreadPool.ThreadCount);
                        }
                    }
                })));
                var rate = (perWorker - 10) * parallelism / Stopwatch.GetElapsedTime(start).TotalSeconds;
                await stop.CancelAsync();
                await probe;
                var delays = probeDelays.Order().ToArray();
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    kind = "threadpool", parallelism, maxThreads, requestsPerSecond = rate,
                    probeP50 = Quantile(delays, .5), probeP99 = Quantile(delays, .99),
                    probeMax = delays.Length == 0 ? double.NaN : delays[^1]
                }));
                record(new(-1, 0, 0, rate));
            });

        // Compaction input and cold restore: whole-object sequential reads in 4 MiB ranges.
        foreach (var parallelism in new[] { 4, 8, 16 })
            await Run($"sequential-read-4mib-parallel-{parallelism}", async record =>
            {
                const int rangeSize = 4 * 1024 * 1024;
                var offsets = Enumerable.Range(0, (int)(tableLength / rangeSize)).Select(i => (long)i * rangeSize);
                var start = Stopwatch.GetTimestamp();
                await Parallel.ForEachAsync(offsets, new ParallelOptions { MaxDegreeOfParallelism = parallelism },
                    async (offset, _) => await ReadRange(table, offset, new byte[rangeSize]));
                var elapsed = Stopwatch.GetElapsedTime(start);
                record(new(0, elapsed.TotalMilliseconds, tableLength, tableLength / 1048576.0 / elapsed.TotalSeconds));
            });

        // WAL and manifest tail probing: the next chunk name usually does not exist yet.
        await Run("probe-404", async record =>
        {
            for (var i = -50; i < samples; i++)
            {
                var start = Stopwatch.GetTimestamp();
                await Request("GET", $"/{prefix}/wal/{i + 1_000_000:D16}", allowStatus: 404);
                if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0));
            }
        });

        // Reading complete WAL chunks and manifest edits.
        foreach (var size in new[] { 1, 64, 256 })
            await Run($"get-small-{size}k", async record =>
            {
                var path = $"/{prefix}/small-{size}k";
                await Request("PUT", path, RandomNumberGenerator.GetBytes(size * 1024),
                    new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                for (var i = -50; i < samples; i++)
                {
                    var start = Stopwatch.GetTimestamp();
                    await Request("GET", path);
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, size * 1024, 0));
                }
            });

        await Run("get-properties", async record =>
        {
            for (var i = -50; i < samples; i++)
            {
                var start = Stopwatch.GetTimestamp();
                await Request("HEAD", table);
                if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0));
            }
        });

        foreach (var count in new[] { 10, 100, 1000 })
            await Run($"list-{count}", async record =>
            {
                var listPrefix = $"{prefix}/list-{count}/";
                await Parallel.ForEachAsync(Enumerable.Range(0, count), new ParallelOptions { MaxDegreeOfParallelism = 32 },
                    async (i, _) => await Request("PUT", $"/{listPrefix}{i:D16}", new byte[16],
                        new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" }));
                for (var i = -10; i < Math.Min(samples, 200); i++)
                {
                    var start = Stopwatch.GetTimestamp();
                    await Request("GET",
                        "?restype=container&comp=list&maxresults=5000&prefix=" + Uri.EscapeDataString(listPrefix));
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0));
                }
            });

        // WAL chunk creation, one at a time (ordinary group commit).
        foreach (var size in new[] { 64, 256, 1024 })
            await Run($"putblob-create-{size}k", async record =>
            {
                var body = RandomNumberGenerator.GetBytes(size * 1024);
                for (var i = -50; i < samples; i++)
                {
                    var start = Stopwatch.GetTimestamp();
                    await Request("PUT", $"/{prefix}/wal-{size}k/{i + 50:D16}", body,
                        new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
                    if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, body.Length, 0));
                }
            });

        // A large commit spanning four chunks uploaded in parallel; one sample is the whole group.
        await Run("putblob-create-256k-parallel-4", async record =>
        {
            var body = RandomNumberGenerator.GetBytes(256 * 1024);
            for (var i = -20; i < samples / 4; i++)
            {
                var group = i + 20;
                var start = Stopwatch.GetTimestamp();
                await Task.WhenAll(Enumerable.Range(0, 4).Select(part => Request("PUT",
                    $"/{prefix}/wal-group/{group * 4 + part:D16}", body,
                    new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" })));
                if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 4 * body.Length, 0));
            }
        });

        await Run("lease-renew-15s", async record =>
        {
            var path = $"/{prefix}/LOCK";
            await Request("PUT", path, new byte[16], new() { ["x-ms-blob-type"] = "BlockBlob", ["If-None-Match"] = "*" });
            var lease = Guid.NewGuid().ToString();
            await Request("PUT", path + "?comp=lease", headers: new()
                { ["x-ms-lease-action"] = "acquire", ["x-ms-lease-duration"] = "15", ["x-ms-proposed-lease-id"] = lease });
            for (var i = -10; i < Math.Min(samples, 200); i++)
            {
                var start = Stopwatch.GetTimestamp();
                await Request("PUT", path + "?comp=lease",
                    headers: new() { ["x-ms-lease-action"] = "renew", ["x-ms-lease-id"] = lease });
                if (i >= 0) record(new(i, Stopwatch.GetElapsedTime(start).TotalMilliseconds, 0, 0));
            }

            await Request("PUT", path + "?comp=lease",
                headers: new() { ["x-ms-lease-action"] = "release", ["x-ms-lease-id"] = lease });
        });

        await File.WriteAllTextAsync("summary.json",
            JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"Objects were left under prefix {prefix}; delete the container after the run.");

        async Task Run(string name, Func<Action<Sample>, Task> action)
        {
            if (filter != null && !name.Contains(filter, StringComparison.OrdinalIgnoreCase)) return;
            var values = new List<Sample>();
            var start = Stopwatch.GetTimestamp();
            string? error = null;
            try
            {
                await action(values.Add);
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            var latencies = values.Where(x => x.Index >= 0).Select(x => x.TotalMs).Order().ToArray();
            var throughput = values.Where(x => x.Throughput > 0).Select(x => x.Throughput).DefaultIfEmpty().Max();
            var result = new
            {
                kind = "case", name, count = latencies.Length, seconds = Stopwatch.GetElapsedTime(start).TotalSeconds,
                error, p50 = Quantile(latencies, .5), p95 = Quantile(latencies, .95), p99 = Quantile(latencies, .99),
                p999 = Quantile(latencies, .999), max = latencies.Length == 0 ? (double?)null : latencies[^1],
                throughput = throughput > 0 ? throughput : (double?)null, utc = DateTime.UtcNow
            };
            results.Add(result);
            Console.WriteLine(JsonSerializer.Serialize(result));
            await File.WriteAllTextAsync(name + ".json", JsonSerializer.Serialize(values));
        }

        long RandomOffset(Random random, int length) => random.NextInt64(tableLength - length) / 4096 * 4096;

        HttpRequestMessage CreateRequest(string method, string path, ReadOnlyMemory<byte> body,
            Dictionary<string, string>? headers)
        {
            var url = root + path;
            if (sasQuery.Length > 0) url += (path.Contains('?') ? "&" : "?") + sasQuery;
            var request = new HttpRequestMessage(new HttpMethod(method), url);
            request.Headers.TryAddWithoutValidation("x-ms-date", DateTime.UtcNow.ToString("R"));
            if (method is "PUT" or "POST") request.Content = new ReadOnlyMemoryContent(body);
            if (headers != null)
                foreach (var header in headers)
                    request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            return request;
        }

        async Task<string> Request(string method, string path, ReadOnlyMemory<byte> body = default,
            Dictionary<string, string>? headers = null, int allowStatus = 0)
        {
            using var request = CreateRequest(method, path, body, headers);
            using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            if (!response.IsSuccessStatusCode && (int)response.StatusCode != allowStatus)
                throw new StorageError((int)response.StatusCode, await response.Content.ReadAsStringAsync());
            await response.Content.CopyToAsync(Stream.Null);
            return response.Headers.ETag?.ToString() ?? "";
        }

        async Task ReadRange(string path, long offset, byte[] buffer)
        {
            using var request = CreateRequest("GET", path, default, null);
            request.Headers.Range = new RangeHeaderValue(offset, offset + buffer.Length - 1);
            using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            if (!response.IsSuccessStatusCode)
                throw new StorageError((int)response.StatusCode, await response.Content.ReadAsStringAsync());
            await using var stream = await response.Content.ReadAsStreamAsync();
            await stream.ReadExactlyAsync(buffer);
        }

        void ReadRangeSync(string path, long offset, byte[] buffer)
        {
            using var request = CreateRequest("GET", path, default, null);
            request.Headers.Range = new RangeHeaderValue(offset, offset + buffer.Length - 1);
            using var response = http.Send(request, HttpCompletionOption.ResponseHeadersRead);
            if (!response.IsSuccessStatusCode) throw new StorageError((int)response.StatusCode, "range read");
            using var stream = response.Content.ReadAsStream();
            stream.ReadExactly(buffer);
        }

        static string BlockId(int index) => Convert.ToBase64String(Encoding.ASCII.GetBytes(index.ToString("D12")));
    }

    static double? Quantile(double[] ordered, double q) =>
        ordered.Length == 0 ? null : ordered[Math.Min(ordered.Length - 1, (int)Math.Ceiling(q * ordered.Length) - 1)];

    record Sample(int Index, double TotalMs, long Bytes, double Throughput);

    sealed class StorageError(int status, string message) : Exception($"HTTP {status}: {message}");
}
